import time
import re

from qdrant_client.http import models as qm

from .config import settings
from .embedder import embed_one, embed_sparse_one
from .qdrant_client_factory import DENSE_VECTOR_NAME, SPARSE_VECTOR_NAME, get_client


HN_ITEM_URL = "https://news.ycombinator.com/item?id={id}"
PLACEHOLDER_TEXT = {"[delayed]", "[dead]", "[flagged]"}
TOKEN_RE = re.compile(r"\w+")


def _is_placeholder(payload: dict | None) -> bool:
    text = (payload or {}).get("text")
    return (text or "").strip().lower() in PLACEHOLDER_TEXT


def _normalize_payload(payload: dict | None) -> dict:
    payload = dict(payload or {})
    if not payload.get("hn_url") and str(payload.get("source", "")).startswith("hn:"):
        hn_id = payload.get("hn_id") or payload.get("stream_id")
        if hn_id:
            payload["hn_url"] = HN_ITEM_URL.format(id=hn_id)
    return payload


def _stem(term: str) -> str:
    """Strip common English plural suffixes so 'startup'/'startups' compare equal."""
    if len(term) > 4 and term.endswith("ies"):
        return term[:-3] + "y"
    if len(term) > 4 and term.endswith("es"):
        return term[:-2]
    if len(term) > 3 and term.endswith("s") and not term.endswith("ss"):
        return term[:-1]
    return term


def _haystack_stems(text: str) -> set[str]:
    return {_stem(t) for t in TOKEN_RE.findall(text.lower())}


def _lexical_score(query: str, text: str) -> int:
    q = query.strip().lower()
    haystack = text.lower()
    if not q:
        return 0
    if q in haystack:
        return 100
    terms = [t for t in TOKEN_RE.findall(q) if len(t) > 1]
    if not terms:
        return 0
    stems = _haystack_stems(text)
    return sum(1 for term in terms if term in haystack or _stem(term) in stems)


def _query_terms(query: str) -> list[str]:
    return [t for t in TOKEN_RE.findall(query.lower()) if len(t) > 1]


def _requires_lexical_match(query: str) -> bool:
    terms = _query_terms(query)
    return len(terms) == 1 and len(terms[0]) >= 4


def _match_evidence(query: str, text: str) -> dict:
    haystack = text.lower()
    terms = _query_terms(query)
    stems = _haystack_stems(text)
    matched_terms = [term for term in terms if term in haystack or _stem(term) in stems]
    exact_query_match = bool(query.strip()) and query.strip().lower() in haystack
    return {
        "exact_query_match": exact_query_match,
        "matched_terms": matched_terms,
        "match_type": "lexical" if exact_query_match or matched_terms else "semantic",
    }


async def search(
    query: str,
    k: int = 5,
    source: str | None = None,
) -> list[dict]:
    vector = await embed_one(query)
    sparse_vector = await embed_sparse_one(query)
    cutoff = int(time.time()) - settings.window_seconds
    must = [qm.FieldCondition(key="ts", range=qm.Range(gte=cutoff))]
    if source:
        must.append(qm.FieldCondition(key="source", match=qm.MatchValue(value=source)))
    client = get_client()
    prefetch_limit = max(k * 10, 50)
    require_lexical = _requires_lexical_match(query)
    res = await client.query_points(
        collection_name=settings.collection_name,
        prefetch=[
            qm.Prefetch(
                query=vector,
                using=DENSE_VECTOR_NAME,
                filter=qm.Filter(must=must),
                limit=prefetch_limit,
            ),
            qm.Prefetch(
                query=sparse_vector,
                using=SPARSE_VECTOR_NAME,
                filter=qm.Filter(must=must),
                limit=prefetch_limit,
            ),
        ],
        query=qm.FusionQuery(fusion=qm.Fusion.RRF),
        limit=prefetch_limit,
        with_payload=True,
    )
    hits = []
    for p in res.points:
        if _is_placeholder(p.payload):
            continue
        payload = _normalize_payload(p.payload)
        text = str(payload.get("text", ""))
        lexical_score = _lexical_score(query, text)
        evidence = _match_evidence(query, text)
        if require_lexical and lexical_score == 0:
            continue
        hits.append(
            {
                "id": str(p.id),
                "score": p.score,
                "payload": payload,
                "evidence": evidence,
                "_lexical_score": lexical_score,
            }
        )
    hits.sort(key=lambda h: (h["_lexical_score"], h["score"]), reverse=True)
    for h in hits:
        h.pop("_lexical_score", None)
    return hits[:k]

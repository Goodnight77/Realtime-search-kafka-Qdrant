from types import SimpleNamespace
from unittest.mock import AsyncMock

from app import search as search_mod
from app.qdrant_client_factory import DENSE_VECTOR_NAME, SPARSE_VECTOR_NAME


def _hit(id_, score, text, **payload_overrides):
    payload = {"text": text, "source": "hn:comment", "ts": 1, **payload_overrides}
    return SimpleNamespace(id=id_, score=score, payload=payload)


async def test_search_builds_hybrid_rrf_query_with_ts_and_source_filter(monkeypatch):
    monkeypatch.setattr(search_mod, "embed_one", AsyncMock(return_value=[0.1, 0.2]))
    monkeypatch.setattr(
        search_mod, "embed_sparse_one",
        AsyncMock(return_value=search_mod.qm.SparseVector(indices=[0], values=[0.1])),
    )
    monkeypatch.setattr(search_mod.time, "time", lambda: 1_000_000)
    monkeypatch.setattr(search_mod.settings, "window_seconds", 3600)

    fake_client = AsyncMock()
    fake_client.query_points.return_value = SimpleNamespace(points=[])
    monkeypatch.setattr(search_mod, "get_client", lambda: fake_client)

    await search_mod.search("kafka streams", k=5, source="hn:comment")

    kwargs = fake_client.query_points.call_args.kwargs
    assert isinstance(kwargs["query"], search_mod.qm.FusionQuery)
    assert kwargs["query"].fusion == search_mod.qm.Fusion.RRF
    assert len(kwargs["prefetch"]) == 2

    used_vectors = {p.using for p in kwargs["prefetch"]}
    assert used_vectors == {DENSE_VECTOR_NAME, SPARSE_VECTOR_NAME}

    for prefetch in kwargs["prefetch"]:
        conds = prefetch.filter.must
        ts_cond = next(c for c in conds if c.key == "ts")
        assert ts_cond.range.gte == 1_000_000 - 3600
        source_cond = next(c for c in conds if c.key == "source")
        assert source_cond.match.value == "hn:comment"


async def test_search_excludes_placeholders_and_requires_lexical_match_for_single_term(monkeypatch):
    monkeypatch.setattr(search_mod, "embed_one", AsyncMock(return_value=[0.1]))
    monkeypatch.setattr(
        search_mod, "embed_sparse_one",
        AsyncMock(return_value=search_mod.qm.SparseVector(indices=[0], values=[0.1])),
    )
    fake_client = AsyncMock()
    fake_client.query_points.return_value = SimpleNamespace(
        points=[
            _hit("1", 0.9, "[dead]"),
            _hit("2", 0.8, "totally unrelated content"),
            _hit("3", 0.5, "I love rust programming"),
        ]
    )
    monkeypatch.setattr(search_mod, "get_client", lambda: fake_client)

    hits = await search_mod.search("rust", k=5)

    ids = [h["id"] for h in hits]
    assert "1" not in ids  # placeholder text dropped
    assert "2" not in ids  # single 4+ char query term requires a lexical match
    assert ids == ["3"]


async def test_search_respects_k_limit(monkeypatch):
    monkeypatch.setattr(search_mod, "embed_one", AsyncMock(return_value=[0.1]))
    monkeypatch.setattr(
        search_mod, "embed_sparse_one",
        AsyncMock(return_value=search_mod.qm.SparseVector(indices=[0], values=[0.1])),
    )
    fake_client = AsyncMock()
    fake_client.query_points.return_value = SimpleNamespace(
        points=[_hit(str(i), 1.0 - i * 0.01, f"item {i} startups") for i in range(10)]
    )
    monkeypatch.setattr(search_mod, "get_client", lambda: fake_client)

    hits = await search_mod.search("startups", k=3)

    assert len(hits) == 3

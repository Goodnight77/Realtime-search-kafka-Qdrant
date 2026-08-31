"""Recent article headlines, independent of the reader's search query."""
import time
from urllib.parse import urlsplit

from qdrant_client.http import models as qm

from .config import settings
from .qdrant_client_factory import get_client
from .sources.catalog import FEEDS


async def latest_news() -> list[dict]:
    now = int(time.time())
    names = {f"rss:{feed['id']}": feed["name"] for feed in FEEDS}
    names.update({"hn:story": "Hacker News", "rss:item": "RSS"})
    points, _ = await get_client().scroll(
        collection_name=settings.collection_name,
        scroll_filter=qm.Filter(must=[
            qm.FieldCondition(key="ts", range=qm.Range(gte=now - min(settings.window_seconds, 86400), lte=now)),
            qm.FieldCondition(key="source", match=qm.MatchAny(any=list(names))),
        ]),
        order_by=qm.OrderBy(key="ts", direction=qm.Direction.DESC),
        limit=100,
        with_payload=True,
        with_vectors=False,
    )
    articles = []
    seen = set()
    for point in points:
        payload = point.payload or {}
        url = payload.get("external_url") or payload.get("hn_url") or ""
        try:
            parsed = urlsplit(url)
            if parsed.scheme not in {"https", "http"} or not parsed.hostname:
                continue
        except ValueError:
            continue
        # Older points predate title storage; use a short text excerpt for them.
        title = " ".join(str(payload.get("title") or payload.get("text") or "").split())
        if not title or title.lower() in {"[dead]", "[flagged]", "[delayed]"}:
            continue
        key = parsed._replace(fragment="").geturl()
        if key in seen:
            continue
        seen.add(key)
        articles.append({
            "id": str(point.id), "title": title[:177] + "…" if len(title) > 180 else title,
            "url": url, "source": payload.get("source_name") or names.get(payload.get("source"), "News"),
            "ts": payload["ts"],
        })
        if len(articles) == 5:
            break
    return articles

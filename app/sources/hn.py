import asyncio
import html
import logging
import re

import httpx

from .base import Source


HN_BASE = "https://hacker-news.firebaseio.com/v0"
HN_ITEM_URL = "https://news.ycombinator.com/item?id={id}"
TAG_RE = re.compile(r"<[^>]+>")
PLACEHOLDER_TEXT = {"[delayed]", "[dead]", "[flagged]"}
MAX_FETCH_CONCURRENCY = 20
MAX_ITEMS_PER_CYCLE = 200

log = logging.getLogger(__name__)


def _clean(text: str | None) -> str:
    if not text:
        return ""
    return html.unescape(TAG_RE.sub(" ", text)).strip()


def _to_msg(item: dict | None) -> dict | None:
    if not item:
        return None
    if item.get("dead") or item.get("deleted"):
        return None
    t = item.get("type")
    title = item.get("title") or ""
    text = _clean(item.get("text"))
    if t == "story":
        body = (title + " " + text).strip()
    elif t == "comment":
        body = text
    elif t in ("ask", "job", "poll"):
        body = (title + " " + text).strip()
    else:
        return None
    if not body or body.lower() in PLACEHOLDER_TEXT:
        return None
    item_id = item.get("id")
    return {
        "text": body[:2000],
        "source": f"hn:{t}",
        "ts": int(item.get("time") or 0),
        "id": item_id,
        "hn_id": item_id,
        "author": item.get("by"),
        "hn_url": HN_ITEM_URL.format(id=item_id) if item_id is not None else None,
        "external_url": item.get("url"),
        "parent_id": item.get("parent"),
    }


class HNSource(Source):
    name = "hn"
    poll_interval_s = 3.0

    def __init__(self) -> None:
        self._http: httpx.AsyncClient | None = None
        self._sem = asyncio.Semaphore(MAX_FETCH_CONCURRENCY)
        self._last_id: int | None = None

    async def start(self) -> None:
        self._http = httpx.AsyncClient(timeout=10.0)
        self._last_id = await self._fetch_max()
        log.info("hn source starting from max item id=%d", self._last_id)

    async def stop(self) -> None:
        if self._http is not None:
            await self._http.aclose()

    async def _fetch_max(self) -> int:
        r = await self._http.get(f"{HN_BASE}/maxitem.json")
        r.raise_for_status()
        return int(r.text)

    async def _fetch_item(self, item_id: int) -> dict | None:
        async with self._sem:
            try:
                r = await self._http.get(f"{HN_BASE}/item/{item_id}.json")
                if r.status_code != 200 or r.text in ("null", ""):
                    return None
                return r.json()
            except Exception as e:
                log.warning("item %d fetch failed: %s", item_id, e)
                return None

    async def poll(self) -> list[dict]:
        try:
            current = await self._fetch_max()
        except Exception as e:
            log.warning("maxitem fetch failed: %s", e)
            return []
        if current <= self._last_id:
            return []

        ids = list(range(self._last_id + 1, current + 1))[:MAX_ITEMS_PER_CYCLE]
        self._last_id = current

        items = await asyncio.gather(*(self._fetch_item(i) for i in ids))
        messages = []
        for item in items:
            msg = _to_msg(item)
            if msg:
                messages.append(msg)
        return messages

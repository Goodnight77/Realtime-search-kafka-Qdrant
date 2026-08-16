import logging
import time
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

import httpx

from ..config import settings
from .base import Source


log = logging.getLogger(__name__)


def _parse_pubdate(value: str | None) -> int:
    if value:
        try:
            return int(parsedate_to_datetime(value).timestamp())
        except (TypeError, ValueError):
            pass
    return int(time.time())


class RSSSource(Source):
    """Generic RSS 2.0 feed poller. Second Source impl, proves the interface
    isn't HN-shaped: cursor is a seen-guid set, not a monotonic id range."""

    name = "rss"
    poll_interval_s = 30.0

    def __init__(self, feed_url: str | None = None) -> None:
        self._feed_url = feed_url or settings.rss_feed_url
        self._http: httpx.AsyncClient | None = None
        self._seen: set[str] = set()  # ponytail: unbounded, cap with an LRU if the feed is huge/long-lived

    async def start(self) -> None:
        if not self._feed_url:
            raise ValueError("RSS_FEED_URL is not configured")
        self._http = httpx.AsyncClient(timeout=10.0)

    async def stop(self) -> None:
        if self._http is not None:
            await self._http.aclose()

    async def poll(self) -> list[dict]:
        r = await self._http.get(self._feed_url)
        r.raise_for_status()
        root = ET.fromstring(r.text)

        messages = []
        for item in root.iter("item"):
            guid = (item.findtext("guid") or item.findtext("link") or "").strip()
            if not guid or guid in self._seen:
                continue
            self._seen.add(guid)

            title = (item.findtext("title") or "").strip()
            description = (item.findtext("description") or "").strip()
            text = f"{title} {description}".strip()[:2000]
            if not text:
                continue

            messages.append(
                {
                    "text": text,
                    "source": "rss:item",
                    "ts": _parse_pubdate(item.findtext("pubDate")),
                    "id": guid,
                    "external_url": (item.findtext("link") or "").strip() or None,
                }
            )
        return messages

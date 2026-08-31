import logging
import time
import re
from collections import deque
from datetime import datetime
from html import unescape
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
        except (TypeError, ValueError, OverflowError):
            try:
                return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
            except (TypeError, ValueError, OverflowError):
                pass
    return int(time.time())


class RSSSource(Source):
    """Generic RSS 2.0 feed poller. Second Source impl, proves the interface
    isn't HN-shaped: cursor is a seen-guid set, not a monotonic id range."""

    name = "rss"
    poll_interval_s = 30.0

    def __init__(self, feed_url: str | None = None, *, source_id=None, source_name=None) -> None:
        self._feed_url = settings.rss_feed_url if feed_url is None else feed_url
        self.source_id = source_id
        self.source_name = source_name
        self._http: httpx.AsyncClient | None = None
        self._seen: set[str] = set()  # ponytail: unbounded, cap with an LRU if the feed is huge/long-lived
        self._seen_order = deque()

    async def start(self) -> None:
        if not self._feed_url:
            raise ValueError("RSS_FEED_URL is not configured")
        self._http = httpx.AsyncClient(timeout=10.0, follow_redirects=True, headers={"User-Agent": "NewsSearch/1.0 (RSS reader)"})

    async def stop(self) -> None:
        if self._http is not None:
            await self._http.aclose()

    async def poll(self) -> list[dict]:
        r = await self._http.get(self._feed_url)
        r.raise_for_status()
        if len(r.text) > 2_000_000:
            raise ValueError("Feed exceeds the 2 MB size limit")
        root = ET.fromstring(r.text)
        atom = "{http://www.w3.org/2005/Atom}"
        is_atom = root.tag == atom + "feed"
        if root.tag != "rss" and not is_atom:
            raise ValueError("This URL does not contain an RSS 2.0 or Atom feed")

        messages = []
        for item in root.iter(atom + "entry" if is_atom else "item"):
            prefix = atom if is_atom else ""
            link = (item.findtext("link") or "").strip()
            if is_atom:
                link = next((e.get("href", "") for e in item.findall(atom + "link") if e.get("rel", "alternate") == "alternate"), "")
            guid = (item.findtext(prefix + ("id" if is_atom else "guid")) or link).strip()
            if not guid or guid in self._seen:
                continue

            title = (item.findtext(prefix + "title") or "").strip()
            description = (item.findtext(prefix + ("summary" if is_atom else "description")) or item.findtext(prefix + "content") or "").strip()
            text = unescape(re.sub(r"<[^>]+>", " ", f"{title} {description}")).strip()[:2000]
            if not text:
                continue
            self._seen.add(guid)
            self._seen_order.append(guid)
            if len(self._seen_order) > 10000:
                self._seen.discard(self._seen_order.popleft())

            messages.append(
                {
                    "text": text,
                    "title": unescape(re.sub(r"<[^>]+>", " ", title)).strip(),
                    "source": f"rss:{self.source_id}" if self.source_id else "rss:item",
                    "source_name": self.source_name,
                    "feed_url": self._feed_url,
                    "ts": _parse_pubdate(item.findtext(prefix + ("published" if is_atom else "pubDate")) or item.findtext(prefix + "updated")),
                    "id": guid,
                    "external_url": link or None,
                }
            )
        return messages

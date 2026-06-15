"""
Hacker News Firebase → Kafka producer.

Polls https://hacker-news.firebaseio.com/v0/maxitem.json, fetches every new
item since last seen, normalises to {text, source, ts, id}, pushes to Kafka
topic configured in settings.kafka_topic.
"""

import asyncio
import html
import json
import logging
import re

import httpx
from aiokafka import AIOKafkaProducer

from app.config import settings


HN_BASE = "https://hacker-news.firebaseio.com/v0"
HN_ITEM_URL = "https://news.ycombinator.com/item?id={id}"
TAG_RE = re.compile(r"<[^>]+>")
PLACEHOLDER_TEXT = {"[delayed]", "[dead]", "[flagged]"}
POLL_INTERVAL_S = 3.0
MAX_FETCH_CONCURRENCY = 20
MAX_ITEMS_PER_CYCLE = 200


log = logging.getLogger("hn_producer")


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


async def _fetch_max(client: httpx.AsyncClient) -> int:
    r = await client.get(f"{HN_BASE}/maxitem.json")
    r.raise_for_status()
    return int(r.text)


async def _fetch_item(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    item_id: int,
) -> dict | None:
    async with sem:
        try:
            r = await client.get(f"{HN_BASE}/item/{item_id}.json")
            if r.status_code != 200 or r.text in ("null", ""):
                return None
            return r.json()
        except Exception as e:
            log.warning("item %d fetch failed: %s", item_id, e)
            return None


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    await producer.start()
    log.info("kafka producer started → %s", settings.kafka_bootstrap)

    sem = asyncio.Semaphore(MAX_FETCH_CONCURRENCY)

    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            last = await _fetch_max(http)
            log.info("starting from max item id=%d", last)
            while True:
                await asyncio.sleep(POLL_INTERVAL_S)
                try:
                    current = await _fetch_max(http)
                except Exception as e:
                    log.warning("maxitem fetch failed: %s", e)
                    continue
                if current <= last:
                    continue

                ids = list(range(last + 1, current + 1))[:MAX_ITEMS_PER_CYCLE]
                last = current

                items = await asyncio.gather(*(_fetch_item(http, sem, i) for i in ids))
                sent = 0
                for item in items:
                    msg = _to_msg(item)
                    if not msg:
                        continue
                    await producer.send_and_wait(settings.kafka_topic, msg)
                    sent += 1
                    log.info(
                        "sent %s id=%s ts=%s text=%s",
                        msg["source"],
                        msg["id"],
                        msg["ts"],
                        msg["text"][:80],
                    )
                if sent:
                    log.info("cycle: %d sent / %d ids", sent, len(ids))
    finally:
        await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

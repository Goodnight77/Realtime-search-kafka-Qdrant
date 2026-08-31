import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field

from qdrant_client.http import models as qm

from .config import settings
from .embedder import embed, embed_sparse
from .metrics import INGEST_TOTAL
from .qdrant_client_factory import DENSE_VECTOR_NAME, SPARSE_VECTOR_NAME, get_client


log = logging.getLogger(__name__)
HN_ITEM_URL = "https://news.ycombinator.com/item?id={id}"
PLACEHOLDER_TEXT = {"[delayed]", "[dead]", "[flagged]"}
POINT_ID_NAMESPACE = uuid.UUID("a8f8a9d4-3b1e-4f6a-9c2d-7e5b6f0a1c3d")


@dataclass
class IngestState:
    new_event: asyncio.Event = field(default_factory=asyncio.Event)
    total_ingested: int = 0


state = IngestState()


def _is_placeholder(text: str | None) -> bool:
    return (text or "").strip().lower() in PLACEHOLDER_TEXT


def _hn_url_for(msg: dict) -> str | None:
    hn_id = msg.get("hn_id") or msg.get("id")
    if not hn_id or not str(msg.get("source", "")).startswith("hn:"):
        return None
    return HN_ITEM_URL.format(id=hn_id)


async def _flush(buf: list[dict]) -> None:
    if not buf:
        return
    buf[:] = [m for m in buf if not _is_placeholder(m.get("text"))]
    if not buf:
        return
    client = get_client()
    texts = [m["text"] for m in buf]
    vectors = await embed(texts)
    sparse_vectors = await embed_sparse(texts)
    points = []
    for m, v, sv in zip(buf, vectors, sparse_vectors):
        hn_id = m.get("hn_id") or m.get("id")
        point_id = (
            str(uuid.uuid5(POINT_ID_NAMESPACE, f"{m.get('source')}:{hn_id}"))
            if hn_id is not None
            else str(uuid.uuid4())
        )
        points.append(
            qm.PointStruct(
                id=point_id,
                vector={
                    DENSE_VECTOR_NAME: v,
                    SPARSE_VECTOR_NAME: sv,
                },
                payload={
                    "text": m["text"],
                    "title": m.get("title"),
                    "ts": int(m.get("ts") or time.time()),
                    "source": m.get("source", "unknown"),
                    "source_name": m.get("source_name"),
                    "feed_url": m.get("feed_url"),
                    "stream_id": m.get("id"),
                    "hn_id": m.get("hn_id") or m.get("id"),
                    "author": m.get("author"),
                    "hn_url": m.get("hn_url") or _hn_url_for(m),
                    "external_url": m.get("external_url"),
                    "parent_id": m.get("parent_id"),
                },
            )
        )
    await client.upsert(collection_name=settings.collection_name, points=points)
    for p in points:
        INGEST_TOTAL.labels(source=p.payload["source"]).inc()
    state.total_ingested += len(points)
    log.info("upserted %d (total=%d)", len(points), state.total_ingested)
    buf.clear()
    state.new_event.set()


async def batch_writer(queue: asyncio.Queue) -> None:
    buf: list[dict] = []
    flush_interval = settings.batch_flush_ms / 1000.0

    while True:
        try:
            msg = await asyncio.wait_for(queue.get(), timeout=flush_interval)
            buf.append(msg)
            if len(buf) >= settings.batch_size:
                await _flush(buf)
        except asyncio.TimeoutError:
            await _flush(buf)
        except asyncio.CancelledError:
            await _flush(buf)
            raise
        except Exception as e:
            log.exception("batch_writer error: %s", e)


async def _trim_to_window_size() -> None:
    """Keep newest WINDOW_SIZE points. Delete the rest by ts ascending."""
    client = get_client()
    count_res = await client.count(collection_name=settings.collection_name, exact=False)
    if count_res.count <= settings.window_size:
        return
    over = count_res.count - settings.window_size
    scroll, _ = await client.scroll(
        collection_name=settings.collection_name,
        limit=over,
        with_payload=False,
        with_vectors=False,
        order_by=qm.OrderBy(key="ts", direction=qm.Direction.ASC),
    )
    ids = [p.id for p in scroll]
    if ids:
        await client.delete(
            collection_name=settings.collection_name,
            points_selector=qm.PointIdsList(points=ids),
        )
        log.info("trimmed %d oldest points", len(ids))


async def window_cleaner() -> None:
    client = get_client()
    while True:
        try:
            await asyncio.sleep(5.0)
            cutoff = int(time.time()) - settings.window_seconds
            await client.delete(
                collection_name=settings.collection_name,
                points_selector=qm.FilterSelector(
                    filter=qm.Filter(
                        must=[qm.FieldCondition(key="ts", range=qm.Range(lt=cutoff))]
                    )
                ),
            )
            await _trim_to_window_size()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("window cleanup error: %s", e)

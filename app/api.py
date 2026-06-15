import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .embedder import warmup as embedder_warmup
from .ingest import batch_writer, state, window_cleaner
from .config import settings
from .qdrant_client_factory import close_client, ensure_collection, get_client
from .search import search
from .stream_source import kafka_source


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    await ensure_collection()
    await embedder_warmup()
    _tasks.append(asyncio.create_task(kafka_source(queue), name="kafka_source"))
    _tasks.append(asyncio.create_task(batch_writer(queue), name="batch_writer"))
    _tasks.append(asyncio.create_task(window_cleaner(), name="window_cleaner"))
    log.info("startup complete")
    try:
        yield
    finally:
        for t in _tasks:
            t.cancel()
        for t in _tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        await close_client()
        log.info("shutdown complete")


app = FastAPI(title="Real-Time Qdrant Search", lifespan=lifespan)

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
async def root():
    return FileResponse("static/index.html")


class SearchRequest(BaseModel):
    query: str
    k: int = 5
    source: str | None = None


@app.get("/health")
async def health():
    client = get_client()
    count = await client.count(collection_name=settings.collection_name, exact=False)
    return {
        "status": "ok",
        "ingested": state.total_ingested,
        "in_window": count.count,
        "window_seconds": settings.window_seconds,
        "window_size": settings.window_size,
    }


@app.post("/search")
async def search_endpoint(req: SearchRequest):
    hits = await search(req.query, req.k, req.source)
    return {
        "query": req.query,
        "k": req.k,
        "source": req.source,
        "hits": hits,
    }


@app.websocket("/ws/search")
async def ws_search(ws: WebSocket):
    await ws.accept()
    try:
        init = await ws.receive_json()
        query = init["query"]
        k = int(init.get("k", 5))
        source = init.get("source")
        last_ids: set[str] = set()
        while True:
            hits = await search(query, k, source)
            ids = {h["id"] for h in hits}
            if ids != last_ids:
                await ws.send_json({"query": query, "hits": hits})
                last_ids = ids
            state.new_event.clear()
            try:
                await asyncio.wait_for(state.new_event.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
    except WebSocketDisconnect:
        return

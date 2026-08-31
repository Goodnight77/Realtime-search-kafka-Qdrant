import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from qdrant_client.http.exceptions import ResponseHandlingException

from .embedder import warmup as embedder_warmup
from .ingest import batch_writer, state, window_cleaner
from .config import settings
from .qdrant_client_factory import close_client, ensure_collection, get_client
from .search import search
from .news import latest_news
from .stream_source import kafka_source
from .sources.manager import manager


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
_tasks: list[asyncio.Task] = []
_ready = False


async def start_pipeline():
    global _ready
    while True:
        try:
            await ensure_collection()
            await embedder_warmup()
            _tasks.append(asyncio.create_task(kafka_source(queue), name="kafka_source"))
            _tasks.append(asyncio.create_task(batch_writer(queue), name="batch_writer"))
            _tasks.append(asyncio.create_task(window_cleaner(), name="window_cleaner"))
            _ready = True
            return
        except Exception as exc:
            log.warning("Search startup pending: %s", exc)
            await asyncio.sleep(15)


async def start_feeds():
    while True:
        try:
            await manager.start()
            return
        except Exception as exc:
            log.warning("Feed publisher waiting for Kafka: %s", exc)
            await asyncio.sleep(15)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ready
    _tasks.append(asyncio.create_task(start_pipeline(), name="start_pipeline"))
    _tasks.append(asyncio.create_task(start_feeds(), name="start_feeds"))
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
        await manager.stop()
        await close_client()
        _ready = False
        _tasks.clear()
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


async def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if settings.api_key and x_api_key != settings.api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid or missing API key")


@app.get("/sources")
async def list_sources():
    return {"sources": [{"id": "hacker-news", "name": "Hacker News", "filter": None}, *manager.list()]}


@app.get("/health")
async def health():
    if not _ready:
        raise HTTPException(503, "Search is starting or waiting for Qdrant. The publication list is available.")
    client = get_client()
    for attempt in range(2):
        try:
            count = await client.count(collection_name=settings.collection_name, exact=False)
            break
        except ResponseHandlingException as exc:
            if attempt == 0:
                await asyncio.sleep(0.2)
                continue
            log.warning("Qdrant health check failed: %s", exc)
            raise HTTPException(503, "Cannot currently reach the search database. Retrying shortly.") from exc
    return {
        "status": "ok",
        "ingested": state.total_ingested,
        "in_window": count.count,
        "window_seconds": settings.window_seconds,
        "window_size": settings.window_size,
    }


@app.get("/metrics", include_in_schema=False)
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/news", dependencies=[Depends(require_api_key)])
async def news_endpoint():
    if not _ready:
        raise HTTPException(503, "Headlines are starting. Please try again shortly.")
    try:
        return {"articles": await latest_news()}
    except Exception as exc:
        log.warning("Headlines unavailable: %s", exc)
        raise HTTPException(503, "Headlines are temporarily unavailable.") from exc


@app.post("/search", dependencies=[Depends(require_api_key)])
async def search_endpoint(req: SearchRequest):
    if not _ready:
        raise HTTPException(503, "Search is waiting for Qdrant and embedding models. Please try again shortly.")
    hits = await search(req.query, req.k, req.source)
    return {
        "query": req.query,
        "k": req.k,
        "source": req.source,
        "hits": hits,
    }


@app.websocket("/ws/search")
async def ws_search(ws: WebSocket):
    if settings.api_key and ws.headers.get("x-api-key") != settings.api_key:
        await ws.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    await ws.accept()
    try:
        init = await ws.receive_json()
        if not _ready:
            await ws.send_json({"error": "Search is starting or waiting for Qdrant."})
            await ws.close(code=1013)
            return
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

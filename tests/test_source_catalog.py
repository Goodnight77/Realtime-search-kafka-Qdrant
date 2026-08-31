import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest

from app.api import app
from app.sources.catalog import FEEDS
from app.sources.manager import SourceManager


async def test_catalog_is_read_only_and_available_without_search_services():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/sources")
        assert response.status_code == 200
        assert len(response.json()["sources"]) == len(FEEDS) + 1
        assert (await client.post("/sources", json={"name": "User feed", "url": "https://example.com"})).status_code == 405
        assert (await client.delete("/sources/lobsters")).status_code == 404


async def test_failed_publish_retries_pending_article(monkeypatch):
    import app.sources.manager as module

    message = {"id": "1", "source": "rss:example", "text": "Story", "ts": 1}
    source = AsyncMock()
    source.poll_interval_s = 0
    source.poll.side_effect = lambda: [message.copy()]
    monkeypatch.setattr(module, "RSSSource", lambda *args, **kwargs: source)
    manager = SourceManager()
    manager.producer = AsyncMock()
    manager.producer.send_and_wait.side_effect = [RuntimeError("Kafka unavailable"), None, asyncio.CancelledError()]
    with pytest.raises(asyncio.CancelledError):
        await manager.run({"id": "example", "name": "Example", "url": "https://example.com/feed"})
    calls = manager.producer.send_and_wait.call_args_list
    assert calls[0].args[1] == calls[1].args[1] == message
    assert manager.runtime["example"]["published"] == 1
    source.stop.assert_awaited_once()

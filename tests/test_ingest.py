import asyncio
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

from prometheus_client import REGISTRY

from app import ingest


def _msg(**overrides):
    base = {"text": "hello world", "source": "hn:comment", "ts": 111, "hn_id": 999}
    base.update(overrides)
    return base


async def test_flush_generates_deterministic_dedup_id(monkeypatch):
    fake_client = AsyncMock()
    monkeypatch.setattr(ingest, "get_client", lambda: fake_client)
    monkeypatch.setattr(ingest, "embed", AsyncMock(return_value=[[0.1, 0.2]]))
    monkeypatch.setattr(ingest, "embed_sparse", AsyncMock(return_value=[ingest.qm.SparseVector(indices=[0], values=[0.1])]))

    buf = [_msg()]
    await ingest._flush(buf)

    fake_client.upsert.assert_called_once()
    points = fake_client.upsert.call_args.kwargs["points"]
    assert len(points) == 1
    expected_id = str(uuid.uuid5(ingest.POINT_ID_NAMESPACE, "hn:comment:999"))
    assert points[0].id == expected_id


async def test_flush_same_source_and_hn_id_is_idempotent(monkeypatch):
    fake_client = AsyncMock()
    monkeypatch.setattr(ingest, "get_client", lambda: fake_client)
    monkeypatch.setattr(ingest, "embed", AsyncMock(return_value=[[0.1, 0.2]]))
    monkeypatch.setattr(ingest, "embed_sparse", AsyncMock(return_value=[ingest.qm.SparseVector(indices=[0], values=[0.1])]))

    await ingest._flush([_msg()])
    first_id = fake_client.upsert.call_args.kwargs["points"][0].id

    await ingest._flush([_msg(text="edited reply, same hn_id")])
    second_id = fake_client.upsert.call_args.kwargs["points"][0].id

    assert first_id == second_id


async def test_flush_different_hn_id_gets_different_dedup_id(monkeypatch):
    fake_client = AsyncMock()
    monkeypatch.setattr(ingest, "get_client", lambda: fake_client)
    monkeypatch.setattr(ingest, "embed", AsyncMock(return_value=[[0.1, 0.2]]))
    monkeypatch.setattr(ingest, "embed_sparse", AsyncMock(return_value=[ingest.qm.SparseVector(indices=[0], values=[0.1])]))

    await ingest._flush([_msg(hn_id=999)])
    id_a = fake_client.upsert.call_args.kwargs["points"][0].id

    await ingest._flush([_msg(hn_id=1000)])
    id_b = fake_client.upsert.call_args.kwargs["points"][0].id

    assert id_a != id_b


async def test_flush_drops_placeholder_only_batch(monkeypatch):
    fake_client = AsyncMock()
    monkeypatch.setattr(ingest, "get_client", lambda: fake_client)
    monkeypatch.setattr(ingest, "embed", AsyncMock(return_value=[]))
    monkeypatch.setattr(ingest, "embed_sparse", AsyncMock(return_value=[]))

    await ingest._flush([_msg(text="[dead]")])

    fake_client.upsert.assert_not_called()


async def test_flush_increments_ingest_counter_by_source(monkeypatch):
    fake_client = AsyncMock()
    monkeypatch.setattr(ingest, "get_client", lambda: fake_client)
    monkeypatch.setattr(ingest, "embed", AsyncMock(return_value=[[0.1, 0.2]]))
    monkeypatch.setattr(ingest, "embed_sparse", AsyncMock(return_value=[ingest.qm.SparseVector(indices=[0], values=[0.1])]))

    before = REGISTRY.get_sample_value("ingest_total", {"source": "hn:comment"}) or 0
    await ingest._flush([_msg(source="hn:comment", hn_id=54321)])
    after = REGISTRY.get_sample_value("ingest_total", {"source": "hn:comment"})

    assert after == before + 1


async def test_trim_noop_when_under_window_size(monkeypatch):
    fake_client = AsyncMock()
    fake_client.count.return_value = SimpleNamespace(count=5)
    monkeypatch.setattr(ingest, "get_client", lambda: fake_client)
    monkeypatch.setattr(ingest.settings, "window_size", 10)

    await ingest._trim_to_window_size()

    fake_client.scroll.assert_not_called()
    fake_client.delete.assert_not_called()


async def test_trim_deletes_oldest_over_window_size(monkeypatch):
    fake_client = AsyncMock()
    fake_client.count.return_value = SimpleNamespace(count=15)
    fake_client.scroll.return_value = ([SimpleNamespace(id="a"), SimpleNamespace(id="b")], None)
    monkeypatch.setattr(ingest, "get_client", lambda: fake_client)
    monkeypatch.setattr(ingest.settings, "window_size", 10)

    await ingest._trim_to_window_size()

    assert fake_client.scroll.call_args.kwargs["limit"] == 5  # over = 15 - 10
    deleted = fake_client.delete.call_args.kwargs["points_selector"].points
    assert deleted == ["a", "b"]


async def test_window_cleaner_deletes_by_ts_cutoff(monkeypatch):
    fake_client = AsyncMock()
    fake_client.count.return_value = SimpleNamespace(count=0)
    monkeypatch.setattr(ingest, "get_client", lambda: fake_client)
    monkeypatch.setattr(ingest.settings, "window_seconds", 3600)
    monkeypatch.setattr(ingest.time, "time", lambda: 1_000_000)

    calls = {"n": 0}

    async def fake_sleep(_):
        calls["n"] += 1
        if calls["n"] > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(ingest.asyncio, "sleep", fake_sleep)

    try:
        await ingest.window_cleaner()
    except asyncio.CancelledError:
        pass

    fake_client.delete.assert_called_once()
    cond = fake_client.delete.call_args.kwargs["points_selector"].filter.must[0]
    assert cond.key == "ts"
    assert cond.range.lt == 1_000_000 - 3600

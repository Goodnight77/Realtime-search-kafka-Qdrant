import asyncio

from app.sources import runner
from app.sources.base import Source


class FakeProducer:
    def __init__(self, *args, **kwargs):
        self.sent = []

    async def start(self):
        pass

    async def stop(self):
        pass

    async def send_and_wait(self, topic, msg):
        self.sent.append((topic, msg))


def _patch_producer(monkeypatch):
    created = []

    def factory(*args, **kwargs):
        p = FakeProducer()
        created.append(p)
        return p

    monkeypatch.setattr(runner, "AIOKafkaProducer", factory)
    return created


class CountingSource(Source):
    name = "fake"
    poll_interval_s = 0

    def __init__(self, batches):
        self._batches = list(batches)
        self.started = False
        self.stopped = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def poll(self):
        if not self._batches:
            raise asyncio.CancelledError
        return self._batches.pop(0)


async def test_run_producer_sends_messages_and_stops_cleanly(monkeypatch):
    created = _patch_producer(monkeypatch)
    source = CountingSource(
        [
            [{"text": "a", "source": "x", "ts": 1, "id": "1"}],
            [{"text": "b", "source": "x", "ts": 2, "id": "2"}],
        ]
    )

    try:
        await runner.run_producer(source)
    except asyncio.CancelledError:
        pass

    assert source.started
    assert source.stopped
    assert [msg["id"] for _, msg in created[0].sent] == ["1", "2"]


class FlakyThenDoneSource(Source):
    name = "flaky"
    poll_interval_s = 0

    def __init__(self):
        self._calls = 0

    async def poll(self):
        self._calls += 1
        if self._calls == 1:
            raise RuntimeError("boom")
        if self._calls == 2:
            return [{"text": "ok", "source": "x", "ts": 1, "id": "1"}]
        raise asyncio.CancelledError


async def test_run_producer_survives_poll_exception(monkeypatch):
    created = _patch_producer(monkeypatch)

    try:
        await runner.run_producer(FlakyThenDoneSource())
    except asyncio.CancelledError:
        pass

    assert len(created[0].sent) == 1
    assert created[0].sent[0][1]["id"] == "1"

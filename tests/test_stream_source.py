import asyncio
from types import SimpleNamespace

from prometheus_client import REGISTRY

from app import stream_source


class FakeConsumer:
    def __init__(self, *args, **kwargs):
        self._messages = [
            SimpleNamespace(topic="stream_search", partition=0, offset=41, value={"text": "hi"})
        ]

    async def start(self):
        pass

    async def stop(self):
        pass

    def highwater(self, tp):
        return 50

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for m in self._messages:
            yield m


async def test_kafka_source_sets_consumer_lag_gauge(monkeypatch):
    monkeypatch.setattr(stream_source, "AIOKafkaConsumer", FakeConsumer)
    queue = asyncio.Queue()

    await stream_source.kafka_source(queue)

    lag = REGISTRY.get_sample_value(
        "kafka_consumer_lag", {"topic": "stream_search", "partition": "0"}
    )
    assert lag == 50 - 41 - 1
    assert queue.qsize() == 1

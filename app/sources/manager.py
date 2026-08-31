"""Publish the developer-curated news feeds through Kafka.

Edit FEEDS in catalog.py to change publications. Run one API worker per instance.
"""
import asyncio
import json
import logging
import time

from aiokafka import AIOKafkaProducer

from ..config import settings
from .catalog import FEEDS
from .rss import RSSSource

log = logging.getLogger(__name__)


class SourceManager:
    def __init__(self):
        self.tasks = []
        self.runtime = {}
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap,
            value_serializer=lambda value: json.dumps(value).encode(),
        )
        try:
            await self.producer.start()
        except BaseException:
            await self.producer.stop()
            self.producer = None
            raise
        for record in FEEDS:
            self.tasks.append(asyncio.create_task(self.run(record), name=f"feed:{record['id']}"))

    async def stop(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        if self.producer:
            await self.producer.stop()
            self.producer = None

    def list(self):
        return [{**record, "filter": f"rss:{record['id']}",
                 **self.runtime.get(record["id"], {"status": "starting"})}
                for record in FEEDS]

    async def run(self, record):
        source = RSSSource(record["url"], source_id=record["id"], source_name=record["name"])
        info = self.runtime[record["id"]] = {"status": "starting", "published": 0, "last_poll": None, "error": None}
        pending = []
        try:
            await source.start()
            while True:
                try:
                    if not pending:
                        pending = await source.poll()
                    while pending:
                        await self.producer.send_and_wait(settings.kafka_topic, pending[0])
                        pending.pop(0)
                        info["published"] += 1
                    info.update(status="active", last_poll=int(time.time()), error=None)
                except Exception as exc:
                    info.update(status="error", error=str(exc)[:250])
                    log.warning("Feed %s failed: %s", record["name"], exc)
                await asyncio.sleep(source.poll_interval_s)
        finally:
            await source.stop()


manager = SourceManager()

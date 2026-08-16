import asyncio
import json
import logging

from aiokafka import AIOKafkaProducer

from ..config import settings
from .base import Source


log = logging.getLogger(__name__)


async def run_producer(source: Source) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    await producer.start()
    log.info("%s producer started -> %s", source.name, settings.kafka_bootstrap)
    await source.start()

    try:
        while True:
            await asyncio.sleep(source.poll_interval_s)
            try:
                messages = await source.poll()
            except Exception as e:
                log.warning("%s poll failed: %s", source.name, e)
                continue

            for msg in messages:
                await producer.send_and_wait(settings.kafka_topic, msg)
                log.info(
                    "sent %s id=%s ts=%s text=%s",
                    msg["source"],
                    msg["id"],
                    msg["ts"],
                    msg["text"][:80],
                )
            if messages:
                log.info("%s cycle: %d sent", source.name, len(messages))
    finally:
        await source.stop()
        await producer.stop()

import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer, TopicPartition

from .config import settings
from .metrics import KAFKA_CONSUMER_LAG


log = logging.getLogger(__name__)


async def kafka_source(queue: asyncio.Queue) -> None:
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=settings.kafka_group_id,
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    await consumer.start()
    log.info(
        "kafka source consuming topic=%s bootstrap=%s",
        settings.kafka_topic,
        settings.kafka_bootstrap,
    )
    try:
        async for msg in consumer:
            highwater = consumer.highwater(TopicPartition(msg.topic, msg.partition))
            if highwater is not None:
                lag = max(highwater - msg.offset - 1, 0)
                KAFKA_CONSUMER_LAG.labels(topic=msg.topic, partition=msg.partition).set(lag)
            v = msg.value
            if isinstance(v, dict) and v.get("text"):
                await queue.put(v)
    finally:
        await consumer.stop()

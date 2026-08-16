"""
Generic RSS feed -> Kafka producer. Set RSS_FEED_URL in .env. Entrypoint
over app/sources/rss.py -- second Source implementation, proves the same
Kafka -> embed -> Qdrant pipeline isn't HN-specific.
"""

import asyncio
import logging

from app.sources.rss import RSSSource
from app.sources.runner import run_producer


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    await run_producer(RSSSource())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

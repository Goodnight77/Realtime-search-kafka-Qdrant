"""
Hacker News Firebase -> Kafka producer. Entrypoint over app/sources/hn.py.
"""

import asyncio
import logging

from app.sources.hn import HNSource
from app.sources.runner import run_producer


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    await run_producer(HNSource())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

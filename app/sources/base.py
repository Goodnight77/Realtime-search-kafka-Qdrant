from abc import ABC, abstractmethod


class Source(ABC):
    """A pollable feed producing normalized {text, source, ts, id} messages.

    Each source owns its own cursor (last seen id, seen-set, etc.) between
    poll() calls, so the runner loop stays identical across sources.
    """

    name: str = "source"
    poll_interval_s: float = 3.0

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    @abstractmethod
    async def poll(self) -> list[dict]:
        """Return new messages since the last poll() call."""

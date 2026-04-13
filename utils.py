import asyncio
import time


class RateLimiter:
    """Enforces a max number of API calls per time period across coroutines."""

    def __init__(self, max_calls: int, period: float) -> None:
        self._lock: asyncio.Lock | None = None
        self._max_calls = max_calls
        self._period = period
        self._calls: list[float] = []

    async def wait(self) -> None:
        if self._lock is None:
            self._lock = asyncio.Lock()
        while True:
            async with self._lock:
                now = time.time()
                self._calls = [t for t in self._calls if now - t < self._period]
                if len(self._calls) < self._max_calls:
                    self._calls.append(time.time())
                    return
                sleep_for = self._period - (now - self._calls[0])
            await asyncio.sleep(max(sleep_for, 0))

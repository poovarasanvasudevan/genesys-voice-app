import asyncio
import time


class RateLimiter:
    """Token-window limiter for Genesys JSON and binary send rates."""

    def __init__(self, rate_limit: float, burst_limit: int, window_seconds: float = 1.0):
        self.rate_limit = rate_limit
        self.burst_limit = burst_limit
        self.window = window_seconds
        self.timestamps: list[float] = []
        self.lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self.lock:
            now = time.time()
            window_start = now - self.window
            self.timestamps = [ts for ts in self.timestamps if ts > window_start]

            if len(self.timestamps) >= self.burst_limit:
                return False
            if len(self.timestamps) >= self.rate_limit:
                oldest = self.timestamps[0]
                if now - oldest < self.window:
                    return False

            self.timestamps.append(now)
            return True

    def get_current_rate(self) -> float:
        now = time.time()
        window_start = now - self.window
        recent = [ts for ts in self.timestamps if ts > window_start]
        return len(recent) / self.window if recent else 0.0

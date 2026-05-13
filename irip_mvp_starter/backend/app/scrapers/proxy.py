"""Thread-safe ScraperAPI proxy utilities for IRIP scrapers.

Provides:
  - SlidingWindowRateLimiter: enforces a per-day (or per-window) call cap
    across all threads so the free-tier quota is never accidentally exhausted.
  - ApiKeyPool: round-robin across one or more ScraperAPI keys; builds the
    proxy URL that wraps any target URL for residential-IP routing.

Both classes are safe to share across threads (GitHub Actions is single-
threaded, but Render's FastAPI workers are multi-threaded — safety is free).

Usage (run_daily_scrape.py):
    pool = ApiKeyPool.from_env(os.getenv("SCRAPERAPI_KEY"))
    scraper = AmazonReviewScraper(scraperapi_pool=pool)
"""
from __future__ import annotations

import threading
import time
from collections import deque
from urllib.parse import quote_plus


# ============================================================
# SlidingWindowRateLimiter
# ============================================================


class SlidingWindowRateLimiter:
    """Thread-safe sliding-window rate limiter.

    Allows at most ``max_calls`` calls within any rolling ``window_seconds``
    interval.  Callers block in ``acquire()`` until a slot is available.

    Args:
        max_calls:       Maximum calls allowed within the window.
        window_seconds:  Duration of the sliding window in seconds.

    Example — 900 requests per day:
        limiter = SlidingWindowRateLimiter(max_calls=900, window_seconds=86400)
        limiter.acquire()   # blocks if 900 calls already made in last 24 h
    """

    def __init__(self, max_calls: int, window_seconds: float) -> None:
        if max_calls < 1:
            raise ValueError("max_calls must be >= 1")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        self._max_calls = max_calls
        self._window = window_seconds
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a call slot is available, then consume one slot."""
        while True:
            with self._lock:
                now = time.monotonic()
                # Evict timestamps that have slid out of the window
                while self._timestamps and self._timestamps[0] <= now - self._window:
                    self._timestamps.popleft()
                if len(self._timestamps) < self._max_calls:
                    self._timestamps.append(now)
                    return
                # How long until the oldest slot expires
                wait = self._window - (now - self._timestamps[0])
            time.sleep(max(0.05, wait))

    @property
    def calls_in_window(self) -> int:
        """Current number of calls within the active window (for logging)."""
        with self._lock:
            now = time.monotonic()
            while self._timestamps and self._timestamps[0] <= now - self._window:
                self._timestamps.popleft()
            return len(self._timestamps)


# ============================================================
# ApiKeyPool
# ============================================================


class ApiKeyPool:
    """Thread-safe round-robin pool of ScraperAPI keys.

    Wraps any target URL in the ScraperAPI proxy URL format.
    An integrated ``SlidingWindowRateLimiter`` enforces a per-day quota so the
    free-tier limit (1,000 req/month ≈ 33/day) is never blown in a single run.

    Args:
        keys:              One or more ScraperAPI API keys.
        max_calls_per_day: Max proxy calls across all keys per 24-hour window.
                           Default 900 leaves a 10% buffer on the 1 000/month
                           free tier (33 days × 900/day ≈ plan limit).
    """

    _ENDPOINT = "http://api.scraperapi.com"

    def __init__(
        self,
        keys: list[str],
        max_calls_per_day: int = 900,
    ) -> None:
        if not keys:
            raise ValueError("ApiKeyPool requires at least one key")
        self._keys = keys
        self._index = 0
        self._lock = threading.Lock()
        self._limiter = SlidingWindowRateLimiter(
            max_calls=max_calls_per_day,
            window_seconds=86_400,
        )

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls, env_value: str | None, **kwargs: int) -> "ApiKeyPool | None":
        """Create a pool from a (possibly comma-separated) env-var value.

        Returns None if the value is empty or None — callers treat None pool
        as "ScraperAPI disabled; use direct requests."

        Example env values:
            SCRAPERAPI_KEY=abc123                  → pool with 1 key
            SCRAPERAPI_KEY=abc123,def456,ghi789    → pool with 3 keys
        """
        if not env_value:
            return None
        keys = [k.strip() for k in env_value.split(",") if k.strip()]
        return cls(keys, **kwargs) if keys else None

    # ------------------------------------------------------------------
    # Key selection
    # ------------------------------------------------------------------

    def _next_key(self) -> str:
        """Return the next key in round-robin order (thread-safe)."""
        with self._lock:
            key = self._keys[self._index % len(self._keys)]
            self._index += 1
            return key

    # ------------------------------------------------------------------
    # URL building
    # ------------------------------------------------------------------

    def build_url(
        self,
        target_url: str,
        render: bool = False,
        country_code: str = "in",
    ) -> str:
        """Wrap *target_url* in the ScraperAPI proxy URL format.

        Acquires one slot from the rate limiter before returning — callers
        should not call ``acquire()`` separately.

        Args:
            target_url:   The original URL to fetch through a residential IP.
            render:       Set True only for JS-heavy pages (costs 5× credits).
            country_code: Two-letter country code for geolocation (default "in"
                          routes through Indian residential IPs).

        Returns:
            The full ScraperAPI proxy URL string.
        """
        self._limiter.acquire()

        key = self._next_key()
        encoded = quote_plus(target_url)
        render_param = "true" if render else "false"
        return (
            f"{self._ENDPOINT}"
            f"?api_key={key}"
            f"&url={encoded}"
            f"&render={render_param}"
            f"&country_code={country_code}"
        )

    @property
    def daily_calls_used(self) -> int:
        """How many proxy calls have been made in the current 24-hour window."""
        return self._limiter.calls_in_window

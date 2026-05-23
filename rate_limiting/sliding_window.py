"""Sliding window log rate limiter using Redis sorted sets.

Sliding Window Log Algorithm:
1. Store each request timestamp in a sorted set
2. Remove timestamps older than the window
3. Count remaining timestamps
4. Allow if count < limit, else reject

Advantages: Exact count within window, no burst at window boundaries.
Disadvantage: Memory scales with request count (use sliding window counter for scale).

Sliding Window Counter (approximation):
- Store count for current and previous window
- Weight: count = current + previous * ((window - elapsed) / window)
"""
import time
import redis
from typing import Tuple


class SlidingWindowLog:
    """Exact sliding window rate limiter using Redis sorted sets.

    Memory: O(requests) — stores one entry per request in the window.
    Best for: strict rate limits, low-to-medium traffic.
    """

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.r = redis.from_url(redis_url)

    def is_allowed(
        self,
        key: str,
        limit: int,
        window_seconds: int,
        cost: int = 1,
    ) -> Tuple[bool, int, float]:
        """Check if request is within the sliding window rate limit.

        Args:
            key: Identifier (user ID, IP, etc.)
            limit: Max requests per window
            window_seconds: Window duration in seconds
            cost: Weight of this request (default 1)

        Returns:
            (allowed, requests_in_window, retry_after_seconds)
        """
        now = time.time()
        window_start = now - window_seconds
        sorted_key = f"sw:{key}"

        pipe = self.r.pipeline()
        # Remove expired entries
        pipe.zremrangebyscore(sorted_key, 0, window_start)
        # Count current entries
        pipe.zcard(sorted_key)
        results = pipe.execute()
        current_count = results[1]

        if current_count + cost <= limit:
            # Add current request(s)
            pipe = self.r.pipeline()
            for _ in range(cost):
                pipe.zadd(sorted_key, {f"{now}-{id(pipe)}": now})
            pipe.expire(sorted_key, window_seconds + 1)
            pipe.execute()
            return True, current_count + cost, 0.0
        else:
            # Find oldest entry to calculate retry-after
            oldest = self.r.zrange(sorted_key, 0, 0, withscores=True)
            retry_after = 0.0
            if oldest:
                oldest_ts = oldest[0][1]
                retry_after = max(0.0, oldest_ts + window_seconds - now)
            return False, current_count, retry_after

    def count(self, key: str, window_seconds: int) -> int:
        """Current request count in the sliding window."""
        now = time.time()
        window_start = now - window_seconds
        return self.r.zcount(f"sw:{key}", window_start, "+inf")

    def reset(self, key: str) -> None:
        self.r.delete(f"sw:{key}")


class SlidingWindowCounter:
    """Approximate sliding window using two counters.

    Memory: O(1) — fixed 2 counters regardless of traffic.
    More efficient than log, slight approximation at window boundary.

    Formula: weighted_count = current + previous * (1 - elapsed/window)
    """

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.r = redis.from_url(redis_url)

    def is_allowed(
        self,
        key: str,
        limit: int,
        window_seconds: int,
    ) -> Tuple[bool, float]:
        """Check sliding window counter.

        Returns:
            (allowed, approximate_count)
        """
        now = time.time()
        current_window = int(now // window_seconds)
        prev_window = current_window - 1

        curr_key = f"swc:{key}:{current_window}"
        prev_key = f"swc:{key}:{prev_window}"

        pipe = self.r.pipeline()
        pipe.get(curr_key)
        pipe.get(prev_key)
        results = pipe.execute()

        current_count = int(results[0] or 0)
        prev_count = int(results[1] or 0)

        # Sliding weight: fraction of previous window still in range
        elapsed = now % window_seconds
        weight = 1 - elapsed / window_seconds
        approx_count = current_count + prev_count * weight

        if approx_count < limit:
            pipe = self.r.pipeline()
            pipe.incr(curr_key)
            pipe.expire(curr_key, window_seconds * 2)
            pipe.execute()
            return True, approx_count + 1
        return False, approx_count


if __name__ == "__main__":
    print("=" * 55)
    print("Sliding Window Log Rate Limiter (10 req / 10 seconds)")
    print("=" * 55)

    limiter = SlidingWindowLog()
    limiter.reset("test-user")

    for i in range(15):
        allowed, count, retry_after = limiter.is_allowed("test-user", limit=10, window_seconds=10)
        status = "✓" if allowed else "✗"
        retry_str = f" retry in {retry_after:.1f}s" if not allowed else ""
        print(f"  Request {i+1:02d}: {status} (count={count}{retry_str})")
        time.sleep(0.2)

    print("\n" + "=" * 55)
    print("Sliding Window Counter (approximate, 5 req / 5 seconds)")
    print("=" * 55)
    counter = SlidingWindowCounter()
    for i in range(10):
        allowed, count = counter.is_allowed("test-counter", limit=5, window_seconds=5)
        status = "✓" if allowed else "✗"
        print(f"  Request {i+1:02d}: {status} (approx_count={count:.1f})")
        time.sleep(0.3)

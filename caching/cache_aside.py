"""Cache-aside (lazy loading) pattern with TTL and cache invalidation.

Cache-aside is the most common caching pattern:
1. Application checks cache first
2. On cache miss: fetch from source (DB/API), write to cache
3. On cache hit: return cached value

Best for: read-heavy workloads, data that changes infrequently.
"""
import json
import time
import hashlib
from typing import Any, Callable, Optional
import redis


class CacheAside:
    """Generic cache-aside wrapper with TTL, compression, and invalidation.

    Example:
        cache = CacheAside(redis_url="redis://localhost:6379", ttl=300)

        def get_user_from_db(user_id):
            return {"id": user_id, "name": "Alice"}

        user = cache.get(f"user:{user_id}", lambda: get_user_from_db(user_id))
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        ttl: int = 300,
        key_prefix: str = "ca:",
    ):
        self.r = redis.from_url(redis_url, decode_responses=True)
        self.ttl = ttl
        self.key_prefix = key_prefix
        self._hits = 0
        self._misses = 0

    def _full_key(self, key: str) -> str:
        return f"{self.key_prefix}{key}"

    def get(
        self,
        key: str,
        fetch_fn: Callable[[], Any],
        ttl: Optional[int] = None,
        cache_none: bool = False,
    ) -> Any:
        """Try cache first, fall back to fetch_fn, cache result.

        Args:
            key: Cache key
            fetch_fn: Callable to fetch data on cache miss
            ttl: Override default TTL for this key
            cache_none: Whether to cache None results

        Returns:
            Cached or freshly fetched value
        """
        full_key = self._full_key(key)
        cached = self.r.get(full_key)

        if cached is not None:
            self._hits += 1
            return json.loads(cached)

        # Cache miss — fetch from source
        self._misses += 1
        value = fetch_fn()

        if value is not None or cache_none:
            self.r.setex(
                full_key,
                ttl or self.ttl,
                json.dumps(value),
            )

        return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Manually set a cache value."""
        self.r.setex(self._full_key(key), ttl or self.ttl, json.dumps(value))

    def invalidate(self, key: str) -> bool:
        """Remove a single key from cache. Returns True if key existed."""
        return bool(self.r.delete(self._full_key(key)))

    def invalidate_pattern(self, pattern: str) -> int:
        """Remove all keys matching pattern. Returns count of deleted keys.

        Warning: KEYS command scans full keyspace — use SCAN in production.
        """
        full_pattern = self._full_key(pattern)
        keys = list(self.r.scan_iter(full_pattern))
        if keys:
            return self.r.delete(*keys)
        return 0

    def invalidate_tags(self, tag: str) -> int:
        """Invalidate all keys associated with a tag (stored in a Redis set)."""
        tag_key = f"tag:{tag}"
        keys = self.r.smembers(tag_key)
        if not keys:
            return 0
        deleted = self.r.delete(*[self._full_key(k) for k in keys], tag_key)
        return deleted

    def get_with_tag(self, key: str, fetch_fn: Callable, tags: list, ttl: Optional[int] = None) -> Any:
        """Cache with tag association for bulk invalidation."""
        result = self.get(key, fetch_fn, ttl)
        # Register this key under each tag
        for tag in tags:
            self.r.sadd(f"tag:{tag}", key)
            self.r.expire(f"tag:{tag}", (ttl or self.ttl) + 60)
        return result

    def hit_rate(self) -> float:
        """Cache hit rate as a percentage."""
        total = self._hits + self._misses
        return (self._hits / total * 100) if total > 0 else 0.0

    def stats(self) -> dict:
        """Return cache statistics."""
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{self.hit_rate():.1f}%",
        }

    def ttl_remaining(self, key: str) -> int:
        """Seconds until key expires. -1=no expiry, -2=doesn't exist."""
        return self.r.ttl(self._full_key(key))

    def mget(self, keys: list, fetch_fn: Callable[[list], dict], ttl: Optional[int] = None) -> dict:
        """Batch get: fetch all missing keys in one call.

        Args:
            keys: List of cache keys
            fetch_fn: Called with list of missing keys, returns {key: value}
        """
        full_keys = [self._full_key(k) for k in keys]
        cached_values = self.r.mget(full_keys)

        result = {}
        missing_keys = []

        for key, cached in zip(keys, cached_values):
            if cached is not None:
                result[key] = json.loads(cached)
                self._hits += 1
            else:
                missing_keys.append(key)
                self._misses += 1

        if missing_keys:
            fresh_values = fetch_fn(missing_keys)
            pipe = self.r.pipeline()
            for key, value in fresh_values.items():
                if value is not None:
                    pipe.setex(self._full_key(key), ttl or self.ttl, json.dumps(value))
                result[key] = value
            pipe.execute()

        return result


# ─────────────────────────── Demo ─────────────────────────────────────
if __name__ == "__main__":
    cache = CacheAside(ttl=10)

    call_count = 0

    def expensive_db_query(user_id: int) -> dict:
        global call_count
        call_count += 1
        time.sleep(0.1)  # simulate 100ms DB query
        return {"id": user_id, "name": f"User {user_id}", "email": f"user{user_id}@example.com"}

    print("=" * 50)
    print("Cache-Aside Pattern Demo")
    print("=" * 50)

    print("\nFirst call (cache miss):")
    t = time.perf_counter()
    result = cache.get("user:1", lambda: expensive_db_query(1))
    print(f"  Result: {result}")
    print(f"  Time: {time.perf_counter()-t:.3f}s | DB calls: {call_count}")

    print("\nSecond call (cache hit):")
    t = time.perf_counter()
    result = cache.get("user:1", lambda: expensive_db_query(1))
    print(f"  Result: {result}")
    print(f"  Time: {time.perf_counter()-t:.3f}s | DB calls: {call_count}")

    print("\nInvalidate and refetch:")
    cache.invalidate("user:1")
    t = time.perf_counter()
    result = cache.get("user:1", lambda: expensive_db_query(1))
    print(f"  Time: {time.perf_counter()-t:.3f}s | DB calls: {call_count}")

    print(f"\nStats: {cache.stats()}")

    # Batch get demo
    print("\nBatch get (mget):")
    def batch_fetch(keys):
        return {k: {"id": k, "data": f"value for {k}"} for k in keys}

    results = cache.mget(["item:1", "item:2", "item:3"], batch_fetch)
    print(f"  Fetched {len(results)} items")
    results2 = cache.mget(["item:1", "item:2", "item:4"], batch_fetch)
    print(f"  Second batch: {cache.stats()}")

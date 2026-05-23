"""Token bucket rate limiter using Redis — allows controlled bursting.

Token Bucket Algorithm:
- Bucket holds up to 'capacity' tokens
- Tokens refill at 'rate' tokens/second
- Each request consumes 'cost' tokens
- If insufficient tokens: request is rejected

Advantages over fixed window:
- Allows temporary bursting (up to capacity)
- Smooth, predictable rate limiting
- Fair across time
"""
import time
import redis
from typing import Tuple


class TokenBucketLimiter:
    """Redis-backed token bucket rate limiter.

    Example:
        limiter = TokenBucketLimiter()
        allowed, tokens_left = limiter.is_allowed("user:123", rate=10, capacity=50)
        if not allowed:
            return 429, "Rate limit exceeded"
    """

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.r = redis.from_url(redis_url)

    def is_allowed(
        self,
        key: str,
        rate: float,
        capacity: float,
        cost: float = 1.0,
    ) -> Tuple[bool, float]:
        """Check if a request is allowed under the token bucket policy.

        Uses Redis HSET for atomic reads. For strict atomicity in high
        concurrency, use a Lua script (see is_allowed_atomic).

        Args:
            key: Unique identifier (user ID, IP, API key)
            rate: Token refill rate (tokens per second)
            capacity: Maximum bucket size (burst limit)
            cost: Tokens consumed per request (default 1)

        Returns:
            (allowed: bool, tokens_remaining: float)
        """
        now = time.time()
        bucket_key = f"tb:{key}"

        data = self.r.hgetall(bucket_key)

        if data:
            tokens = float(data.get(b"tokens", capacity))
            last_refill = float(data.get(b"last_refill", now))
            # Refill tokens based on elapsed time
            elapsed = now - last_refill
            tokens = min(capacity, tokens + elapsed * rate)
        else:
            tokens = float(capacity)

        last_refill = now

        if tokens >= cost:
            tokens -= cost
            self.r.hset(
                bucket_key,
                mapping={"tokens": str(tokens), "last_refill": str(last_refill)},
            )
            # TTL: time for empty bucket to fully refill + buffer
            self.r.expire(bucket_key, int(capacity / rate) + 60)
            return True, tokens
        else:
            # Still update last_refill even on rejection (passive refill)
            self.r.hset(bucket_key, mapping={"tokens": str(tokens), "last_refill": str(last_refill)})
            self.r.expire(bucket_key, int(capacity / rate) + 60)
            return False, tokens

    def is_allowed_atomic(
        self,
        key: str,
        rate: float,
        capacity: float,
        cost: float = 1.0,
    ) -> Tuple[bool, float]:
        """Atomic token bucket using Lua script — safe for high concurrency."""
        lua_script = """
        local key = KEYS[1]
        local rate = tonumber(ARGV[1])
        local capacity = tonumber(ARGV[2])
        local cost = tonumber(ARGV[3])
        local now = tonumber(ARGV[4])

        local data = redis.call('HGETALL', key)
        local tokens = capacity
        local last_refill = now

        if #data > 0 then
            for i = 1, #data, 2 do
                if data[i] == 'tokens' then tokens = tonumber(data[i+1]) end
                if data[i] == 'last_refill' then last_refill = tonumber(data[i+1]) end
            end
            local elapsed = now - last_refill
            tokens = math.min(capacity, tokens + elapsed * rate)
        end

        local allowed = 0
        if tokens >= cost then
            tokens = tokens - cost
            allowed = 1
        end

        local ttl = math.ceil(capacity / rate) + 60
        redis.call('HSET', key, 'tokens', tostring(tokens), 'last_refill', tostring(now))
        redis.call('EXPIRE', key, ttl)

        return {allowed, tostring(tokens)}
        """
        result = self.r.eval(lua_script, 1, f"tb:{key}", rate, capacity, cost, time.time())
        allowed = bool(result[0])
        tokens = float(result[1])
        return allowed, tokens

    def reset(self, key: str) -> None:
        """Reset bucket to full capacity."""
        self.r.delete(f"tb:{key}")

    def remaining_tokens(self, key: str, rate: float, capacity: float) -> float:
        """Get current token count (approximately)."""
        data = self.r.hgetall(f"tb:{key}")
        if not data:
            return capacity
        tokens = float(data.get(b"tokens", 0))
        last_refill = float(data.get(b"last_refill", time.time()))
        elapsed = time.time() - last_refill
        return min(capacity, tokens + elapsed * rate)

    def retry_after(self, key: str, rate: float, capacity: float, cost: float = 1.0) -> float:
        """Seconds until next request would be allowed."""
        current = self.remaining_tokens(key, rate, capacity)
        deficit = cost - current
        return max(0.0, deficit / rate)


if __name__ == "__main__":
    limiter = TokenBucketLimiter()

    print("=" * 55)
    print("Token Bucket Rate Limiter (5 req/sec, burst 10)")
    print("=" * 55)

    # Reset from previous runs
    limiter.reset("demo-user")

    allowed_count = 0
    blocked_count = 0

    for i in range(20):
        allowed, remaining = limiter.is_allowed("demo-user", rate=5, capacity=10)
        status = "✓ ALLOWED" if allowed else "✗ BLOCKED"
        if allowed:
            allowed_count += 1
        else:
            blocked_count += 1
        print(f"  Request {i+1:02d}: {status} (tokens: {remaining:.2f})")
        time.sleep(0.1)  # 10 requests/sec > 5 tok/sec limit → burst then block

    print(f"\nSummary: {allowed_count} allowed, {blocked_count} blocked")
    print(f"Retry after: {limiter.retry_after('demo-user', rate=5, capacity=10):.2f}s")

    print("\n--- Atomic Lua script version ---")
    limiter.reset("demo-user-atomic")
    for i in range(5):
        allowed, remaining = limiter.is_allowed_atomic("demo-user-atomic", rate=2, capacity=5)
        status = "✓" if allowed else "✗"
        print(f"  {status} tokens={remaining:.2f}")
        time.sleep(0.2)

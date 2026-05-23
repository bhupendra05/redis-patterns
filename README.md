# redis-patterns

Production-ready Redis patterns in Python: caching, rate limiting, leaderboards, distributed locks, and pub/sub.

## Patterns

| Pattern | File | Redis Data Structure | Use Case |
|---------|------|---------------------|----------|
| Cache-Aside | `caching/cache_aside.py` | String (GET/SET) | DB query caching, API response cache |
| Token Bucket | `rate_limiting/token_bucket.py` | Hash (HSET/HGET) | API rate limiting with burst |
| Sliding Window Log | `rate_limiting/sliding_window.py` | Sorted Set (ZADD/ZCARD) | Strict per-second rate limits |
| Leaderboard | `leaderboard/leaderboard.py` | Sorted Set (ZADD/ZREVRANGE) | Games, rankings, scores |
| Distributed Lock | `distributed_lock/redlock.py` | String (SET NX PX) | Prevent duplicate jobs, mutual exclusion |
| Pub/Sub | `pubsub/pub.py`, `pubsub/sub.py` | Pub/Sub channels | Real-time events, notifications |

## Quick Start

```bash
# Start Redis
docker run -d -p 6379:6379 redis:7-alpine
# OR: brew install redis && redis-server

# Install
pip install -r requirements.txt  # just: redis>=5.0.0

# Run any demo
python caching/cache_aside.py
python leaderboard/leaderboard.py
python distributed_lock/redlock.py
```

## Cache-Aside Pattern

The most common caching pattern — check cache first, fall back to source on miss.

```python
from caching.cache_aside import CacheAside

cache = CacheAside(redis_url="redis://localhost:6379", ttl=300)

# Simple usage
def get_user(user_id):
    return cache.get(
        f"user:{user_id}",
        fetch_fn=lambda: db.query(f"SELECT * FROM users WHERE id={user_id}"),
        ttl=600,  # 10 minutes
    )

# Cache invalidation
cache.invalidate("user:42")
cache.invalidate_pattern("user:*")  # invalidate all user keys

# Batch get (one round-trip for misses)
users = cache.mget(
    keys=["user:1", "user:2", "user:3"],
    fetch_fn=lambda missing_keys: db.batch_fetch(missing_keys),
)

# Stats
print(cache.stats())  # {'hits': 45, 'misses': 5, 'hit_rate': '90.0%'}
```

## Token Bucket Rate Limiter

Allows bursting up to `capacity`, then enforces `rate` requests/sec.

```python
from rate_limiting.token_bucket import TokenBucketLimiter

limiter = TokenBucketLimiter()

def api_handler(user_id, request):
    allowed, tokens_left = limiter.is_allowed(
        key=f"user:{user_id}",
        rate=10,        # 10 requests/sec steady state
        capacity=50,    # burst up to 50 requests
        cost=1,         # each request costs 1 token
    )
    if not allowed:
        retry_in = limiter.retry_after(f"user:{user_id}", rate=10, capacity=50)
        return 429, f"Rate limited. Retry in {retry_in:.1f}s"
    return process(request)

# Atomic Lua script version (safe for high concurrency)
allowed, tokens = limiter.is_allowed_atomic("user:123", rate=10, capacity=50)
```

## Sliding Window Log Rate Limiter

Exact count within a sliding window — no burst at window reset boundaries.

```python
from rate_limiting.sliding_window import SlidingWindowLog, SlidingWindowCounter

# Exact (stores one entry per request)
limiter = SlidingWindowLog()
allowed, count, retry_after = limiter.is_allowed(
    "user:123", limit=100, window_seconds=60
)

# Approximate counter (O(1) memory)
counter = SlidingWindowCounter()
allowed, approx_count = counter.is_allowed("user:123", limit=100, window_seconds=60)
```

## Leaderboard

Real-time rankings using Redis sorted sets.

```python
from leaderboard.leaderboard import Leaderboard, MultiPeriodLeaderboard

lb = Leaderboard("game-scores")

# Add/update scores
lb.add_score("alice", 1500)
lb.add_score("bob", 2300)
lb.add_score("alice", 200, increment=True)  # alice now has 1700

# Query
lb.top_n(10)                    # → [("bob", 2300, 1), ("alice", 1700, 2), ...]
lb.get_rank("alice")            # → 2
lb.get_score("alice")           # → 1700.0
lb.around_user("alice", n=3)    # 3 above and below alice
lb.percentile_rank("alice")     # → 65.0 (top 35%)
lb.in_range(1000, 2000)        # all players scoring 1000-2000

# Daily/Weekly/Monthly/All-time
multi = MultiPeriodLeaderboard("game")
multi.record_score("alice", 500)
print(multi.daily.top_n(10))
print(multi.alltime.top_n(10))
```

## Distributed Lock

Prevent concurrent execution across multiple processes/servers.

```python
from distributed_lock.redlock import RedisLock

lock = RedisLock()

# Context manager (recommended — auto-releases on error)
with lock.acquire("payment:order:123", ttl=30000) as acquired:
    if acquired:
        process_payment()  # only one process runs this at a time
    else:
        return "Concurrent modification detected"

# Manual (for fine-grained control)
token = lock.acquire_manual("cron:daily-report", ttl=60000)
if token:
    try:
        run_report()
    finally:
        lock.release("cron:daily-report", token)

# Extend a running lock
lock.extend("cron:daily-report", token, additional_ms=30000)
```

## Pub/Sub

Decouple producers from consumers with Redis Pub/Sub.

```python
# Publisher (pub.py)
from pubsub.pub import Publisher

pub = Publisher()
pub.publish_typed("order-events", "order.created", {
    "order_id": 1001, "customer_id": 42, "total": 99.99
})

# Subscriber (sub.py)
from pubsub.sub import Subscriber

sub = Subscriber()
sub.subscribe("order-events", handler=lambda msg: process_order(msg["payload"]))
sub.subscribe_pattern("user.*", handler=lambda msg: update_analytics(msg))
sub.listen()  # blocking

# Background listener
thread = sub.listen_in_background()
```

## When to Use Each Pattern

| Pattern | When |
|---------|------|
| Cache-Aside | DB queries > 10ms, repeated reads, data changes infrequently |
| Token Bucket | API rate limits, allow bursting, smooth overall rate |
| Sliding Window | Strict per-second limits, financial transactions |
| Leaderboard | Any real-time ranking — games, sales, engagement |
| Distributed Lock | Exactly-once job execution, inventory management |
| Pub/Sub | Real-time notifications, decoupled microservices, event streaming |

## Requirements

- Python 3.9+
- Redis 6.0+ (for Lua script support)
- `redis>=5.0.0`

```bash
# Docker Redis (recommended for dev)
docker run -d --name redis -p 6379:6379 redis:7-alpine
redis-cli ping  # → PONG
```

## License

MIT

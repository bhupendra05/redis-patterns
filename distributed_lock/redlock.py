"""Distributed lock using Redis — Redlock algorithm.

Redlock algorithm (multi-instance):
1. Get current time T1
2. Try to acquire lock in N Redis instances with TTL
3. Lock is acquired if N/2+1 instances succeed AND elapsed < TTL
4. If failed, release all partial locks

Single-instance simplified version (for single Redis deployments).
"""
import uuid
import time
import redis
from contextlib import contextmanager
from typing import Optional


class RedisLock:
    """Single-instance Redis distributed lock using SET NX PX.

    SET key value NX PX ms atomically:
    - NX: only set if not exists (prevents overwriting)
    - PX: set expiry in milliseconds (prevents deadlock on crash)
    - Value: unique token (prevents releasing someone else's lock)

    Example:
        lock = RedisLock(redis_url="redis://localhost:6379")

        # Context manager (recommended)
        with lock.acquire("payment:user:123", ttl=30000) as acquired:
            if acquired:
                process_payment()

        # Manual
        token = lock.acquire_manual("job:42", ttl=10000)
        if token:
            try:
                do_work()
            finally:
                lock.release("job:42", token)
    """

    RELEASE_SCRIPT = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    """

    EXTEND_SCRIPT = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("pexpire", KEYS[1], ARGV[2])
    else
        return 0
    end
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        retry_delay: float = 0.1,
        retry_times: int = 3,
    ):
        self.r = redis.from_url(redis_url)
        self.retry_delay = retry_delay
        self.retry_times = retry_times

    def acquire_manual(
        self,
        key: str,
        ttl: int = 30000,
        retry: bool = True,
    ) -> Optional[str]:
        """Try to acquire lock. Returns unique token if acquired, None if failed.

        Args:
            key: Lock name
            ttl: Lock expiry in milliseconds (prevents deadlocks on crash)
            retry: Whether to retry on failure

        Returns:
            Token string (pass to release()) or None if lock not acquired
        """
        token = str(uuid.uuid4())
        attempts = self.retry_times if retry else 1

        for attempt in range(attempts):
            acquired = self.r.set(key, token, px=ttl, nx=True)
            if acquired:
                return token
            if attempt < attempts - 1:
                time.sleep(self.retry_delay)
        return None

    def release(self, key: str, token: str) -> bool:
        """Release lock only if we own it (compare-and-delete via Lua).

        Args:
            key: Lock name
            token: Token returned by acquire_manual()

        Returns:
            True if lock was released, False if lock was not ours
        """
        result = self.r.eval(self.RELEASE_SCRIPT, 1, key, token)
        return bool(result)

    def extend(self, key: str, token: str, additional_ms: int) -> bool:
        """Extend lock TTL if we still own it.

        Useful for long-running jobs that need more time.

        Returns:
            True if extension succeeded
        """
        result = self.r.eval(self.EXTEND_SCRIPT, 1, key, token, additional_ms)
        return bool(result)

    def is_locked(self, key: str) -> bool:
        """Check if a lock is currently held."""
        return self.r.exists(key) > 0

    def ttl_ms(self, key: str) -> int:
        """Remaining TTL in milliseconds. -2 = doesn't exist, -1 = no TTL."""
        return self.r.pttl(key)

    @contextmanager
    def acquire(self, key: str, ttl: int = 30000, retry: bool = True):
        """Context manager for lock acquisition.

        Example:
            with lock.acquire("resource:123") as acquired:
                if acquired:
                    do_exclusive_work()
        """
        token = self.acquire_manual(key, ttl, retry)
        acquired = token is not None
        try:
            yield acquired
        finally:
            if acquired and token:
                self.release(key, token)

    @contextmanager
    def must_acquire(self, key: str, ttl: int = 30000):
        """Context manager that raises if lock cannot be acquired.

        Raises:
            RuntimeError: If lock acquisition fails after retries
        """
        token = self.acquire_manual(key, ttl, retry=True)
        if token is None:
            raise RuntimeError(f"Could not acquire lock: {key}")
        try:
            yield token
        finally:
            self.release(key, token)


class Redlock:
    """Multi-instance Redlock algorithm for high availability.

    Uses N independent Redis instances. Lock is valid only if
    acquired on majority (N//2 + 1) within time limit.

    For production: use N=5 Redis instances in different availability zones.
    """

    def __init__(self, redis_urls: list):
        self.instances = [redis.from_url(url) for url in redis_urls]
        self.quorum = len(self.instances) // 2 + 1

    def _acquire_one(self, r: redis.Redis, key: str, token: str, ttl: int) -> bool:
        try:
            return bool(r.set(key, token, px=ttl, nx=True))
        except Exception:
            return False

    def _release_one(self, r: redis.Redis, key: str, token: str) -> None:
        try:
            script = "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end"
            r.eval(script, 1, key, token)
        except Exception:
            pass

    def acquire(self, key: str, ttl: int = 10000) -> Optional[str]:
        """Try to acquire Redlock across all instances.

        Returns:
            Token if lock acquired, None otherwise
        """
        token = str(uuid.uuid4())
        start = time.perf_counter()
        acquired_count = 0

        for r in self.instances:
            if self._acquire_one(r, key, token, ttl):
                acquired_count += 1

        elapsed_ms = (time.perf_counter() - start) * 1000
        validity_time = ttl - elapsed_ms - 2  # 2ms clock drift buffer

        if acquired_count >= self.quorum and validity_time > 0:
            return token

        # Failed — release all partial locks
        self.release(key, token)
        return None

    def release(self, key: str, token: str) -> None:
        """Release lock on all instances."""
        for r in self.instances:
            self._release_one(r, key, token)


if __name__ == "__main__":
    lock = RedisLock()

    print("=" * 50)
    print("Distributed Lock Demo")
    print("=" * 50)

    # Basic usage
    print("\n1. Basic lock/release:")
    token = lock.acquire_manual("test-resource", ttl=5000)
    if token:
        print(f"  Lock acquired: {token[:16]}...")
        print(f"  TTL: {lock.ttl_ms('test-resource')}ms")
        print(f"  Is locked: {lock.is_locked('test-resource')}")
        released = lock.release("test-resource", token)
        print(f"  Released: {released}")
        print(f"  Is locked after release: {lock.is_locked('test-resource')}")

    # Context manager
    print("\n2. Context manager:")
    with lock.acquire("shared-job", ttl=5000) as acquired:
        print(f"  Acquired: {acquired}")
        if acquired:
            print("  Doing exclusive work...")
            time.sleep(0.1)
    print("  Lock released automatically")

    # Cannot steal someone else's lock
    print("\n3. Lock safety (cannot release others' lock):")
    token1 = lock.acquire_manual("exclusive-key", ttl=5000)
    fake_token = str(uuid.uuid4())
    released = lock.release("exclusive-key", fake_token)
    print(f"  Release with wrong token: {released}")
    released = lock.release("exclusive-key", token1)
    print(f"  Release with correct token: {released}")

    # Contention simulation
    print("\n4. Contention (first caller wins):")
    results = []
    for i in range(5):
        t = lock.acquire_manual("contested-resource", ttl=2000, retry=False)
        results.append(("acquired" if t else "blocked", t))

    for i, (status, token) in enumerate(results):
        print(f"  Thread {i+1}: {status}")
        if token:
            lock.release("contested-resource", token)

"""Real-time leaderboard using Redis sorted sets (ZADD/ZRANGE).

Redis sorted sets are perfect for leaderboards:
- ZADD: O(log n) — add/update score
- ZRANK/ZREVRANK: O(log n) — get rank
- ZRANGE/ZREVRANGE: O(log n + k) — get top-k
- ZINCRBY: O(log n) — atomic score increment

All operations are O(log n) — fast even with millions of players.
"""
import time
import redis
from typing import List, Optional, Tuple


class Leaderboard:
    """Real-time leaderboard backed by Redis sorted sets.

    Example:
        lb = Leaderboard("game-daily")
        lb.add_score("alice", 1500)
        lb.add_score("bob", 2300)
        print(lb.top_n(10))
        print(lb.get_rank("alice"))
    """

    def __init__(self, name: str, redis_url: str = "redis://localhost:6379"):
        self.name = name
        self.key = f"leaderboard:{name}"
        self.r = redis.from_url(redis_url)

    def add_score(self, user_id: str, score: float, increment: bool = False) -> float:
        """Set or increment a player's score.

        Args:
            user_id: Player identifier
            score: Score value (or delta if increment=True)
            increment: If True, add score to existing score

        Returns:
            New score
        """
        if increment:
            return float(self.r.zincrby(self.key, score, user_id))
        self.r.zadd(self.key, {user_id: score})
        return score

    def add_scores_batch(self, scores: dict) -> None:
        """Add multiple scores in one call. scores = {user_id: score}"""
        self.r.zadd(self.key, scores)

    def get_rank(self, user_id: str) -> Optional[int]:
        """Get 1-based rank (1 = first place). Returns None if not in leaderboard."""
        rank = self.r.zrevrank(self.key, user_id)
        return rank + 1 if rank is not None else None

    def get_score(self, user_id: str) -> Optional[float]:
        """Get a player's score. Returns None if not in leaderboard."""
        score = self.r.zscore(self.key, user_id)
        return float(score) if score is not None else None

    def top_n(self, n: int = 10) -> List[Tuple[str, float, int]]:
        """Get top N players.

        Returns:
            List of (user_id, score, rank) tuples
        """
        results = self.r.zrevrange(self.key, 0, n - 1, withscores=True)
        return [
            (uid.decode() if isinstance(uid, bytes) else uid, score, rank + 1)
            for rank, (uid, score) in enumerate(results)
        ]

    def bottom_n(self, n: int = 10) -> List[Tuple[str, float, int]]:
        """Get bottom N players (lowest scores first)."""
        total = self.r.zcard(self.key)
        results = self.r.zrange(self.key, 0, n - 1, withscores=True)
        return [
            (uid.decode() if isinstance(uid, bytes) else uid, score, total - i)
            for i, (uid, score) in enumerate(results)
        ]

    def around_user(self, user_id: str, n: int = 5) -> List[Tuple[str, float, int]]:
        """Get n players above and below a given player.

        Returns:
            List of (user_id, score, rank) tuples centered on user
        """
        rank = self.r.zrevrank(self.key, user_id)
        if rank is None:
            return []
        total = self.r.zcard(self.key)
        start = max(0, rank - n)
        end = min(total - 1, rank + n)
        results = self.r.zrevrange(self.key, start, end, withscores=True)
        return [
            (
                uid.decode() if isinstance(uid, bytes) else uid,
                score,
                start + i + 1,
            )
            for i, (uid, score) in enumerate(results)
        ]

    def count(self) -> int:
        """Total number of players in the leaderboard."""
        return self.r.zcard(self.key)

    def in_range(self, min_score: float, max_score: float) -> List[Tuple[str, float]]:
        """Get all players with scores in [min_score, max_score]."""
        results = self.r.zrangebyscore(self.key, min_score, max_score, withscores=True)
        return [(uid.decode() if isinstance(uid, bytes) else uid, score) for uid, score in results]

    def remove(self, user_id: str) -> bool:
        """Remove a player from the leaderboard."""
        return bool(self.r.zrem(self.key, user_id))

    def clear(self) -> None:
        """Clear the entire leaderboard."""
        self.r.delete(self.key)

    def percentile_rank(self, user_id: str) -> Optional[float]:
        """Get percentile rank (0-100). 100 = top player."""
        rank = self.r.zrevrank(self.key, user_id)
        if rank is None:
            return None
        total = self.r.zcard(self.key)
        return (1 - rank / total) * 100 if total > 0 else 100.0

    def expire_at(self, unix_timestamp: int) -> None:
        """Set the leaderboard to expire at a Unix timestamp."""
        self.r.expireat(self.key, unix_timestamp)

    def expire_in(self, seconds: int) -> None:
        """Set the leaderboard to expire in N seconds (for daily/weekly boards)."""
        self.r.expire(self.key, seconds)


class MultiPeriodLeaderboard:
    """Manages daily, weekly, monthly, and all-time leaderboards simultaneously.

    Example:
        lb = MultiPeriodLeaderboard("game")
        lb.record_score("alice", 500)
        print(lb.daily.top_n(10))
        print(lb.weekly.top_n(10))
    """

    def __init__(self, name: str, redis_url: str = "redis://localhost:6379"):
        self.name = name
        self.redis_url = redis_url
        self._update_boards()

    def _update_boards(self) -> None:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        day_str = now.strftime("%Y-%m-%d")
        week_str = f"{now.year}-W{now.isocalendar()[1]:02d}"
        month_str = now.strftime("%Y-%m")

        self.daily = Leaderboard(f"{self.name}:daily:{day_str}", self.redis_url)
        self.weekly = Leaderboard(f"{self.name}:weekly:{week_str}", self.redis_url)
        self.monthly = Leaderboard(f"{self.name}:monthly:{month_str}", self.redis_url)
        self.alltime = Leaderboard(f"{self.name}:alltime", self.redis_url)

    def record_score(self, user_id: str, score: float) -> None:
        """Record a score across all time periods."""
        self._update_boards()
        for board in [self.daily, self.weekly, self.monthly, self.alltime]:
            board.add_score(user_id, score, increment=True)


if __name__ == "__main__":
    lb = Leaderboard("demo-game")
    lb.clear()

    print("=" * 50)
    print("Leaderboard Demo (Redis sorted sets)")
    print("=" * 50)

    # Add players
    players = [
        ("alice", 1500), ("bob", 2300), ("carol", 1800),
        ("dave", 900), ("eve", 2100), ("frank", 2800),
        ("grace", 750), ("henry", 3200), ("iris", 1200),
    ]
    lb.add_scores_batch(dict(players))

    print("\nTop 5:")
    for uid, score, rank in lb.top_n(5):
        print(f"  #{rank} {uid}: {score:.0f}")

    print(f"\nTotal players: {lb.count()}")
    print(f"Alice's rank: #{lb.get_rank('alice')}")
    print(f"Alice's score: {lb.get_score('alice'):.0f}")
    print(f"Alice's percentile: {lb.percentile_rank('alice'):.1f}%")

    print("\nAround 'carol' (±2 places):")
    for uid, score, rank in lb.around_user("carol", n=2):
        marker = " ← carol" if uid == "carol" else ""
        print(f"  #{rank} {uid}: {score:.0f}{marker}")

    print("\nPlayers scoring 1000-2000:")
    for uid, score in lb.in_range(1000, 2000):
        print(f"  {uid}: {score:.0f}")

    # Increment
    new_score = lb.add_score("alice", 500, increment=True)
    print(f"\nAlice gets +500 → new score: {new_score:.0f}, rank: #{lb.get_rank('alice')}")

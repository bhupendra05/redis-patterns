"""Redis Pub/Sub publisher.

Pub/Sub decouples producers from consumers:
- Publisher sends to a channel — no knowledge of subscribers
- Subscribers receive messages from channels they subscribe to
- Messages are NOT persisted — if no subscriber, message is lost
- For persistence, use Redis Streams instead

Run subscriber first: python sub.py
Then run publisher: python pub.py
"""
import json
import time
import redis


class Publisher:
    """Redis Pub/Sub message publisher.

    Example:
        pub = Publisher()
        pub.publish("notifications", {"type": "email", "to": "alice@example.com"})
    """

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.r = redis.from_url(redis_url)

    def publish(self, channel: str, message: dict) -> int:
        """Publish a message to a channel.

        Args:
            channel: Channel name
            message: Dict to serialize as JSON

        Returns:
            Number of subscribers that received the message
        """
        payload = json.dumps({"timestamp": time.time(), "data": message})
        receivers = self.r.publish(channel, payload)
        return receivers

    def publish_typed(self, channel: str, event_type: str, data: dict) -> int:
        """Publish a typed event with standard envelope."""
        message = {
            "event": event_type,
            "payload": data,
            "timestamp": time.time(),
        }
        return self.r.publish(channel, json.dumps(message))

    def subscriber_count(self, channel: str) -> int:
        """Get number of active subscribers on a channel."""
        info = self.r.execute_command("PUBSUB", "NUMSUB", channel)
        return info[1] if len(info) >= 2 else 0


if __name__ == "__main__":
    pub = Publisher()

    print("Publisher started. Sending events every second...")
    print("Start sub.py to receive messages.\n")

    events = [
        ("order-events",    "order.created",   {"order_id": 1001, "customer": "alice", "total": 99.99}),
        ("order-events",    "order.shipped",   {"order_id": 1001, "tracking": "TRK123"}),
        ("user-events",     "user.login",      {"user_id": 42, "ip": "192.168.1.1"}),
        ("notifications",   "email.welcome",   {"to": "alice@example.com", "template": "welcome"}),
        ("order-events",    "order.delivered", {"order_id": 1001, "delivered_at": "2024-01-15T10:00:00Z"}),
        ("user-events",     "user.logout",     {"user_id": 42}),
        ("notifications",   "push.promo",      {"user_id": 42, "message": "20% off this weekend!"}),
    ]

    for channel, event_type, data in events:
        receivers = pub.publish_typed(channel, event_type, data)
        print(f"  [{channel}] {event_type} → {receivers} subscriber(s) received")
        time.sleep(1)

    print("\nSending shutdown signal...")
    pub.publish_typed("system", "shutdown", {"reason": "demo complete"})

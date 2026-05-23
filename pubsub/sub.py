"""Redis Pub/Sub subscriber with pattern matching.

Two subscription modes:
1. Exact channel: SUBSCRIBE channel → exact match
2. Pattern: PSUBSCRIBE pattern → glob-style matching
   e.g., "order.*" matches "order.created", "order.shipped"

Important: Pub/Sub messages are NOT persisted.
If subscriber is offline, messages are lost.
Use Redis Streams for guaranteed delivery.

Run this before pub.py:
    python sub.py
"""
import json
import time
import threading
import redis


class Subscriber:
    """Redis Pub/Sub subscriber with message routing.

    Example:
        sub = Subscriber()
        sub.subscribe("order-events", handler=lambda msg: print(msg))
        sub.subscribe_pattern("user.*", handler=lambda msg: print(msg))
        sub.listen()  # blocking
    """

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.r = redis.from_url(redis_url)
        self.pubsub = self.r.pubsub()
        self._handlers: dict = {}
        self._pattern_handlers: dict = {}
        self._running = False
        self._message_count = 0

    def subscribe(self, channel: str, handler=None) -> None:
        """Subscribe to exact channel.

        Args:
            channel: Exact channel name
            handler: Callback(message_dict) for incoming messages
        """
        self.pubsub.subscribe(channel)
        if handler:
            self._handlers[channel] = handler
        print(f"  Subscribed to channel: {channel}")

    def subscribe_pattern(self, pattern: str, handler=None) -> None:
        """Subscribe to channels matching a glob pattern.

        Args:
            pattern: Glob pattern (e.g., "order.*", "user:*:events")
            handler: Callback(message_dict) for matching messages
        """
        self.pubsub.psubscribe(pattern)
        if handler:
            self._pattern_handlers[pattern] = handler
        print(f"  Subscribed to pattern: {pattern}")

    def unsubscribe(self, channel: str) -> None:
        self.pubsub.unsubscribe(channel)
        self._handlers.pop(channel, None)

    def _dispatch(self, raw_message: dict) -> None:
        """Route message to appropriate handler."""
        msg_type = raw_message.get("type")
        if msg_type not in ("message", "pmessage"):
            return

        self._message_count += 1
        channel = raw_message.get("channel", b"").decode()
        pattern = raw_message.get("pattern")
        data_raw = raw_message.get("data", b"")

        try:
            data = json.loads(data_raw)
        except (json.JSONDecodeError, TypeError):
            data = {"raw": data_raw.decode() if isinstance(data_raw, bytes) else str(data_raw)}

        message = {
            "channel": channel,
            "pattern": pattern.decode() if isinstance(pattern, bytes) else pattern,
            "event": data.get("event", "unknown"),
            "payload": data.get("payload", data),
            "timestamp": data.get("timestamp"),
        }

        # Try exact handler first
        if channel in self._handlers:
            self._handlers[channel](message)
        elif pattern:
            # Try pattern handler
            pattern_str = message["pattern"] or ""
            if pattern_str in self._pattern_handlers:
                self._pattern_handlers[pattern_str](message)
        else:
            # Default handler
            self._default_handler(message)

    def _default_handler(self, message: dict) -> None:
        """Default message handler — prints to stdout."""
        print(f"  [{message['channel']}] {message['event']}: {message['payload']}")

    def listen(self, timeout: float = None) -> None:
        """Listen for messages (blocking).

        Args:
            timeout: Stop after N seconds (None = run forever)
        """
        self._running = True
        start = time.time()
        print("\nListening for messages (Ctrl+C to stop)...\n")

        try:
            for raw_msg in self.pubsub.listen():
                if not self._running:
                    break
                if timeout and (time.time() - start) > timeout:
                    break
                self._dispatch(raw_msg)
        except KeyboardInterrupt:
            print("\nStopped by user.")
        finally:
            self.stop()

    def listen_in_background(self) -> threading.Thread:
        """Start listening in a background thread. Returns the thread."""
        def _run():
            self.listen()
        t = threading.Thread(target=_run, daemon=True)
        t.start()
        return t

    def stop(self) -> None:
        self._running = False
        self.pubsub.close()
        print(f"\nSubscriber stopped. Received {self._message_count} messages.")

    @property
    def message_count(self) -> int:
        return self._message_count


def handle_order_event(message: dict) -> None:
    """Custom handler for order events."""
    event = message["event"]
    payload = message["payload"]
    print(f"  [ORDER] {event}")
    if event == "order.created":
        print(f"    → New order #{payload.get('order_id')} from {payload.get('customer')}")
    elif event == "order.shipped":
        print(f"    → Order #{payload.get('order_id')} shipped: {payload.get('tracking')}")
    elif event == "order.delivered":
        print(f"    → Order #{payload.get('order_id')} delivered!")


def handle_user_event(message: dict) -> None:
    print(f"  [USER] {message['event']}: user_id={message['payload'].get('user_id')}")


def handle_notification(message: dict) -> None:
    print(f"  [NOTIF] {message['event']}: {message['payload']}")


if __name__ == "__main__":
    sub = Subscriber()

    # Subscribe to specific channels with custom handlers
    sub.subscribe("order-events", handler=handle_order_event)
    sub.subscribe("user-events", handler=handle_user_event)
    sub.subscribe("notifications", handler=handle_notification)
    # Pattern subscription: catch all system events
    sub.subscribe_pattern("system", handler=lambda m: print(f"  [SYSTEM] {m['event']}"))

    print("\nSubscriber ready. Run pub.py in another terminal.")
    sub.listen()

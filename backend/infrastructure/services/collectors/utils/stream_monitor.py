"""
Redis Streams monitoring utility.

Provides:
- Stream statistics (length, lag, pending messages)
- Consumer group monitoring
- Dead letter queue detection
- Performance metrics
"""
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from redis import asyncio as aioredis

from settings import logger


@dataclass
class StreamStats:
    """Statistics for a Redis Stream."""
    stream_name: str
    length: int
    first_entry_id: Optional[str]
    last_entry_id: Optional[str]
    consumer_groups: list[dict]
    messages_per_second: float
    timestamp: str


@dataclass
class ConsumerGroupStats:
    """Statistics for a consumer group."""
    group_name: str
    consumers_count: int
    pending_count: int
    last_delivered_id: str
    consumers: list[dict]


class StreamMonitor:
    """Monitor Redis Streams for orderbook pipeline."""

    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client
        self.previous_lengths = {}
        self.previous_timestamps = {}

    async def get_stream_info(self, stream_name: str) -> StreamStats:
        """Get comprehensive statistics for a stream."""
        try:
            # Get basic stream info
            info = await self.redis.xinfo_stream(stream_name)

            # Get consumer groups
            groups_info = []
            try:
                groups = await self.redis.xinfo_groups(stream_name)
                for group in groups:
                    group_stats = await self._get_group_stats(stream_name, group[b"name"].decode())
                    groups_info.append({
                        "name": group[b"name"].decode(),
                        "consumers": group[b"consumers"],
                        "pending": group[b"pending"],
                        "last_delivered_id": group[b"last-delivered-id"].decode(),
                        "stats": group_stats
                    })
            except Exception as e:
                logger.warning(f"Failed to get consumer groups for {stream_name}: {e}")

            # Calculate messages/second
            current_length = info[b"length"]
            current_time = time.time()

            if stream_name in self.previous_lengths:
                time_delta = current_time - self.previous_timestamps[stream_name]
                length_delta = current_length - self.previous_lengths[stream_name]
                msgs_per_sec = length_delta / time_delta if time_delta > 0 else 0
            else:
                msgs_per_sec = 0

            self.previous_lengths[stream_name] = current_length
            self.previous_timestamps[stream_name] = current_time

            return StreamStats(
                stream_name=stream_name,
                length=current_length,
                first_entry_id=info[b"first-entry"][0].decode() if info[b"first-entry"] else None,
                last_entry_id=info[b"last-entry"][0].decode() if info[b"last-entry"] else None,
                consumer_groups=groups_info,
                messages_per_second=msgs_per_sec,
                timestamp=datetime.now().isoformat()
            )

        except Exception as e:
            logger.error(f"Failed to get stream info for {stream_name}: {e}")
            return None

    async def _get_group_stats(self, stream_name: str, group_name: str) -> ConsumerGroupStats:
        """Get detailed statistics for a consumer group."""
        try:
            consumers = await self.redis.xinfo_consumers(stream_name, group_name)
            pending = await self.redis.xpending(stream_name, group_name)

            consumers_info = []
            for consumer in consumers:
                consumers_info.append({
                    "name": consumer[b"name"].decode(),
                    "pending": consumer[b"pending"],
                    "idle": consumer[b"idle"]
                })

            return ConsumerGroupStats(
                group_name=group_name,
                consumers_count=len(consumers),
                pending_count=pending[0] if pending else 0,
                last_delivered_id="",
                consumers=consumers_info
            )

        except Exception as e:
            logger.error(f"Failed to get group stats for {group_name}: {e}")
            return None

    async def monitor_lag(self, stream_name: str, group_name: str) -> dict:
        """
        Monitor consumer group lag.

        Lag = messages in stream that haven't been consumed yet.
        """
        try:
            # Get stream length
            info = await self.redis.xinfo_stream(stream_name)
            stream_length = info[b"length"]

            # Get group info
            groups = await self.redis.xinfo_groups(stream_name)
            group_info = None
            for group in groups:
                if group[b"name"].decode() == group_name:
                    group_info = group
                    break

            if not group_info:
                return {"error": f"Group {group_name} not found"}

            # Get pending messages
            pending = await self.redis.xpending(stream_name, group_name)
            pending_count = pending[0] if pending else 0

            # Calculate lag (approximate)
            # This is a simplified calculation; actual lag depends on consumer position
            last_delivered_id = group_info[b"last-delivered-id"].decode()

            return {
                "stream_name": stream_name,
                "group_name": group_name,
                "stream_length": stream_length,
                "pending_messages": pending_count,
                "last_delivered_id": last_delivered_id,
                "estimated_lag": stream_length - pending_count,
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Failed to monitor lag: {e}")
            return {"error": str(e)}

    async def check_stuck_messages(
        self,
        stream_name: str,
        group_name: str,
        idle_threshold_ms: int = 60000  # 1 minute
    ) -> list[dict]:
        """
        Detect messages stuck in pending state.

        These are messages that were delivered but not acknowledged.
        """
        try:
            # Get pending messages with details
            pending_details = await self.redis.xpending_range(
                stream_name,
                group_name,
                min="-",
                max="+",
                count=100
            )

            stuck_messages = []
            for msg in pending_details:
                message_id = msg[0].decode()
                consumer = msg[1].decode()
                idle_time = msg[2]
                delivery_count = msg[3]

                if idle_time > idle_threshold_ms:
                    stuck_messages.append({
                        "message_id": message_id,
                        "consumer": consumer,
                        "idle_ms": idle_time,
                        "delivery_count": delivery_count
                    })

            return stuck_messages

        except Exception as e:
            logger.error(f"Failed to check stuck messages: {e}")
            return []

    async def claim_stuck_messages(
        self,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        idle_threshold_ms: int = 60000
    ) -> int:
        """
        Claim stuck messages for reprocessing.

        Returns number of messages claimed.
        """
        stuck = await self.check_stuck_messages(stream_name, group_name, idle_threshold_ms)

        if not stuck:
            return 0

        message_ids = [msg["message_id"] for msg in stuck]

        try:
            claimed = await self.redis.xclaim(
                stream_name,
                group_name,
                consumer_name,
                min_idle_time=idle_threshold_ms,
                message_ids=message_ids
            )
            logger.info(f"Claimed {len(claimed)} stuck messages for {consumer_name}")
            return len(claimed)

        except Exception as e:
            logger.error(f"Failed to claim stuck messages: {e}")
            return 0

    async def print_status(self, stream_name: str):
        """Print human-readable stream status."""
        stats = await self.get_stream_info(stream_name)

        if not stats:
            print(f"Stream '{stream_name}' not found or error occurred")
            return

        print(f"\n{'=' * 80}")
        print(f"Stream: {stats.stream_name}")
        print(f"{'=' * 80}")
        print(f"Length: {stats.length} messages")
        print(f"Rate: {stats.messages_per_second:.2f} messages/second")
        print(f"First ID: {stats.first_entry_id}")
        print(f"Last ID: {stats.last_entry_id}")

        if stats.consumer_groups:
            print(f"\nConsumer Groups ({len(stats.consumer_groups)}):")
            for group in stats.consumer_groups:
                print(f"\n  Group: {group['name']}")
                print(f"    Consumers: {group['consumers']}")
                print(f"    Pending: {group['pending']}")
                print(f"    Last delivered: {group['last_delivered_id']}")

                if group.get("stats") and group["stats"].consumers:
                    print(f"    Consumer details:")
                    for consumer in group["stats"].consumers:
                        print(f"      - {consumer['name']}: {consumer['pending']} pending, {consumer['idle']}ms idle")
        else:
            print("\nNo consumer groups")

        print(f"{'=' * 80}\n")

    async def continuous_monitor(
        self,
        stream_name: str,
        interval: int = 10,
        check_stuck: bool = True
    ):
        """
        Continuously monitor stream and print status.

        Args:
            stream_name: Name of stream to monitor
            interval: Check interval in seconds
            check_stuck: Whether to check for stuck messages
        """
        logger.info(f"Starting continuous monitor for {stream_name}")

        while True:
            try:
                await self.print_status(stream_name)

                if check_stuck:
                    # Check all consumer groups for stuck messages
                    stats = await self.get_stream_info(stream_name)
                    if stats and stats.consumer_groups:
                        for group in stats.consumer_groups:
                            stuck = await self.check_stuck_messages(
                                stream_name,
                                group["name"],
                                idle_threshold_ms=60000
                            )
                            if stuck:
                                logger.warning(
                                    f"Found {len(stuck)} stuck messages in group {group['name']}"
                                )
                                for msg in stuck[:5]:  # Show first 5
                                    logger.warning(
                                        f"  Message {msg['message_id']} stuck for {msg['idle_ms']}ms "
                                        f"(delivered {msg['delivery_count']} times)"
                                    )

                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in continuous monitor: {e}")
                await asyncio.sleep(interval)


async def main():
    """CLI entry point."""
    import argparse
    from settings import settings

    parser = argparse.ArgumentParser(description="Monitor Redis Streams")
    parser.add_argument("--stream", default="orderbooks:updates", help="Stream name")
    parser.add_argument("--interval", type=int, default=10, help="Check interval (seconds)")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--claim-stuck", action="store_true", help="Claim stuck messages")
    parser.add_argument("--consumer", default="monitor", help="Consumer name for claiming")

    args = parser.parse_args()

    # Connect to Redis
    redis = await aioredis.from_url(
        settings.redis.dsn,
        decode_responses=False
    )

    monitor = StreamMonitor(redis)

    try:
        if args.claim_stuck:
            # Claim stuck messages
            count = await monitor.claim_stuck_messages(
                args.stream,
                "spread_collectors",
                args.consumer
            )
            print(f"Claimed {count} stuck messages")

        elif args.once:
            # Single check
            await monitor.print_status(args.stream)

        else:
            # Continuous monitoring
            await monitor.continuous_monitor(args.stream, args.interval)

    finally:
        await redis.close()


if __name__ == "__main__":
    asyncio.run(main())

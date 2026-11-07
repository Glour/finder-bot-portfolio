"""
Performance profiling utilities for orderbook pipeline.

Tracks end-to-end latency from WebSocket to Telegram broadcast.
"""
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from settings import logger


@dataclass
class LatencyMetrics:
    """Metrics for a single orderbook processing."""
    symbol: str
    ws_timestamp: int  # milliseconds
    redis_write_at: Optional[int] = None
    loader_start_at: Optional[int] = None
    loader_end_at: Optional[int] = None
    analyzer_start_at: Optional[int] = None
    analyzer_end_at: Optional[int] = None
    broadcast_start_at: Optional[int] = None
    broadcast_end_at: Optional[int] = None

    @property
    def ws_to_redis(self) -> Optional[float]:
        """WebSocket to Redis write latency (ms)."""
        if self.redis_write_at:
            return self.redis_write_at - self.ws_timestamp
        return None

    @property
    def redis_to_loader(self) -> Optional[float]:
        """Redis write to loader pickup latency (ms)."""
        if self.redis_write_at and self.loader_start_at:
            return self.loader_start_at - self.redis_write_at
        return None

    @property
    def loader_duration(self) -> Optional[float]:
        """Loader processing duration (ms)."""
        if self.loader_start_at and self.loader_end_at:
            return self.loader_end_at - self.loader_start_at
        return None

    @property
    def loader_to_analyzer(self) -> Optional[float]:
        """Loader to analyzer latency (ms)."""
        if self.loader_end_at and self.analyzer_start_at:
            return self.analyzer_start_at - self.loader_end_at
        return None

    @property
    def analyzer_duration(self) -> Optional[float]:
        """Analyzer processing duration (ms)."""
        if self.analyzer_start_at and self.analyzer_end_at:
            return self.analyzer_end_at - self.analyzer_start_at
        return None

    @property
    def analyzer_to_broadcast(self) -> Optional[float]:
        """Analyzer to broadcast latency (ms)."""
        if self.analyzer_end_at and self.broadcast_start_at:
            return self.broadcast_start_at - self.analyzer_end_at
        return None

    @property
    def broadcast_duration(self) -> Optional[float]:
        """Broadcast duration (ms)."""
        if self.broadcast_start_at and self.broadcast_end_at:
            return self.broadcast_end_at - self.broadcast_start_at
        return None

    @property
    def total_latency(self) -> Optional[float]:
        """Total end-to-end latency (ms)."""
        if self.broadcast_end_at:
            return self.broadcast_end_at - self.ws_timestamp
        elif self.analyzer_end_at:
            return self.analyzer_end_at - self.ws_timestamp
        elif self.loader_end_at:
            return self.loader_end_at - self.ws_timestamp
        elif self.redis_write_at:
            return self.redis_write_at - self.ws_timestamp
        return None


class LatencyTracker:
    """
    Tracks end-to-end latency for orderbook processing.

    Thread-safe, uses deque with maxlen for memory efficiency.
    """

    def __init__(self, max_tracked: int = 10000):
        """
        Initialize tracker.

        Args:
            max_tracked: Maximum number of tracked orderbooks (FIFO eviction)
        """
        self.max_tracked = max_tracked
        # Key: orderbook key, Value: LatencyMetrics
        self._metrics: dict[str, LatencyMetrics] = {}
        # Recent latencies for statistics (last 1000)
        self._recent_total: deque[float] = deque(maxlen=1000)
        self._recent_ws_to_redis: deque[float] = deque(maxlen=1000)
        self._recent_loader: deque[float] = deque(maxlen=1000)
        self._recent_analyzer: deque[float] = deque(maxlen=1000)
        self._recent_broadcast: deque[float] = deque(maxlen=1000)

        # Statistics
        self.total_tracked = 0
        self.total_completed = 0

    def start_tracking(self, orderbook_key: str, symbol: str, ws_timestamp: int):
        """
        Start tracking an orderbook.

        Args:
            orderbook_key: Unique orderbook identifier
            symbol: Token symbol
            ws_timestamp: WebSocket timestamp in milliseconds
        """
        # Limit memory usage
        if len(self._metrics) >= self.max_tracked:
            # Remove oldest (FIFO)
            oldest_key = next(iter(self._metrics))
            del self._metrics[oldest_key]

        self._metrics[orderbook_key] = LatencyMetrics(
            symbol=symbol,
            ws_timestamp=ws_timestamp
        )
        self.total_tracked += 1

    def record_redis_write(self, orderbook_key: str):
        """Record Redis write timestamp."""
        if orderbook_key in self._metrics:
            self._metrics[orderbook_key].redis_write_at = int(time.time() * 1000)

    def record_loader_start(self, orderbook_key: str):
        """Record loader start timestamp."""
        if orderbook_key in self._metrics:
            self._metrics[orderbook_key].loader_start_at = int(time.time() * 1000)

    def record_loader_end(self, orderbook_key: str):
        """Record loader end timestamp."""
        if orderbook_key in self._metrics:
            self._metrics[orderbook_key].loader_end_at = int(time.time() * 1000)

    def record_analyzer_start(self, orderbook_key: str):
        """Record analyzer start timestamp."""
        if orderbook_key in self._metrics:
            self._metrics[orderbook_key].analyzer_start_at = int(time.time() * 1000)

    def record_analyzer_end(self, orderbook_key: str):
        """Record analyzer end timestamp."""
        if orderbook_key in self._metrics:
            m = self._metrics[orderbook_key]
            m.analyzer_end_at = int(time.time() * 1000)

            # Update statistics
            if m.analyzer_duration:
                self._recent_analyzer.append(m.analyzer_duration)

    def record_broadcast_start(self, orderbook_key: str):
        """Record broadcast start timestamp."""
        if orderbook_key in self._metrics:
            self._metrics[orderbook_key].broadcast_start_at = int(time.time() * 1000)

    def record_broadcast_end(self, orderbook_key: str):
        """Record broadcast end timestamp and finalize metrics."""
        if orderbook_key in self._metrics:
            m = self._metrics[orderbook_key]
            m.broadcast_end_at = int(time.time() * 1000)

            # Update statistics
            if m.total_latency:
                self._recent_total.append(m.total_latency)
            if m.ws_to_redis:
                self._recent_ws_to_redis.append(m.ws_to_redis)
            if m.loader_duration:
                self._recent_loader.append(m.loader_duration)
            if m.broadcast_duration:
                self._recent_broadcast.append(m.broadcast_duration)

            self.total_completed += 1

            # Log if high latency
            if m.total_latency and m.total_latency > 5000:  # > 5 seconds
                self._log_slow_orderbook(orderbook_key, m)

    def _log_slow_orderbook(self, orderbook_key: str, metrics: LatencyMetrics):
        """Log detailed breakdown for slow orderbooks."""
        logger.warning(
            f"SLOW ORDERBOOK: {metrics.symbol} ({orderbook_key})\n"
            f"  Total latency: {metrics.total_latency:.0f}ms\n"
            f"  WS→Redis: {metrics.ws_to_redis:.0f}ms\n"
            f"  Redis→Loader: {metrics.redis_to_loader or 0:.0f}ms\n"
            f"  Loader: {metrics.loader_duration or 0:.0f}ms\n"
            f"  Loader→Analyzer: {metrics.loader_to_analyzer or 0:.0f}ms\n"
            f"  Analyzer: {metrics.analyzer_duration or 0:.0f}ms\n"
            f"  Analyzer→Broadcast: {metrics.analyzer_to_broadcast or 0:.0f}ms\n"
            f"  Broadcast: {metrics.broadcast_duration or 0:.0f}ms"
        )

    def get_stats(self) -> dict:
        """Get current statistics."""
        def avg(values: deque) -> float:
            return sum(values) / len(values) if values else 0

        def p95(values: deque) -> float:
            if not values:
                return 0
            sorted_vals = sorted(values)
            idx = int(len(sorted_vals) * 0.95)
            return sorted_vals[idx] if idx < len(sorted_vals) else sorted_vals[-1]

        return {
            "total_tracked": self.total_tracked,
            "total_completed": self.total_completed,
            "in_flight": len(self._metrics),
            "avg_total_latency_ms": avg(self._recent_total),
            "p95_total_latency_ms": p95(self._recent_total),
            "avg_ws_to_redis_ms": avg(self._recent_ws_to_redis),
            "avg_loader_ms": avg(self._recent_loader),
            "avg_analyzer_ms": avg(self._recent_analyzer),
            "avg_broadcast_ms": avg(self._recent_broadcast),
        }

    def log_stats(self):
        """Log current statistics."""
        stats = self.get_stats()
        logger.info(
            f"Latency Stats | "
            f"Total: {stats['avg_total_latency_ms']:.0f}ms (p95: {stats['p95_total_latency_ms']:.0f}ms) | "
            f"WS→Redis: {stats['avg_ws_to_redis_ms']:.0f}ms | "
            f"Loader: {stats['avg_loader_ms']:.0f}ms | "
            f"Analyzer: {stats['avg_analyzer_ms']:.0f}ms | "
            f"Broadcast: {stats['avg_broadcast_ms']:.0f}ms | "
            f"Tracked: {stats['total_tracked']} | "
            f"Completed: {stats['total_completed']}"
        )


@dataclass
class ComponentTimer:
    """Timer for measuring component execution time."""
    name: str
    start_time: float = field(default_factory=time.perf_counter)
    timings: dict[str, float] = field(default_factory=dict)

    def checkpoint(self, label: str):
        """Record a timing checkpoint."""
        now = time.perf_counter()
        elapsed = (now - self.start_time) * 1000  # milliseconds
        self.timings[label] = elapsed

    def log(self, min_duration_ms: float = 100):
        """Log timings if total duration exceeds threshold."""
        total = (time.perf_counter() - self.start_time) * 1000
        if total > min_duration_ms:
            parts = [f"{k}={v:.1f}ms" for k, v in self.timings.items()]
            logger.info(f"{self.name} timing: {', '.join(parts)}, total={total:.1f}ms")


# Global tracker instance
_global_tracker: Optional[LatencyTracker] = None


def get_tracker() -> LatencyTracker:
    """Get or create global tracker instance."""
    global _global_tracker
    if _global_tracker is None:
        _global_tracker = LatencyTracker()
    return _global_tracker


def reset_tracker():
    """Reset global tracker (for testing)."""
    global _global_tracker
    _global_tracker = None

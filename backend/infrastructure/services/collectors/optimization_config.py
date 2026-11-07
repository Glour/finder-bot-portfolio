"""
Optimization configuration for orderbook pipeline.

Feature flags control which optimizations are enabled.
All flags default to False for backwards compatibility.
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class OptimizationConfig:
    """
    Central configuration for pipeline optimizations.

    All features are opt-in via environment variables for safe rollout.
    """

    # Phase 1: Token metadata caching
    use_token_metadata_cache: bool = False
    token_cache_ttl_seconds: int = 300  # 5 minutes

    # Phase 2: Redis Streams for incremental updates
    use_redis_streams: bool = False
    stream_consumer_group: str = "spread_collectors"
    stream_consumer_name: Optional[str] = None  # Auto-generated if None

    # Phase 3: msgpack-only serialization
    use_msgpack_only: bool = False

    # Performance tuning
    batch_size: int = 200
    analysis_interval: float = 0.5
    flush_interval: float = 0.02

    # Monitoring and profiling
    enable_profiling: bool = False
    enable_latency_tracking: bool = False
    log_slow_orderbooks: bool = True
    slow_orderbook_threshold_ms: float = 5000

    # Redis Streams configuration
    stream_max_len: int = 10000
    stream_read_count: int = 100
    stream_block_ms: int = 1000

    @classmethod
    def from_env(cls) -> "OptimizationConfig":
        """
        Load configuration from environment variables.

        Environment variables:
        - OPTIMIZE_TOKEN_CACHE: Enable token metadata caching
        - OPTIMIZE_USE_STREAMS: Enable Redis Streams mode
        - OPTIMIZE_MSGPACK_ONLY: Use msgpack everywhere (no JSON)
        - OPTIMIZE_ENABLE_PROFILING: Enable detailed profiling logs
        - OPTIMIZE_ENABLE_LATENCY_TRACKING: Track end-to-end latency
        - OPTIMIZE_BATCH_SIZE: Batch size for parallel processing
        - OPTIMIZE_ANALYSIS_INTERVAL: Analysis interval (seconds)
        - OPTIMIZE_FLUSH_INTERVAL: Redis flush interval (seconds)
        """
        return cls(
            # Phase 1
            use_token_metadata_cache=os.getenv("OPTIMIZE_TOKEN_CACHE", "false").lower() == "true",
            token_cache_ttl_seconds=int(os.getenv("OPTIMIZE_TOKEN_CACHE_TTL", "300")),

            # Phase 2
            use_redis_streams=os.getenv("OPTIMIZE_USE_STREAMS", "false").lower() == "true",
            stream_consumer_group=os.getenv("OPTIMIZE_STREAM_GROUP", "spread_collectors"),
            stream_consumer_name=os.getenv("OPTIMIZE_STREAM_CONSUMER", None),

            # Phase 3
            use_msgpack_only=os.getenv("OPTIMIZE_MSGPACK_ONLY", "false").lower() == "true",

            # Performance
            batch_size=int(os.getenv("OPTIMIZE_BATCH_SIZE", "200")),
            analysis_interval=float(os.getenv("OPTIMIZE_ANALYSIS_INTERVAL", "0.5")),
            flush_interval=float(os.getenv("OPTIMIZE_FLUSH_INTERVAL", "0.02")),

            # Monitoring
            enable_profiling=os.getenv("OPTIMIZE_ENABLE_PROFILING", "false").lower() == "true",
            enable_latency_tracking=os.getenv("OPTIMIZE_ENABLE_LATENCY_TRACKING", "false").lower() == "true",
            log_slow_orderbooks=os.getenv("OPTIMIZE_LOG_SLOW", "true").lower() == "true",
            slow_orderbook_threshold_ms=float(os.getenv("OPTIMIZE_SLOW_THRESHOLD_MS", "5000")),

            # Streams
            stream_max_len=int(os.getenv("OPTIMIZE_STREAM_MAXLEN", "10000")),
            stream_read_count=int(os.getenv("OPTIMIZE_STREAM_READ_COUNT", "100")),
            stream_block_ms=int(os.getenv("OPTIMIZE_STREAM_BLOCK_MS", "1000")),
        )

    def summary(self) -> str:
        """Return human-readable configuration summary."""
        enabled_features = []

        if self.use_token_metadata_cache:
            enabled_features.append(f"Token Cache (TTL: {self.token_cache_ttl_seconds}s)")
        if self.use_redis_streams:
            enabled_features.append(f"Redis Streams (group: {self.stream_consumer_group})")
        if self.use_msgpack_only:
            enabled_features.append("msgpack-only serialization")
        if self.enable_profiling:
            enabled_features.append("Detailed profiling")
        if self.enable_latency_tracking:
            enabled_features.append("Latency tracking")

        if not enabled_features:
            enabled_features.append("None (baseline performance)")

        return (
            f"Optimization Config:\n"
            f"  Enabled: {', '.join(enabled_features)}\n"
            f"  Batch size: {self.batch_size}\n"
            f"  Analysis interval: {self.analysis_interval}s\n"
            f"  Flush interval: {self.flush_interval}s"
        )


# Default configuration (all optimizations disabled)
DEFAULT_CONFIG = OptimizationConfig()

# Load from environment
ENV_CONFIG = OptimizationConfig.from_env()

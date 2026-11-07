"""
Configuration for spread collection and arbitrage analysis.

Centralizes all magic numbers and thresholds into typed configuration classes.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict


class SignalLevelType(str, Enum):
    """Signal severity levels for arbitrage opportunities."""
    NONE = "None"
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"
    VERY_HIGH = "Very High"

    @classmethod
    def ordered(cls) -> list["SignalLevelType"]:
        """Return levels in ascending order."""
        return [cls.NONE, cls.LOW, cls.MEDIUM, cls.HIGH, cls.VERY_HIGH]

    def __lt__(self, other):
        """Enable level comparison."""
        if not isinstance(other, SignalLevelType):
            return NotImplemented
        return self.ordered().index(self) < self.ordered().index(other)

    def __gt__(self, other):
        """Enable level comparison."""
        if not isinstance(other, SignalLevelType):
            return NotImplemented
        return self.ordered().index(self) > self.ordered().index(other)

    def __le__(self, other):
        """Enable level comparison."""
        return self == other or self < other

    def __ge__(self, other):
        """Enable level comparison."""
        return self == other or self > other


@dataclass(frozen=True)
class SpreadThresholds:
    """Minimum spread percentage thresholds for each signal level."""
    very_high: float
    high: float
    medium: float
    low: float
    none: float = 0.0

    def get_threshold(self, level: SignalLevelType) -> float:
        """Get threshold for a specific level."""
        mapping = {
            SignalLevelType.VERY_HIGH: self.very_high,
            SignalLevelType.HIGH: self.high,
            SignalLevelType.MEDIUM: self.medium,
            SignalLevelType.LOW: self.low,
            SignalLevelType.NONE: self.none,
        }
        return mapping[level]

    def to_dict(self) -> Dict[SignalLevelType, float]:
        """Convert to dictionary mapping."""
        return {
            SignalLevelType.VERY_HIGH: self.very_high,
            SignalLevelType.HIGH: self.high,
            SignalLevelType.MEDIUM: self.medium,
            SignalLevelType.LOW: self.low,
            SignalLevelType.NONE: self.none,
        }


@dataclass(frozen=True)
class ProfitThresholds:
    """Minimum profit (USDT) thresholds for each signal level."""
    very_high: float
    high: float
    medium: float
    low: float
    none: float = 0.0

    def get_threshold(self, level: SignalLevelType) -> float:
        """Get threshold for a specific level."""
        mapping = {
            SignalLevelType.VERY_HIGH: self.very_high,
            SignalLevelType.HIGH: self.high,
            SignalLevelType.MEDIUM: self.medium,
            SignalLevelType.LOW: self.low,
            SignalLevelType.NONE: self.none,
        }
        return mapping[level]

    def to_dict(self) -> Dict[SignalLevelType, float]:
        """Convert to dictionary mapping."""
        return {
            SignalLevelType.VERY_HIGH: self.very_high,
            SignalLevelType.HIGH: self.high,
            SignalLevelType.MEDIUM: self.medium,
            SignalLevelType.LOW: self.low,
            SignalLevelType.NONE: self.none,
        }


@dataclass(frozen=True)
class CacheUpdateThresholds:
    """Thresholds for cache update triggers."""
    # Spread change thresholds (percentage points)
    spread_trigger: Dict[SignalLevelType, float]
    # Profit change thresholds (percentage of old value)
    profit_trigger: Dict[SignalLevelType, float]
    # Spread reset thresholds (below this, clear cache)
    spread_reset: Dict[SignalLevelType, float]

    # High spread special handling
    high_spread_threshold: float = 50.0
    high_spread_trigger_percent: float = 0.10  # 10%
    high_spread_trigger_min: float = 10.0

    # Very high level special handling
    very_high_recheck_factor: float = 0.4  # 40% drop triggers recheck


@dataclass(frozen=True)
class ArbitrageConfig:
    """
    Complete configuration for arbitrage detection and filtering.

    Separate configs for spot and futures markets.
    """
    # Market type
    is_futures: bool = False

    # Spread thresholds
    spread_thresholds: SpreadThresholds = field(default_factory=lambda: SpreadThresholds(
        very_high=20.0,
        high=10.0,
        medium=3.0,
        low=1.0,
    ))

    # Profit thresholds
    profit_thresholds: ProfitThresholds = field(default_factory=lambda: ProfitThresholds(
        very_high=200.0,
        high=100.0,
        medium=35.0,
        low=2.0,
    ))

    # Minimum profit to consider (USDT)
    min_profit_threshold: float = 5.0

    # Profit-based level boost thresholds (USDT)
    profit_boost_threshold: float = 500.0       # $500+ profit → boost +1 level
    profit_boost_high_threshold: float = 5000.0  # $5000+ profit → boost +2 levels

    # Major assets that get automatic boost for any detected spread
    major_assets: tuple = ("BTC", "ETH", "SOL", "USDT", "USDC")
    major_asset_min_boost: int = 1  # Always boost by at least 1 level for major assets

    # DEX price staleness (seconds) — skip DEX prices older than this
    max_dex_price_age: float = 5.0

    # Minimum absolute profit change (USDT) required to trigger repeat signal
    # Prevents $7→$8 ($1 change) from triggering when 10% threshold = $0.70
    profit_trigger_min_abs: float = 10.0  # at least $10 change in profit

    # DEX-specific thresholds for signal spam reduction
    dex_spread_trigger_min: float = 0.5     # min spread change (pp) for DEX signals
    dex_profit_trigger_min_pct: float = 15.0  # min profit change (%) for DEX signals

    # Cache TTL and cooldown (seconds)
    cache_ttl: int = 1800        # TTL for arbitrage:* cache keys
    cooldown_ttl: int = 300     # Cooldown between broadcasts of same signal pair (5 min)

    # Cache update thresholds
    cache_thresholds: CacheUpdateThresholds = field(default_factory=lambda: CacheUpdateThresholds(
        spread_trigger={
            SignalLevelType.VERY_HIGH: 15.0,  # Увеличен порог триггера повторных сообщений
            SignalLevelType.HIGH: 10.0,  # Увеличен порог триггера повторных сообщений
            SignalLevelType.MEDIUM: 3.0,
            SignalLevelType.LOW: 2.0,
            SignalLevelType.NONE: 0.0,
        },
        profit_trigger={
            SignalLevelType.VERY_HIGH: 10.0,  # 10%
            SignalLevelType.HIGH: 10.0,
            SignalLevelType.MEDIUM: 10.0,
            SignalLevelType.LOW: 10.0,
            SignalLevelType.NONE: 10.0,
        },
        spread_reset={
            SignalLevelType.VERY_HIGH: 1.0,  # Опущен порог до 1% (почти полное схождение)
            SignalLevelType.HIGH: 1.0,       # Опущен порог до 1% (почти полное схождение)
            SignalLevelType.MEDIUM: 0.7,
            SignalLevelType.LOW: 0.7,
            SignalLevelType.NONE: 0.0,
        }
    ))

    @classmethod
    def for_spot(cls) -> "ArbitrageConfig":
        """Create configuration for spot market."""
        return cls(is_futures=False)

    @classmethod
    def for_futures(cls) -> "ArbitrageConfig":
        """Create configuration for futures market."""
        return cls(
            is_futures=True,
            spread_thresholds=SpreadThresholds(
                very_high=20.0,
                high=10.0,
                medium=5.0,  # Higher threshold for futures
                low=2.0,
            ),
            profit_thresholds=ProfitThresholds(
                very_high=0.0,  # No profit requirement for futures
                high=0.0,
                medium=0.0,
                low=0.0,
            )
        )


@dataclass(frozen=True)
class MergeConfig:
    """Configuration for global token merging."""
    # Maximum price difference percentage for auto-merge
    max_price_diff_pct: float = 3.0

    # Cache timeout for merge candidates (seconds)
    candidate_cache_timeout: int = 3600 * 7  # 7 hours

    # Admin notification chat IDs (should come from settings)
    admin_chat_ids: list[int] = field(default_factory=lambda: [0, 0])

    # Auto-approve thresholds
    auto_approve_price_diff_threshold: float = 3.0  # Auto-approve if diff > 3%
    auto_approve_silent_threshold: float = 3.0  # Silent auto-approve if diff <= 3%

    # Maximum price ratio for same_contract merge (prevents 1000X token merging)
    # If price_a / price_b > this ratio, reject merge even with same contract
    max_contract_price_ratio: float = 10.0

    # Patterns for "multiplied" tokens that should not be merged with base tokens
    # e.g., 1000MOG should not merge with MOG
    multiplied_token_prefixes: tuple[str, ...] = (
        "1000000",  # 1M multiplier (e.g., 1000000MOG)
        "1000",     # 1K multiplier (e.g., 1000BONK, 1000BTT)
        "100",      # 100x multiplier
        "10",       # 10x multiplier
    )


@dataclass(frozen=True)
class BroadcasterConfig:
    """Configuration for message broadcasting and formatting."""
    # Use grouped message template (multiple spreads per token in one message)
    use_grouped_template: bool = True

    # Maximum acceptable delay for LBank source (seconds)
    max_lbank_delay: float = 15.0

    # Maximum acceptable delay for other sources: GATE, XT, MEXC (seconds)
    max_other_delay: float = 5.0


@dataclass(frozen=True)
class PerformanceConfig:
    """Configuration for analyzer performance tuning."""
    # Batch size for parallel token processing
    # Smaller values = more frequent broadcasts, lower latency per batch
    # Larger values = better throughput, higher latency
    batch_size: int = 200

    # Analysis interval (seconds)
    # Lower = more frequent checks, higher CPU usage
    # Higher = less frequent checks, lower CPU usage
    analysis_interval: float = 0.5

    # Redis connection pool size
    redis_max_connections: int = 50

    # Enable detailed profiling logs
    enable_profiling: bool = False


# Price correction map for tokens where a specific source artificially inflates/deflates price.
# Format: {symbol_upper: {source_key: factor}}
# source_key = "{source_name_lower}_{market_type_lower}"  (e.g. "gate_spot", "gate_futures")
# factor = multiplier applied to ALL orderbook prices for that source (e.g. 0.5 = divide by 2)
#
# Example: TQQQX on GATE artificially prices SPOT at 2x the real price.
# So we multiply GATE spot prices by 0.5 to get the real price before arbitrage analysis.
PRICE_CORRECTION_MAP: dict[str, dict[str, float]] = {
    "TQQQX": {
        "gate_spot": 0.5,  # GATE prices TQQQX spot at 2x real — divide by 2
    },
}

# Default configurations
DEFAULT_SPOT_CONFIG = ArbitrageConfig.for_spot()
DEFAULT_FUTURES_CONFIG = ArbitrageConfig.for_futures()
DEFAULT_MERGE_CONFIG = MergeConfig()
DEFAULT_BROADCASTER_CONFIG = BroadcasterConfig()
DEFAULT_PERFORMANCE_CONFIG = PerformanceConfig()

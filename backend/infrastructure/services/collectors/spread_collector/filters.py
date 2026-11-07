"""
Arbitrage signal filtering and cache management.

Refactored to use configuration dataclasses and simplified cache update logic.
"""
import time
from typing import Optional

import orjson

from infrastructure.services.collectors.spread_collector.config import (
    SignalLevelType,
    ArbitrageConfig,
    DEFAULT_SPOT_CONFIG,
    DEFAULT_FUTURES_CONFIG,
)
from infrastructure.services.common.cache import RedisCache
from settings import logger


class ArbitrageFilter:
    """
    Filters arbitrage opportunities based on spread and profit thresholds.

    Uses configurable thresholds and manages Redis cache for deduplication.
    """

    def __init__(
        self,
        is_futures: bool = False,
        cache: Optional[RedisCache] = None,
        config: Optional[ArbitrageConfig] = None
    ):
        """
        Initialize arbitrage filter.

        Args:
            is_futures: Whether this filter is for futures market
            cache: Redis cache instance for deduplication
            config: Custom configuration (uses defaults if not provided)
        """
        self.is_futures = is_futures
        self.cache = cache

        # Use provided config or default based on market type
        if config is not None:
            self.config = config
        else:
            self.config = DEFAULT_FUTURES_CONFIG if is_futures else DEFAULT_SPOT_CONFIG

        logger.debug(
            f"ArbitrageFilter initialized for {'futures' if is_futures else 'spot'} "
            f"with min_profit={self.config.min_profit_threshold}"
        )

    def get_level(
        self,
        spread_pct: float,
        profit: float,
        prev_level: Optional[SignalLevelType] = None,
        symbol: Optional[str] = None
    ) -> SignalLevelType:
        """
        Determine signal level based on spread and profit.

        Args:
            spread_pct: Spread percentage
            profit: Profit in USDT
            prev_level: Previous signal level (for level transitions)
            symbol: Token symbol for major asset boost (e.g. "BTC")

        Returns:
            Signal level (NONE, LOW, MEDIUM, HIGH, VERY_HIGH)
        """
        # Filter out low-profit opportunities
        if profit < self.config.min_profit_threshold:
            return SignalLevelType.NONE

        spread_thresholds = self.config.spread_thresholds.to_dict()
        profit_thresholds = self.config.profit_thresholds.to_dict()

        # Find spread-based level
        spread_level = self._get_level_by_threshold(spread_pct, spread_thresholds)

        # First calculation - check both spread and profit
        if prev_level is None:
            base_level = SignalLevelType.NONE
            for level in reversed(SignalLevelType.ordered()):
                if (spread_pct >= spread_thresholds[level] and
                    profit >= profit_thresholds[level]):
                    base_level = level
                    break
            return self._apply_boost(base_level, profit, symbol)

        # Level upgrade - verify profit requirement
        if spread_level > prev_level:
            base_level = prev_level
            for level in reversed(SignalLevelType.ordered()):
                if (spread_pct >= spread_thresholds[level] and
                    profit >= profit_thresholds[level]):
                    base_level = level
                    break
            return self._apply_boost(base_level, profit, symbol)

        # Level downgrade or same - use spread only
        return self._apply_boost(spread_level, profit, symbol)

    def _apply_boost(
        self,
        base_level: SignalLevelType,
        profit: float,
        symbol: Optional[str] = None
    ) -> SignalLevelType:
        """Apply profit-based and major asset boost to signal level."""
        if base_level == SignalLevelType.NONE:
            return base_level

        boost = 0

        # Profit-based boost
        if profit >= self.config.profit_boost_high_threshold:
            boost = 2
        elif profit >= self.config.profit_boost_threshold:
            boost = 1

        # Major asset boost (minimum guarantee)
        if symbol and symbol.upper() in self.config.major_assets:
            boost = max(boost, self.config.major_asset_min_boost)

        if boost > 0:
            ordered = SignalLevelType.ordered()
            idx = ordered.index(base_level)
            boosted_idx = min(idx + boost, len(ordered) - 1)
            boosted_level = ordered[boosted_idx]
            if boosted_level != base_level:
                logger.debug(
                    f"Level boost: {base_level.value} -> {boosted_level.value} "
                    f"(profit={profit:.0f}$, symbol={symbol}, boost=+{boost})"
                )
            return boosted_level

        return base_level

    @staticmethod
    def _get_level_by_threshold(
        value: float,
        thresholds: dict[SignalLevelType, float]
    ) -> SignalLevelType:
        """Find highest level where value meets threshold."""
        for level in reversed(SignalLevelType.ordered()):
            if value >= thresholds[level]:
                return level
        return SignalLevelType.NONE

    async def update_cache_batch(
        self,
        updates: list[tuple[str, dict]]
    ) -> list[bool]:
        """
        Update Redis cache in batch, returning flags for updated entries.

        Uses atomic SET NX cooldown to prevent race condition duplicates
        between multiple spread-collector instances.

        Args:
            updates: List of (cache_key, result_dict) tuples

        Returns:
            List of boolean flags indicating which updates should be broadcast
        """
        if self.cache is None:
            raise RuntimeError("Cache not initialized for ArbitrageFilter")

        if not updates:
            return []

        keys = [key for key, _ in updates]

        # Fetch current cache values
        cached_values = await self._fetch_cached_batch(keys)

        # Process each update
        updated_flags = []
        cache_operations = []  # [(key, value, operation)]

        for (key, result), cached in zip(updates, cached_values):
            # Detect DEX signals for tighter spam thresholds
            is_dex = (
                result.get("buy_from", "").startswith("dex_")
                or result.get("sell_to", "").startswith("dex_")
            )
            should_update, operation, is_level_upgrade = self._should_update_cache(
                result, cached, key, is_dex=is_dex
            )

            if operation:
                cache_operations.append((key, result if should_update else cached, operation))
                # FIX (2026-02-25): set cooldown on spread_reset (cache delete)
                # prevents immediate re-broadcast after phantom/stale data resets cache
                if operation == "delete":
                    await self.cache.client.set(
                        f"arb_cd:{key}", b"1",
                        ex=self.config.cooldown_ttl,
                        nx=True
                    )

            if should_update:
                # CRITICAL FIX (2026-02-19): Ignore cooldown on level upgrade
                # Previously, cooldown from Low signal blocked High signal broadcast
                if is_level_upgrade:
                    # Overwrite cooldown (without nx=True) to allow immediate broadcast
                    cooldown_key = f"arb_cd:{key}"
                    await self.cache.client.set(cooldown_key, b"1", ex=self.config.cooldown_ttl)
                    updated_flags.append(True)
                else:
                    # Atomic cooldown: SET NX (set-if-not-exists) returns True only for winner
                    won_lock = await self.cache.client.set(
                        f"arb_cd:{key}", b"1",
                        ex=self.config.cooldown_ttl,
                        nx=True
                    )
                    # Only broadcast if we won the atomic lock
                    updated_flags.append(bool(won_lock))
            else:
                updated_flags.append(False)

        # Execute cache operations in batch
        await self._execute_cache_operations(cache_operations)

        logger.debug(
            f"Cache batch update: {sum(updated_flags)}/{len(updates)} entries updated"
        )

        return updated_flags

    async def _fetch_cached_batch(self, keys: list[str]) -> list[Optional[dict]]:
        """Fetch multiple cache entries in one pipeline."""
        if not keys:
            return []

        pipe = self.cache.client.pipeline()
        for key in keys:
            pipe.get(key)

        raw_results = await pipe.execute()

        # Parse JSON results
        results = []
        for raw in raw_results:
            if raw:
                try:
                    results.append(orjson.loads(raw))
                except Exception as e:
                    logger.warning(f"Failed to parse cached value: {e}")
                    results.append(None)
            else:
                results.append(None)

        return results

    def _should_update_cache(
        self,
        new_result: dict,
        cached: Optional[dict],
        cache_key: str,
        is_dex: bool = False
    ) -> tuple[bool, Optional[str], bool]:
        """
        Determine if cache should be updated.

        Returns:
            (should_broadcast, cache_operation, is_level_upgrade)
            cache_operation: "set", "delete", "update", or None
            is_level_upgrade: True if level was upgraded (ignore cooldown)
        """
        new_level = SignalLevelType(new_result["level"])
        new_spread = float(new_result.get("spread_percent", 0) or 0.0)
        new_profit = float(new_result.get("profit", 0) or 0.0)

        # No cache entry - add new
        if not cached:
            return True, "set", False

        old_level = SignalLevelType(cached.get("level"))
        old_spread = float(cached.get("spread_percent", 0) or 0.0)
        old_profit = float(cached.get("profit", 0) or 0.0)

        # Check for cache reset (spread dropped too low)
        # FIX (2026-02-25): do NOT delete cache on reset — just ignore the update.
        # Deleting cache caused immediate re-broadcast on next normal update (infinite cache intent).
        reset_threshold = self.config.cache_thresholds.spread_reset.get(old_level, 0)
        if new_spread < reset_threshold:
            logger.debug(
                f"Cache reset skipped (infinite cache): spread {new_spread:.2f}% < threshold {reset_threshold:.2f}%"
            )
            return False, None, False

        # Level upgraded - always update
        if new_level > old_level:
            logger.debug(f"Level upgrade: {old_level} -> {new_level}")
            return True, "set", True

        # Same level - check if changes are significant
        if new_level == old_level:
            should_update = self._is_significant_change(
                old_level, old_spread, old_profit, new_spread, new_profit, is_dex=is_dex
            )

            if should_update:
                return True, "set", False

            # Special case: Very high level recheck logic
            if self._should_recheck_very_high(old_level, old_spread, new_spread):
                # Update spread but don't broadcast
                return False, "update", False

            logger.debug(
                f"Cache blocked: key={cache_key[-50:]} level={new_level} "
                f"old_spread={old_spread:.2f}% old_profit={old_profit:.2f}$ "
                f"new_spread={new_spread:.2f}% new_profit={new_profit:.2f}$ "
                f"(change not significant)"
            )

        return False, None, False

    def _is_significant_change(
        self,
        level: SignalLevelType,
        old_spread: float,
        old_profit: float,
        new_spread: float,
        new_profit: float,
        is_dex: bool = False
    ) -> bool:
        """Check if spread/profit changes exceed thresholds."""
        spread_diff = new_spread - old_spread
        
        # Игнорировать падение спреда - отправлять сигналы только при росте
        if spread_diff < 0:
            return False
        profit_diff = abs(new_profit - old_profit)

        # Dynamic spread threshold for high spreads
        if old_spread > self.config.cache_thresholds.high_spread_threshold:
            spread_threshold = max(
                old_spread * self.config.cache_thresholds.high_spread_trigger_percent,
                self.config.cache_thresholds.high_spread_trigger_min
            )
        else:
            spread_threshold = self.config.cache_thresholds.spread_trigger.get(
                level, float("inf")
            )

        # Profit threshold (percentage of old value)
        profit_trigger_pct = self.config.cache_thresholds.profit_trigger.get(
            level, float("inf")
        )

        # Apply tighter DEX thresholds to reduce signal spam
        if is_dex:
            spread_threshold = max(spread_threshold, self.config.dex_spread_trigger_min)
            profit_trigger_pct = max(profit_trigger_pct, self.config.dex_profit_trigger_min_pct)

        profit_threshold = (
            old_profit * (profit_trigger_pct / 100.0) if old_profit > 0 else 1.0
        )

        # Check thresholds
        meets_spread = spread_diff >= spread_threshold
        meets_profit = profit_diff >= profit_threshold
        
        # Не учитывать изменение профита при его уменьшении - достаточно только спреда
        profit_decreased = new_profit < old_profit
        
        if profit_decreased:
            # При уменьшении профита - достаточно только изменения спреда
            if meets_spread:
                logger.debug(
                    f"Significant change (profit decreased): spread_diff={spread_diff:.2f} "
                    f"(threshold={spread_threshold:.2f}), "
                    f"profit: {old_profit:.2f}$ -> {new_profit:.2f}$ (decreased, ignoring)"
                )
            return meets_spread
        else:
            # При увеличении профита - требуем оба условия
            if meets_spread and meets_profit:
                logger.debug(
                    f"Significant change: spread_diff={spread_diff:.2f} "
                    f"(threshold={spread_threshold:.2f}), "
                    f"profit_diff={profit_diff:.2f} "
                    f"(threshold={profit_threshold:.2f})"
                )
            return meets_spread and meets_profit

    def _should_recheck_very_high(
        self,
        level: SignalLevelType,
        old_spread: float,
        new_spread: float
    ) -> bool:
        """
        Check if very high signal needs rechecking.

        For VERY_HIGH signals, if spread drops significantly but stays above 50%,
        update the cached spread to catch future increases.
        """
        if level != SignalLevelType.VERY_HIGH:
            return False

        threshold = self.config.cache_thresholds.high_spread_threshold
        factor = self.config.cache_thresholds.very_high_recheck_factor

        return (new_spread < old_spread * factor and
                new_spread > threshold)

    async def _execute_cache_operations(
        self,
        operations: list[tuple[str, dict, str]]
    ) -> None:
        """Execute cache set/delete operations in batch with TTL."""
        if not operations:
            return

        pipe = self.cache.client.pipeline()

        cache_ttl = self.config.cache_ttl

        for key, value, operation in operations:
            if operation == "set" or operation == "update":
                pipe.set(key, orjson.dumps(value))  # no TTL — infinite cache prevents duplicate signals
            elif operation == "delete":
                pipe.delete(key)

        await pipe.execute()

    async def should_broadcast_token_group(
        self,
        token_id: str,
        results: list[dict]
    ) -> bool:
        """
        Check if a token group should be broadcast.
        
        Uses TOKEN-level cache (not pair-level) and filters by MAX spread/profit
        across all pairs for the token.
        
        Args:
            token_id: Global token ID
            results: List of all arbitrage results for this token
            
        Returns:
            True if the token group should be broadcast
        """
        if self.cache is None:
            raise RuntimeError("Cache not initialized for ArbitrageFilter")
        
        if not results:
            return False
        
        # Find max spread and profit across all pairs
        max_spread = max(r.get("spread_percent", 0) for r in results)
        max_profit = max(r.get("profit", 0) for r in results)
        max_level = max((SignalLevelType(r.get("level", "None")) for r in results), 
                       default=SignalLevelType.NONE)
        
        # Use token-level cache key
        cache_key = f"arbitrage:{token_id}"
        
        # Get cached value
        cached_raw = await self.cache.client.get(cache_key)
        cached = None
        if cached_raw:
            try:
                cached = orjson.loads(cached_raw)
            except Exception as e:
                logger.warning(f"Failed to parse cached token group: {e}")
        
        # No cache entry - broadcast
        if not cached:
            # Save to cache
            cache_value = {
                "max_spread": max_spread,
                "max_profit": max_profit,
                "level": max_level.value,
                "timestamp": time.time()
            }
            await self.cache.client.set(cache_key, orjson.dumps(cache_value))
            
            # Set cooldown
            await self.cache.client.set(
                f"arb_cd:{cache_key}", b"1",
                ex=self.config.cooldown_ttl,
                nx=True
            )
            
            return True
        
        # Extract cached values
        old_max_spread = float(cached.get("max_spread", 0) or 0.0)
        old_max_profit = float(cached.get("max_profit", 0) or 0.0)
        old_level = SignalLevelType(cached.get("level", "None"))
        
        # Check for cache reset (spread dropped too low)
        reset_threshold = self.config.cache_thresholds.spread_reset.get(old_level, 0)
        if max_spread < reset_threshold:
            await self.cache.client.delete(cache_key)
            return False
        
        # Level upgraded - always broadcast
        if max_level > old_level:
            cache_value = {
                "max_spread": max_spread,
                "max_profit": max_profit,
                "level": max_level.value,
                "timestamp": time.time()
            }
            await self.cache.client.set(cache_key, orjson.dumps(cache_value))
            
            # CRITICAL FIX (2026-02-19): Reset cooldown on level upgrade
            # Previously, cooldown from Low signal blocked High signal broadcast
            # Now we overwrite cooldown (without nx=True) to allow immediate broadcast
            cooldown_key = f"arb_cd:{cache_key}"
            await self.cache.client.set(cooldown_key, b"1", ex=self.config.cooldown_ttl)
            
            logger.debug(f"Level upgrade: {old_level.value} -> {max_level.value} for {cache_key}, broadcasting")
            return True  # Always broadcast on level upgrade
        
        # Same level - check if changes are significant
        spread_diff = max_spread - old_max_spread
        profit_diff = abs(max_profit - old_max_profit)
        
        # Get thresholds
        if old_max_spread > self.config.cache_thresholds.high_spread_threshold:
            spread_threshold = max(
                old_max_spread * self.config.cache_thresholds.high_spread_trigger_percent,
                self.config.cache_thresholds.high_spread_trigger_min
            )
        else:
            spread_threshold = self.config.cache_thresholds.spread_trigger.get(
                max_level, float("inf")
            )
        
        profit_threshold_pct = self.config.cache_thresholds.profit_trigger.get(
            max_level, 100.0
        )
        
        # Check if change is significant
        spread_changed = spread_diff >= spread_threshold
        profit_pct_changed = (profit_diff / max(old_max_profit, 1.0)) * 100 >= profit_threshold_pct
        profit_abs_changed = profit_diff >= self.config.profit_trigger_min_abs
        profit_changed = profit_pct_changed and profit_abs_changed
        
        if spread_changed or profit_changed:
            # Update cache
            cache_value = {
                "max_spread": max_spread,
                "max_profit": max_profit,
                "level": max_level.value,
                "timestamp": time.time()
            }
            await self.cache.client.set(cache_key, orjson.dumps(cache_value))
            
            # Check cooldown
            won_lock = await self.cache.client.set(
                f"arb_cd:{cache_key}", b"1",
                ex=self.config.cooldown_ttl,
                nx=True
            )
            return bool(won_lock)
        
        # No significant change
        return False

from abc import ABC
import time
from typing import Any, Optional
from uuid import UUID

from infrastructure.database.models.base import SourceType
from infrastructure.database.models.sources import GlobalTokenSource
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.base import BaseParser
from infrastructure.services.collectors.structures.maps import chain_block_time
from infrastructure.services.collectors.structures.normalizer import ChainNormalizer
from infrastructure.services.collectors.token_collector.supports.merge_global_token import get_merge_finder, \
    MergeCandidateFinder
from infrastructure.services.common.cache import RedisCache
from settings import settings, logger


# Trusted sources that can merge by symbol + price for tokens without chain info
# These are major exchanges where token prices can be trusted for comparison
TRUSTED_SOURCES = {
    "hyperliquid", "binance", "bybit", "okx", "kucoin", "coinbase",
    # CEX sources that were creating excessive duplicates — now trusted with price validation
    "mexc", "gate", "xt", "lbank", "bitmart", "ascendex",
}

# Maximum price difference (%) to consider tokens the same
# If prices differ by more than this, tokens are NOT merged (prevents phantom spreads)
# 3% threshold for chainless token merge — prevents phantom spreads
MAX_PRICE_DIFF_PCT = 3.0


class BaseTokenParser(BaseParser, ABC):
    def __init__(
            self,
            cache: RedisCache,
            chain_normalizer: ChainNormalizer,
            repo: RequestsRepo,
            *,
            source_name: str,
            source_type: SourceType
    ):
        super().__init__(cache, repo, source_name=source_name, source_type=source_type)
        self.chain_normalizer = chain_normalizer
        self.merge_finder: MergeCandidateFinder = get_merge_finder(repo, cache, settings.environment)

    async def save_token_data_cex(self, token_map: list[dict]) -> None:
        import gc
        import psutil
        import os
        
        # MEMORY FIX: Monitor memory usage during large operations
        process = psutil.Process(os.getpid())
        memory_start = process.memory_info().rss / 1024 / 1024  # MB
        
        logger.info(f"{self.source_name}: save_token_data_cex started with {len(token_map)} tokens (Memory: {memory_start:.1f}MB)")
        
        # MEMORY FIX: Warn if processing very large token maps
        if len(token_map) > 5000:
            logger.warning(f"{self.source_name}: Large token_map size: {len(token_map)} tokens")
        
        try:
            token_map = self.normalize_token_map(token_map)
            source_id = self.source.id

            # SPOT/FUTURES separation handled by market_type validation in assign_global_ids
            # and FIX-006, NOT by modifying base_coin_name (which creates _FUTURES symbols in DB)

            tokens_with_chains = []
            tokens_without_chains = []
            basecoin_pairs_map = {}

            # Разделяем токены на те, у которых есть chain info и без
            for token in token_map:
                chains = token.get("chains", [])
                if not chains:
                    tokens_without_chains.append(token)
                    continue

                pairs = set()
                for chain in chains:
                    addr = chain.get("contract_address")
                    chain_name = chain.get("chain_name")
                    if not chain_name:
                        continue
                    pair = (chain_name.lower(), addr.lower() if addr else None)
                    pairs.add(pair)

                if not pairs:
                    tokens_without_chains.append(token)
                    continue

                tokens_with_chains.append(token)
                basecoin_pairs_map.setdefault(token["base_coin_name"], set()).update(pairs)

            logger.info(f"{self.source_name}: {len(tokens_with_chains)} tokens with chains, {len(tokens_without_chains)} without chains")

            # назначаем global_token_id через MergeCandidateFinder для токенов с chains
            logger.info(f"{self.source_name}: starting assign_global_ids for tokens with chains...")
            basecoin_global_ids = await self.merge_finder.assign_global_ids(
                source_id=source_id,
                source_name=self.source_name,
                basecoin_pairs_map=basecoin_pairs_map,
                token_map=token_map
            )
            logger.info(f"{self.source_name}: assign_global_ids completed, got {len(basecoin_global_ids)} global_ids")

            # Для токенов без chains - пытаемся найти или создать global_token по symbol
            logger.info(f"{self.source_name}: starting _assign_global_ids_for_chainless_tokens...")
            chainless_global_ids = await self._assign_global_ids_for_chainless_tokens(
                source_id=source_id,
                tokens=tokens_without_chains,
                existing_global_ids=basecoin_global_ids
            )
            logger.info(f"{self.source_name}: chainless_global_ids completed, got {len(chainless_global_ids)} global_ids")
            basecoin_global_ids.update(chainless_global_ids)

            # присваиваем global_token_id цепям токенов (только для токенов с chains)
            for token in tokens_with_chains:
                g_id = basecoin_global_ids.get(token["base_coin_name"])
                if g_id:
                    for chain in token.get("chains", []):
                        chain["global_token_id"] = g_id

            # создаем/обновляем chains (только для токенов с chain info)
            logger.info(f"{self.source_name}: creating/updating chains...")
            chain_data = self.extract_all_chain(tokens_with_chains)
            chain_map = await self.repo.chains.get_or_create_bulk_by_names_and_block_time(chain_data)
            logger.info(f"{self.source_name}: chains done, got {len(chain_map)} chains")

            # Remove temporary fields before saving to DB
            for token in token_map:
                token.pop("price", None)
                token.pop("skip_futures_suffix", None)

            # создаем/обновляем ВСЕ токены (и с chains и без)
            logger.info(f"{self.source_name}: bulk_create_or_update tokens...")
            await self.repo.tokens.bulk_create_or_update(token_map)
            logger.info(f"{self.source_name}: tokens saved")

            # получаем реальные id токенов (только ID, не полные объекты)
            logger.info(f"{self.source_name}: get_token_id_map...")
            token_id_map = await self.repo.tokens.get_token_id_map(source_id=source_id)
            logger.info(f"{self.source_name}: got {len(token_id_map)} token ids")

            # Обрабатываем токены С chain info для token_on_chain
            valid_token_map = []
            token_ids = []
            for token in tokens_with_chains:
                token_id = token_id_map.get((token["symbol"], token["market_type"]))
                if token_id:
                    valid_token_map.append(token)
                    token_ids.append(token_id)

            # подготавливаем token_on_chain (только для токенов с chains)
            token_on_chain_data = self._prepare_token_on_chain_data(token_ids, valid_token_map, chain_map)
            for item in token_on_chain_data:
                item["global_token_id"] = basecoin_global_ids.get(item["base_coin_name"])
            # Drop items where global_token_id is None (UNIQUE conflict: different token, same symbol)
            before_none_filter = len(token_on_chain_data)
            token_on_chain_data = [item for item in token_on_chain_data if item.get("global_token_id") is not None]
            if len(token_on_chain_data) < before_none_filter:
                logger.info(
                    f"{self.source_name}: dropped {before_none_filter - len(token_on_chain_data)} token_chains "
                    f"with no GlobalToken (UNIQUE symbol conflict — different token, same symbol as existing GT)"
                )

            # FIX-006: Validate token_chains to prevent cross-chain contaminations
            logger.info(f"{self.source_name}: validating {len(token_on_chain_data)} token_chains for contamination...")
            token_on_chain_data = await self._validate_token_chains_for_contamination(token_on_chain_data)
            logger.info(f"{self.source_name}: {len(token_on_chain_data)} token_chains passed validation")

            # FIX-STALE-CHAIN: Delete token_chain rows for chains no longer present in exchange data.
            # Required when a token migrates to a different blockchain (e.g., Gate ELON: ETH→APT):
            # upsert uses ON CONFLICT (token_id, chain_id), so the old chain row is never removed
            # and stale contracts keep triggering wrong GT merges.
            stale_deleted = await self.repo.token_on_chain.delete_stale_token_chains(token_on_chain_data)
            if stale_deleted > 0:
                logger.warning(
                    f"{self.source_name}: Deleted {stale_deleted} stale token_chain entries "
                    f"(chain migration detected)"
                )

            # сохраняем token_on_chain
            await self.repo.token_on_chain.bulk_upsert_token_on_chains(token_on_chain_data)

            # создаем/обновляем global_token_sources
            # IMPORTANT: Only create for global_tokens that are actually used by this source's tokens
            # For tokens WITH chains: use global_token_id from token_on_chain_data (validated by contract address)
            # For tokens WITHOUT chains: use chainless_global_ids (source-specific)
            gts_global_ids = set()

            # From tokens with chains - get actual global_token_ids from token_on_chain
            for item in token_on_chain_data:
                if item.get("global_token_id"):
                    gts_global_ids.add(item["global_token_id"])

            # From chainless tokens - add source-specific global_ids
            for g_id in chainless_global_ids.values():
                gts_global_ids.add(g_id)

            if gts_global_ids:
                await self.repo.global_token_sources.bulk_create([
                    {"global_token_id": g_id, "source_id": source_id} for g_id in gts_global_ids
                ])

            # Run merge finder only periodically (every 30 minutes) instead of every iteration
            should_run = await self._should_run_merge_finder()
            if should_run:
                await self.merge_finder.find()
        
        finally:
            # MEMORY FIX: Cleanup and monitoring at end of processing
            try:
                # Force garbage collection after large operations
                gc.collect()
                
                # Monitor memory usage after processing
                memory_end = process.memory_info().rss / 1024 / 1024  # MB
                memory_growth = memory_end - memory_start
                
                logger.info(f"{self.source_name}: save_token_data_cex completed (Memory: {memory_end:.1f}MB, Growth: {memory_growth:+.1f}MB)")
                
                # MEMORY WARNING: Alert if significant memory growth
                if memory_growth > 100:  # More than 100MB growth
                    logger.warning(f"⚠️ {self.source_name}: High memory growth in save_token_data_cex: {memory_growth:.1f}MB")
                    
            except Exception as e:
                logger.warning(f"{self.source_name}: Error in memory monitoring cleanup: {e}")
                pass

    async def _check_market_compatibility(
            self,
            global_token_id: UUID,
            source_id: UUID,
            new_market_type: str,
            new_contract_size: Optional[float]
    ) -> bool:
        """
        Check if a token can be added to an existing global_token.

        Returns False if the existing global_token already has tokens from the same source
        with different market_type or contract_size (indicating incompatibility).
        """
        # Get all token_chains for this global_token from this source
        existing_tokens = await self.repo.tokens.get_by_global_token_and_source(
            global_token_id=global_token_id,
            source_id=source_id
        )

        if not existing_tokens:
            return True  # No existing tokens, compatible

        for existing_token in existing_tokens:
            # Check market_type mismatch
            if existing_token.market_type and new_market_type:
                if str(existing_token.market_type.value) != str(new_market_type):
                    logger.debug(
                        f"Market type conflict: existing {existing_token.market_type.value} "
                        f"vs new {new_market_type} for global_token {global_token_id}"
                    )
                    return False

            # Check contract_size mismatch (ignore nulls)
            existing_size = existing_token.contract_size
            if existing_size is not None and new_contract_size is not None:
                if abs(existing_size - new_contract_size) > 0.0001:  # Allow small floating point errors
                    logger.debug(
                        f"Contract size conflict: existing {existing_size} "
                        f"vs new {new_contract_size} for global_token {global_token_id}"
                    )
                    return False

        return True

    async def _assign_global_ids_for_chainless_tokens(
            self,
            source_id: UUID,
            tokens: list[dict],
            existing_global_ids: dict[str, UUID]
    ) -> dict[str, UUID]:
        """
        Assign global_token_id for tokens without chain info.

        Strategy:
        1. Group tokens by (base_coin_name, market_type, contract_size) to prevent
           mixing SPOT/FUTURES or tokens with different contract_size
        2. For each group, try to reuse existing global_id if compatible
        3. For TRUSTED sources: try to merge by symbol + price validation (<3% difference)
        4. Otherwise: create source-specific global_token with format "{symbol}:{source_name}"

        This prevents SPOT and FUTURES tokens with same symbol from merging into one global_token.
        """
        if not tokens:
            return {}

        # Group tokens by (base_coin_name, market_type, contract_size)
        # This ensures SPOT and FUTURES get separate global_tokens
        token_groups: dict[tuple[str, str, Optional[float]], list[dict]] = {}
        for token in tokens:
            base_coin_name = token["base_coin_name"]
            market_type = token.get("market_type")
            contract_size = token.get("contract_size")
            market_str = str(market_type.value) if market_type else "unknown"

            # Normalize contract_size (treat None and very small values as same)
            normalized_size = None if contract_size is None or abs(contract_size) < 0.0001 else contract_size

            key = (base_coin_name, market_str, normalized_size)
            token_groups.setdefault(key, []).append(token)

        result = {}
        tokens_needing_global_id = []
        is_trusted = self.source_name in TRUSTED_SOURCES

        # Process each group separately
        for (base_coin_name, market_str, contract_size), group_tokens in token_groups.items():
            # CRITICAL: Create unique result key for this specific group
            # This ensures SPOT and FUTURES get separate entries
            # We'll use base_coin_name with suffix for non-SPOT markets
            # Use base_coin_name as key — it already has _FUTURES suffix from save_token_data_cex
            unique_key = base_coin_name

            # Try to reuse existing global_id ONLY for SPOT tokens
            # FUTURES tokens should always get new global_tokens
            if market_str == "SPOT" and base_coin_name in existing_global_ids:
                existing_global_id = existing_global_ids[base_coin_name]

                # CRITICAL FIX (2026-02-15): Check if existing GT has chains
                # If GT already has chains (chained tokens), DO NOT reuse for chainless tokens!
                # This prevents cross-chain conflicts when chainless token later gets chains.
                # Example bug: VON chainless merged with VON (ETH), later VON added BSC chain → contamination
                has_chains = await self.repo.token_on_chain.exists_for_global_token(
                    global_token_id=existing_global_id
                )

                if has_chains:
                    # Existing GT has chains - don't reuse for chainless token
                    logger.info(
                        f"Not reusing global_token {existing_global_id} for chainless {base_coin_name}: "
                        f"GT already has chains (potential cross-chain conflict)"
                    )
                    tokens_needing_global_id.extend(group_tokens)
                    continue

                # Check if this group is compatible with existing global_token
                is_compatible = await self._check_market_compatibility(
                    global_token_id=existing_global_id,
                    source_id=source_id,
                    new_market_type=market_str,
                    new_contract_size=contract_size
                )

                if is_compatible:
                    # Compatible - all tokens in this group use same global_id
                    result[unique_key] = existing_global_id
                    logger.debug(
                        f"Reusing global_token {existing_global_id} for {base_coin_name} SPOT (chainless-only)"
                    )
                    continue
                else:
                    # Not compatible - need new global_token for this group
                    logger.info(
                        f"Token {base_coin_name} SPOT incompatible with existing global_token "
                        f"{existing_global_id}, creating new one"
                    )
                    tokens_needing_global_id.extend(group_tokens)
                    continue

            # No existing global_id for SPOT, or this is FUTURES - need to create new one
            tokens_needing_global_id.extend(group_tokens)

        if not tokens_needing_global_id:
            return result

        # For trusted sources, try to merge by symbol + price validation
        symbols_to_merge = []
        symbols_for_source_specific = []

        if is_trusted:
            # Get symbols to check
            symbol_list = [t["base_coin_name"] for t in tokens_needing_global_id]

            # Look for existing global_tokens with exact symbol match
            existing_global = await self.repo.global_tokens.get_by_symbols_with_prices(symbol_list)
            existing_by_symbol = {gt.symbol: gt for gt in existing_global}

            for token in tokens_needing_global_id:
                symbol = token["base_coin_name"]
                new_price = token.get("price")  # Price from API (e.g., allMids for Hyperliquid)

                if symbol in existing_by_symbol:
                    existing_gt = existing_by_symbol[symbol]
                    existing_price = self._compute_global_token_price(existing_gt)

                    # Check price difference
                    if new_price and existing_price:
                        diff_pct = abs(new_price - existing_price) / ((new_price + existing_price) / 2) * 100
                        if diff_pct <= MAX_PRICE_DIFF_PCT:
                            result[symbol] = existing_gt.id
                            logger.info(
                                f"Merged {symbol} from {self.source_name} with existing global_token "
                                f"(price diff: {diff_pct:.2f}%)"
                            )
                            continue
                        else:
                            logger.info(
                                f"Rejected merge for {symbol} from {self.source_name}: "
                                f"price diff {diff_pct:.2f}% > {MAX_PRICE_DIFF_PCT}% "
                                f"(new: {new_price:.6f}, existing: {existing_price:.6f})"
                            )
                            symbols_for_source_specific.append(token)
                            continue
                    elif new_price is None and existing_price:
                        # No price for new token, but existing has price - merge anyway for trusted source
                        result[symbol] = existing_gt.id
                        logger.info(f"Merged {symbol} from {self.source_name} (no price to compare, trusted source)")
                        continue

                # No existing global_token with this symbol - create new one
                symbols_to_merge.append(token)
        else:
            # Non-trusted sources: all go to source-specific
            symbols_for_source_specific = tokens_needing_global_id

        # Create new global_tokens for trusted symbols that need them
        if symbols_to_merge:
            new_global_tokens = await self.repo.global_tokens.bulk_create(
                [{"symbol": t["base_coin_name"]} for t in symbols_to_merge]
            )
            for gt in new_global_tokens:
                result[gt.symbol] = gt.id
                logger.info(f"Created new global_token for {gt.symbol} from {self.source_name}")

        # Handle source-specific symbols
        if symbols_for_source_specific:
            # Build source-specific symbols preserving market_type info
            # For SPOT: symbol:source, for FUTURES: symbol_FUTURES:source
            source_specific_symbols = []
            original_symbols = []
            for t in symbols_for_source_specific:
                base_coin_name = t["base_coin_name"]
                market_type = t.get("market_type")
                market_str = str(market_type.value) if market_type else "unknown"
                
                # Add _FUTURES suffix for FUTURES tokens to prevent collision with SPOT
                if market_str == "FUTURES":
                    source_symbol = f"{base_coin_name}_FUTURES:{self.source_name}"
                else:
                    source_symbol = f"{base_coin_name}:{self.source_name}"
                
                source_specific_symbols.append(source_symbol)
                original_symbols.append(base_coin_name)

            existing = await self.repo.global_tokens.get_by_symbols(source_specific_symbols)
            existing_by_symbol = {gt.symbol: gt.id for gt in existing}

            symbols_to_create = []
            for original_symbol, source_symbol in zip(original_symbols, source_specific_symbols):
                if source_symbol in existing_by_symbol:
                    result[original_symbol] = existing_by_symbol[source_symbol]
                else:
                    symbols_to_create.append((original_symbol, source_symbol))

            if symbols_to_create:
                new_global_tokens = await self.repo.global_tokens.bulk_create(
                    [{"symbol": source_symbol} for _, source_symbol in symbols_to_create]
                )
                source_to_original = {source_symbol: original for original, source_symbol in symbols_to_create}
                for gt in new_global_tokens:
                    original_symbol = source_to_original.get(gt.symbol)
                    if original_symbol:
                        result[original_symbol] = gt.id

        return result

    @staticmethod
    def _compute_global_token_price(gt) -> Optional[float]:
        """Compute mid-price for global token from orderbook data."""
        best_bid = None
        best_ask = None

        for price in getattr(gt, 'prices', []) or []:
            ob = getattr(price, 'orderbook', None) or {}
            bids = ob.get("bids") or []
            asks = ob.get("asks") or []

            if bids:
                try:
                    max_bid = max(float(b[0]) for b in bids if b and b[0] is not None)
                    best_bid = max(best_bid, max_bid) if best_bid is not None else max_bid
                except (ValueError, TypeError):
                    pass

            if asks:
                try:
                    min_ask = min(float(a[0]) for a in asks if a and a[0] is not None)
                    best_ask = min(best_ask, min_ask) if best_ask is not None else min_ask
                except (ValueError, TypeError):
                    pass

        if best_bid is None or best_ask is None:
            return None

        return (best_bid + best_ask) / 2

    async def save_token_data_dex(self, token_map: list[dict]) -> list[dict]:
        token_map = self.normalize_token_map(token_map)
        source_id = self.source.id
        source_name = self.source_name

        basecoin_pairs_map = {}
        for token in token_map:
            pairs = set()
            for chain in token.get("chains", []):
                chain_name = chain.get("chain_name")
                addr = chain.get("contract_address")
                if not chain_name:
                    continue
                pairs.add((chain_name.lower(), addr.lower() if addr else None))
            if pairs:
                basecoin_pairs_map.setdefault(token["base_coin_name"], set()).update(pairs)

        # назначаем global_token_id через merge_finder
        basecoin_global_id = await self.merge_finder.assign_global_ids(
            source_id=source_id,
            source_name=source_name,
            basecoin_pairs_map=basecoin_pairs_map,
            token_map=token_map,
        )

        # проставляем global_token_id в chains
        for token in token_map:
            g_id = basecoin_global_id.get(token["base_coin_name"])
            if not g_id:
                continue
            for chain in token.get("chains", []):
                chain["global_token_id"] = g_id

        # апдейт токенов
        await self.repo.tokens.bulk_create_or_update(token_map)
        tokens = await self.repo.tokens.get_all(source_id=source_id)
        token_id_map = {(t.symbol, t.market_type): t.id for t in tokens}

        valid_token_map = []
        token_ids = []
        for token in token_map:
            token_id = token_id_map.get((token["symbol"], token["market_type"]))
            if token_id:
                token["id"] = token_id
                valid_token_map.append(token)
                token_ids.append(token_id)

        # апдейт chains
        chain_data = self.extract_all_chain(valid_token_map)
        chain_map = await self.repo.chains.get_or_create_bulk_by_names_and_block_time(chain_data)

        # подготавливаем token_on_chain
        token_on_chain_data = self._prepare_token_on_chain_data(token_ids, valid_token_map, chain_map)
        for item in token_on_chain_data:
            item["global_token_id"] = basecoin_global_id[item["base_coin_name"]]

        # FIX-STALE-CHAIN: same cleanup for DEX path
        stale_deleted = await self.repo.token_on_chain.delete_stale_token_chains(token_on_chain_data)
        if stale_deleted > 0:
            logger.warning(
                f"{self.source_name}: Deleted {stale_deleted} stale token_chain entries "
                f"(chain migration detected)"
            )

        await self.repo.token_on_chain.bulk_upsert_token_on_chains(token_on_chain_data)

        # создаем/обновляем global_token_sources
        unique_gts_pairs = list({
            (item["global_token_id"], source_id)
            for item in token_on_chain_data
        })
        if unique_gts_pairs:
            await self.repo.global_token_sources.bulk_create([
                {"global_token_id": g_id, "source_id": s_id} for g_id, s_id in unique_gts_pairs
            ])

        # Filter by source_id instead of loading all
        created_gts: list[GlobalTokenSource] = await self.repo.global_token_sources.get_all(source_id=source_id)
        gts_id_map = {
            (str(gts.global_token_id), str(gts.source_id)): gts.id for gts in created_gts
        }

        result = []
        new_token_global_token_source = []
        for token in valid_token_map:
            t_id = token.get("id")
            for chain in token.get("chains", []):
                g_id = chain.get("global_token_id")
                if not g_id or t_id is None:
                    continue
                gts_id = gts_id_map.get((str(g_id), str(source_id)))
                if gts_id is None:
                    new_token_global_token_source.append({"global_token_id": g_id, "source_id": source_id})
                    continue

                result.append({
                    "token_id": t_id,
                    "global_token_id": g_id,
                    "global_token_source_id": gts_id,
                    "symbol": token.get("symbol"),
                    "quote_asset": token.get("quote_asset"),
                    "price_usd": token.get("price_usd"),
                    "liquidity_usd": token.get("liquidity_usd"),
                })

        if new_token_global_token_source:
            await self.repo.global_token_sources.bulk_create(new_token_global_token_source)

        # Run merge finder only periodically (every 30 minutes) instead of every iteration
        should_run = await self._should_run_merge_finder()
        if should_run:
            await self.merge_finder.find()

        return result

    @staticmethod
    def _prepare_token_on_chain_data(token_ids: list[UUID], token_map: list[dict], chain_map: dict) -> list[dict]:
        seen_pairs = set()
        result = []

        for token_id, token in zip(token_ids, token_map):
            for chain in token.get("chains", []):
                chain_obj = chain_map.get(chain["chain_name"])
                if not chain_obj:
                    continue

                key = (token_id, chain_obj.id)
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)

                result.append({
                    "base_coin_name": token.get("base_coin_name"),
                    "token_id": token_id,
                    "chain_id": chain_obj.id,
                    "chain_name": chain["chain_name"],
                    "contract_address": chain["contract_address"],  # preserve original case
                    "deposit_enabled": chain.get("deposit_enabled"),
                    "min_deposit_amount": chain.get("min_deposit_amount"),
                    "deposit_confirm": chain.get("deposit_confirm"),
                    "deposit_tax": chain.get("deposit_tax"),
                    "withdraw_enabled": chain.get("withdraw_enabled"),
                    "min_withdraw_amount": chain.get("withdraw_min"),
                    "withdrawal_fee": chain.get("withdrawal_fee"),
                    "withdraw_tax": chain.get("withdraw_tax"),
                })

        return result

    def normalize_token_map(self, token_map: list[dict]) -> list[dict]:
        for token in token_map:
            for chain in token.get("chains", []):
                raw_name = chain["chain_name"]
                norm = self.chain_normalizer.normalize_from_source(raw_name)
                if not norm:
                    continue

                chain["block_time"] = chain_block_time.get(norm)
                db_name = self.chain_normalizer.normalize_to_db(norm)
                if db_name:
                    chain["chain_name"] = db_name

        return token_map

    @staticmethod
    def extract_all_chain(token_map: list[dict]) -> list[dict[str, Any]]:
        seen = {}
        for token in token_map:
            for chain in token.get("chains", []):
                name = chain["chain_name"]
                if name not in seen:
                    seen[name] = {"name": name, "block_time": chain.get("block_time")}
        return list(seen.values())

    async def _should_run_merge_finder(self) -> bool:
        """
        Check if merge finder should run (every 30 minutes per source)
        
        MEMORY FIX: Added memory usage check to prevent running when memory is high.
        """
        try:
            import psutil
            import os
            
            # MEMORY FIX: Skip merge finder if memory usage is too high
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            if memory_mb > 1500:  # More than 1.5GB
                logger.warning(f"⚠️ Skipping merge finder - high memory usage: {memory_mb:.1f}MB")
                return False
        except Exception as e:
            logger.warning(f"Error checking memory for merge finder: {e}")
        
        cache_key = f"{self.source_name}:last_merge_run"
        last_run = await self.cache.get(cache_key)

        if not last_run:
            await self.cache.set(cache_key, time.time(), timeout_secs=1800)  # 30 minutes
            return True

        return False

    async def _mark_chainless_conflict(self, base_asset: str) -> None:
        """
        FIX-008 (2026-03-07): Mark that this source has a DIFFERENT token with the same symbol.

        When FIX-006 detects a contract conflict (same chain, different contract → different token),
        we record (source_name, base_asset) in Redis so the distributor won't do a chainless
        symbol-fallback and wrongly link this exchange's token to the existing GlobalToken.

        Key: distributor:contract_conflict:{source_name}
        Value: Redis SET of base_asset symbols
        TTL: 30 days (refreshed on each token-collector run)
        """
        try:
            redis_key = f"distributor:contract_conflict:{self.source_name}"
            await self.cache.client.sadd(redis_key, base_asset.upper())
            await self.cache.client.expire(redis_key, 30 * 24 * 3600)  # 30 days
        except Exception as e:
            logger.warning(f"{self.source_name}: Failed to mark chainless conflict for {base_asset}: {e}")

    async def _validate_token_chains_for_contamination(
        self, 
        token_on_chain_data: list[dict]
    ) -> list[dict]:
        """
        FIX-006: Filter out token_chains that would create cross-chain contaminations.
        
        Validates that each token_chain's global_token_id doesn't have conflicting chains.
        
        Returns: Filtered list with only valid token_chains.
        """
        if not token_on_chain_data:
            return []
        
        # Group by global_token_id for batch loading
        gt_ids = {item["global_token_id"] for item in token_on_chain_data if item.get("global_token_id")}
        
        if not gt_ids:
            return token_on_chain_data
        
        # Load existing chains for all GTs in one query
        from infrastructure.database.models import TokenOnChain
        from sqlalchemy import select
        
        gt_chains_map = {}  # {gt_id: {chain_id: contract_address}}
        
        async with self.repo.sessionmaker() as session:
            stmt = select(
                TokenOnChain.global_token_id,
                TokenOnChain.chain_id,
                TokenOnChain.contract_address
            ).where(
                TokenOnChain.global_token_id.in_(gt_ids)
            )
            
            result = await session.execute(stmt)
            rows = result.fetchall()
            
            for gt_id, chain_id, contract in rows:
                if gt_id not in gt_chains_map:
                    gt_chains_map[gt_id] = {}
                
                # Normalize contract
                contract_norm = None if contract in [None, "", "none", " ", "null"] else contract.lower()
                
                # Store first occurrence (in case of duplicates in DB)
                if chain_id not in gt_chains_map[gt_id]:
                    gt_chains_map[gt_id][chain_id] = contract_norm
        
        # Filter token_on_chain_data
        valid_items = []
        filtered_count = 0
        
        for item in token_on_chain_data:
            gt_id = item.get("global_token_id")
            chain_id = item.get("chain_id")
            contract = item.get("contract_address")
            
            if not gt_id or not chain_id:
                valid_items.append(item)
                continue
            
            # Normalize contract
            contract_norm = None if contract in [None, "", "none", " ", "null"] else contract.lower()
            
            # Get existing chains for this GT
            existing_chains = gt_chains_map.get(gt_id, {})
            
            if not existing_chains:
                # GT has no existing chains - OK to add
                valid_items.append(item)
                # Update map for next iterations in this batch
                gt_chains_map.setdefault(gt_id, {})[chain_id] = contract_norm
                continue
            
            # Check if this chain already exists in GT
            if chain_id in existing_chains:
                existing_contract = existing_chains[chain_id]
                
                # Same chain contract resolution:
                # NULL == NULL    → OK (both unknown)
                # contract == contract → OK (exact match)
                # NULL → contract  → OK (FIX-006b: learning contract for first time, no conflict)
                # contract → NULL  → SKIP (don't erase known contract)
                # contractA → contractB → SKIP (real conflict)
                if existing_contract is None and contract_norm is None:
                    valid_items.append(item)
                    continue

                if existing_contract and contract_norm and existing_contract == contract_norm:
                    valid_items.append(item)
                    continue

                # FIX-006b: NULL → contract is safe (we previously had no contract data)
                if existing_contract is None and contract_norm:
                    valid_items.append(item)
                    # Update map so subsequent batch items see the new contract
                    gt_chains_map.setdefault(gt_id, {})[chain_id] = contract_norm
                    continue

                # contract → NULL or contractA → contractB → SKIP
                # This means the exchange has a DIFFERENT TOKEN with same symbol on same chain.
                # Mark it in Redis so distributor won't do chainless fallback for this source+symbol.
                base_asset = item.get("base_coin_name", "")
                if base_asset and existing_contract and contract_norm:
                    # Only mark as conflict when BOTH sides have contracts (true different token)
                    await self._mark_chainless_conflict(base_asset)
                logger.warning(
                    f"FIX-006: Filtered token_chain for {item.get('base_coin_name', '?')}: "
                    f"GT {gt_id} already has chain {chain_id} with different contract "
                    f"(existing: {existing_contract}, new: {contract_norm})"
                )
                filtered_count += 1
                continue
            
            # Check if GT has OTHER chains (cross-chain contamination)
            if len(existing_chains) > 0:
                # GT already has chains, and this is a NEW chain (chain_id not in existing_chains)
                # → CROSS-CHAIN CONTAMINATION (likely different token with same symbol on diff chain)
                # Mark for distributor so it skips chainless fallback for this source+symbol.
                base_asset = item.get("base_coin_name", "")
                if base_asset and contract_norm:
                    # Only mark when new token HAS a contract (confirms it's a real different token)
                    await self._mark_chainless_conflict(base_asset)
                chain_names_str = ", ".join([str(c) for c in existing_chains.keys()])
                logger.warning(
                    f"FIX-006: Filtered token_chain for {item.get('base_coin_name', '?')}: "
                    f"GT {gt_id} already has other chains ({chain_names_str}), "
                    f"cannot add new chain {chain_id} (cross-chain contamination prevention)"
                )
                filtered_count += 1
                continue
            
            # All checks passed
            valid_items.append(item)
            # Update map
            gt_chains_map.setdefault(gt_id, {})[chain_id] = contract_norm
        
        if filtered_count > 0:
            logger.info(
                f"{self.source_name}: FIX-006 filtered {filtered_count}/{len(token_on_chain_data)} "
                f"token_chains to prevent contamination"
            )
        
        return valid_items

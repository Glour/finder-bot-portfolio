import asyncio

import orjson
from redis import asyncio as aioredis

from infrastructure.database import setup as database
from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.common.cache import RedisCache
from settings import settings, logger

# FIX-007 symbol blacklist: tokens that should NOT be resolved via symbol lookup.
# Format: {source_name: {base_asset_symbol}}
# Use when a token on a source has the same symbol as a global token but is a different asset.
FIX007_BLACKLIST: dict[str, set] = {
    "xt": {"Q"},      # XT Q_USDT (~2.5e-7) ≠ global Q (~0.013, BSC token)
    "bybit": {"ELON"}, # Bybit ELON (~$0.10) ≠ Dogelon Mars ELON (~$3.7e-8, ETH contract)
}


async def _lookup_gt_by_symbol(base_asset: str, source, repo: RequestsRepo) -> tuple | None:
    """
    FIX-007: Fallback GT lookup by symbol for chainless tokens that have no global_token_sources entry.

    Handles tokens whose chain associations were filtered by FIX-006 (e.g. BTC/ETH/BNB on Asterdex
    where the deposit API returns wallet addresses instead of token contracts, causing contract
    mismatch with existing chain records).

    Searches GlobalToken by exact symbol match, creates global_token_sources entry if found.
    Returns (global_token_id, global_source_id) or None.
    """
    try:
        from infrastructure.database.models import GlobalToken
        from sqlalchemy import select

        async with repo.sessionmaker() as session:
            stmt = select(GlobalToken).where(
                GlobalToken.symbol == base_asset,
                GlobalToken.is_deleted == False,
            )
            result = await session.execute(stmt)
            gts = result.scalars().all()

        if not gts:
            return None

        # Use only if there's an unambiguous single match
        if len(gts) > 1:
            logger.debug(
                f"FIX-007: ambiguous symbol '{base_asset}' ({len(gts)} GTs found), skipping"
            )
            return None

        gt = gts[0]
        global_source = await repo.global_token_sources.get_or_create(gt.id, source.id)
        logger.info(
            f"FIX-007: resolved chainless '{base_asset}' via symbol lookup "
            f"→ GT {gt.id}, GTS {global_source.id}"
        )
        return (gt.id, global_source.id)

    except Exception as e:
        logger.error(f"FIX-007: symbol lookup failed for '{base_asset}': {e}")
        return None


async def _build_chainless_gt_map(source, repo: RequestsRepo) -> dict:
    """
    Build a lookup map for chainless tokens (no token_on_chains) to resolve their global_token_id.

    Used for sources like Hyperliquid FUTURES where tokens have no chain data but do have
    global_token_sources entries created by the token-collector's chainless merge logic.

    Returns: {lookup_key: (global_token_id, global_source_id)}
    lookup_key candidates per token:
      - tok.base_asset                              (merged case: GT symbol = "BTC")
      - tok.base_asset + "_FUTURES:" + source.name  (source-specific FUTURES case)
      - tok.base_asset + ":" + source.name          (source-specific SPOT case)
    """
    try:
        gts_records = await repo.global_token_sources.get_all_with_global_tokens(source_id=source.id)
        result = {}
        for gts in gts_records:
            gt = getattr(gts, "global_token", None)
            if gt and not gt.is_deleted:
                result[gt.symbol] = (gts.global_token_id, gts.id)
        return result
    except Exception as e:
        logger.error(f"Failed to build chainless GT map for {source.name}: {e}")
        return {}


async def load_source_tokens_incremental(source, repo: RequestsRepo, redis: RedisCache, tokens_load_limit: int):
    """
    Load tokens for a source and add them to Redis incrementally.

    Optimized to NOT pre-load all global_token_sources (which can be millions for large exchanges).
    Instead, relies on token_on_chains relationship which is already loaded with tokens.

    Chainless tokens (e.g., Hyperliquid FUTURES perps) fall back to GT lookup via
    global_token_sources symbol map (safe: filtered by source_id).
    """
    redis_key_prefix = f"available_tokens:{source.name}:{source.id}"

    # Cache for global_token_sources - populated on-demand
    gts_cache = {}

    # Preload chainless GT map once per source (used as fallback for tokens without token_on_chains)
    # This is needed for sources like Hyperliquid FUTURES that have chainless perp tokens.
    # NOTE: Only load this map if there are tokens at all; for sources with many GT entries
    # (e.g., large CEX), this load is bounded by the actual source's GTS count.
    chainless_gt_map = None  # Lazy load on first chainless token encountered

    for market_type in MarketType:
        try:
            tokens = await repo.tokens.get_all(
                source_id=source.id,
                market_type=market_type,
                limit=tokens_load_limit,
            )
            redis_key = f"{redis_key_prefix}:{market_type.value}"

            existing_tokens = await redis.client.smembers(redis_key)
            existing_ids = set()
            for t in existing_tokens:
                try:
                    data = orjson.loads(t)
                    if data.get("global_token_id"):
                        existing_ids.add(data["global_token_id"])
                except Exception as e:
                    logger.error(f"Failed to parse token data: {e}")
                    continue

            new_json_tokens = []
            registry_json_tokens = []  # FIX-008: all resolved tokens (for source_registry)
            chainless_resolved = 0
            chainless_skipped = 0

            for tok in tokens:
                # Get global_token_id from token_on_chains (primary source)
                global_token_id = None
                global_source_id_override = None

                if tok.token_on_chains:
                    global_token_id = tok.token_on_chains[0].global_token_id
                else:
                    # Chainless token: try to resolve via global_token_sources map.
                    # This handles sources like Hyperliquid FUTURES where perp tokens
                    # have no chain data but were assigned global_tokens by the token-collector.
                    #
                    # Safety: We filter strictly by source_id, so only GTs linked to THIS
                    # source are considered — no cross-source contamination.

                    if chainless_gt_map is None:
                        chainless_gt_map = await _build_chainless_gt_map(source, repo)

                    base_asset = tok.base_asset
                    market_str = tok.market_type.value

                    # FIX-007 blacklist: skip BEFORE any chainless_gt_map lookup.
                    # Prevents re-linking a token that already has a GTS entry but is wrong.
                    if source.name in FIX007_BLACKLIST and base_asset in FIX007_BLACKLIST[source.name]:
                        logger.debug(f"FIX-007: {base_asset} on {source.name} is blacklisted (pre-map), skipping")
                        chainless_skipped += 1
                        continue

                    # Try lookup strategies in order:
                    # 1. Direct symbol match (merged with shared GT: "BTC" → GT "BTC")
                    entry = chainless_gt_map.get(base_asset)

                    # 2. Source-specific FUTURES: "BTC_FUTURES:hyperliquid"
                    if not entry and market_str == MarketType.FUTURES.value:
                        entry = chainless_gt_map.get(f"{base_asset}_FUTURES:{source.name}")

                    # 3. Source-specific SPOT: "BTC:hyperliquid"
                    if not entry and market_str == MarketType.SPOT.value:
                        entry = chainless_gt_map.get(f"{base_asset}:{source.name}")

                    if entry:
                        global_token_id, global_source_id_override = entry
                        chainless_resolved += 1
                    else:
                        # FIX-007: Last-resort fallback — direct GlobalToken symbol lookup.
                        # Handles tokens whose token_on_chains were filtered by FIX-006
                        # (e.g. BTC/ETH/BNB where exchange deposit API returns wallet
                        # addresses as contract, causing mismatch with existing chain records).
                        # Skip if symbol is explicitly blacklisted for this source.
                        if source.name in FIX007_BLACKLIST and base_asset in FIX007_BLACKLIST[source.name]:
                            logger.debug(f"FIX-007: {base_asset} on {source.name} is blacklisted, skipping")
                            chainless_skipped += 1
                            continue

                        # FIX-008 (2026-03-07): Skip chainless fallback if token_collector
                        # detected a contract conflict for this source+symbol.
                        # Means the exchange has a DIFFERENT token with the same symbol
                        # → linking by symbol would produce phantom spreads.
                        try:
                            conflict_key = f"distributor:contract_conflict:{source.name}"
                            is_conflict = await redis.client.sismember(conflict_key, base_asset.upper())
                            if is_conflict:
                                logger.debug(
                                    f"FIX-008: {base_asset} on {source.name} has contract conflict, "
                                    f"skipping chainless fallback"
                                )
                                chainless_skipped += 1
                                continue
                        except Exception as e:
                            logger.warning(f"FIX-008: Redis check failed for {base_asset}/{source.name}: {e}")

                        fix007_entry = await _lookup_gt_by_symbol(base_asset, source, repo)
                        if fix007_entry:
                            global_token_id, global_source_id_override = fix007_entry
                            # Cache in chainless_gt_map for subsequent iterations
                            chainless_gt_map[base_asset] = fix007_entry
                            chainless_resolved += 1
                        else:
                            chainless_skipped += 1
                            continue  # No GT found even via symbol lookup — skip

                if not global_token_id:
                    chainless_skipped += 1
                    continue

                # Get global_source from cache or create
                if global_source_id_override:
                    global_source_id = global_source_id_override
                    # Also cache it to avoid re-creation
                    gts_cache[str(global_token_id)] = type("GTS", (), {"id": global_source_id_override})()
                else:
                    global_source = gts_cache.get(str(global_token_id))
                    if not global_source:
                        global_source = await repo.global_token_sources.get_or_create(global_token_id, source.id)
                        gts_cache[str(global_token_id)] = global_source
                    global_source_id = global_source.id

                token_data = {
                    "id": str(tok.id),
                    "symbol": tok.symbol,
                    "market_type": tok.market_type.value,
                    "global_token_id": str(global_token_id),
                    "global_source_id": str(global_source_id),
                }
                encoded = orjson.dumps(token_data)

                # FIX-008: Always register in source_registry (webapp sourceMap needs this)
                registry_json_tokens.append(encoded)

                # Only add to available_tokens if not already there
                if str(global_token_id) in existing_ids:
                    continue
                new_json_tokens.append(encoded)

            if chainless_resolved:
                logger.info(f"{source.name}/{market_type.value}: {chainless_resolved} chainless tokens resolved via GT map")
            if chainless_skipped:
                logger.info(f"{source.name}/{market_type.value}: {chainless_skipped} chainless tokens skipped (no GT found)")
            if new_json_tokens:
                await redis.client.sadd(redis_key, *new_json_tokens)
                logger.info(f"Added {len(new_json_tokens)} new tokens into Redis for {source.name}/{market_type.value}")

            # FIX-008: Maintain a persistent source_registry for the webapp sourceMap.
            # available_tokens is ephemeral (price collector removes entries after pickup),
            # so the webapp can't build a complete global_source_id→source mapping from it.
            # source_registry is append-only and never cleared — always has the full picture.
            if registry_json_tokens:
                registry_key = f"source_registry:{source.name}:{market_type.value}"
                await redis.client.sadd(registry_key, *registry_json_tokens)
                logger.debug(f"source_registry {source.name}/{market_type.value}: {len(registry_json_tokens)} entries")

        except Exception as e:
            logger.error(f"Error processing {source.name}/{market_type.value}: {e}", exc_info=True)


async def distributor(repo: RequestsRepo, redis: RedisCache, tokens_load_limit: int = 50000):
    while True:
        try:
            sources = await repo.sources.get_all_cex()
            # Process sources sequentially to avoid memory issues
            for source in sources:
                await load_source_tokens_incremental(source, repo, redis, tokens_load_limit)
        except Exception as e:
            logger.error(f"Error in distributor loop: {e}", exc_info=True)

        await asyncio.sleep(60)


async def main():
    database.redis = aioredis.from_url(settings.redis.dsn, encoding="utf-8")
    database.init_database(settings)
    repo = RequestsRepo(sessionmaker=database.async_session)
    redis = RedisCache(database.redis)
    await distributor(repo, redis)


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import time

import orjson

from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.spread_collector.analyzer import ArbitrageAnalyzer
from infrastructure.services.collectors.spread_collector.broadcaster import ArbitrageMessenger
from infrastructure.services.collectors.spread_collector.filters import ArbitrageFilter
from infrastructure.services.collectors.spread_collector.config import SignalLevelType, DEFAULT_PERFORMANCE_CONFIG, DEFAULT_SPOT_CONFIG, PRICE_CORRECTION_MAP
from infrastructure.services.collectors.spread_collector.service import ArbitrageAnalyzerService
from infrastructure.services.collectors.structures.normalizer import normalize_orderbook, normalize_chains, batched
from infrastructure.services.collectors.token_collector.supports import chain_normalizer_instance
from infrastructure.services.common.cache import RedisCache
from loader import TokenLoader
from settings import logger


class ArbitrageAnalyzerManager:
    def __init__(
        self,
        repo: RequestsRepo,
        cache: RedisCache,
        tokens_load_limit: int,
        interval: float = None,
        performance_config=None,
        signal_writer=None,
    ):
        self.repo = repo
        self.cache = cache
        self.tokens_load_limit = tokens_load_limit
        self.signal_writer = signal_writer

        # Use performance config
        self.perf_config = performance_config or DEFAULT_PERFORMANCE_CONFIG
        self.interval = interval if interval is not None else self.perf_config.analysis_interval

        self.loader = TokenLoader(repo, cache, tokens_load_limit)
        self.analyzer_service = ArbitrageAnalyzerService(analyzer=ArbitrageAnalyzer())
        self.messenger = ArbitrageMessenger(chain_normalizer=chain_normalizer_instance, signal_writer=self.signal_writer)
        self.filters = {
            "spot": ArbitrageFilter(is_futures=False, cache=cache),
            "futures": ArbitrageFilter(is_futures=True, cache=cache)
        }

    async def run(self):
        while True:
            start = time.perf_counter()
            try:
                # Load tokens from Redis
                load_start = time.perf_counter()
                token_data = await self.cache.client.lrange("tokens:analyzer", 0, -1)
                if not token_data:
                    logger.error("Tokens queue is empty")
                    await asyncio.sleep(0.01)
                    continue

                tokens = [orjson.loads(t) for t in token_data]
                load_time = time.perf_counter() - load_start

                # Analyze tokens
                analyze_start = time.perf_counter()
                results = []
                for batch in batched(tokens, self.perf_config.batch_size):
                    # Parallel processing of tokens in batch
                    arbitrage_results = await asyncio.gather(*[self._analyze_token_group(t) for t in batch])
                    for r in arbitrage_results:
                        if r:
                            results.extend(r)
                analyze_time = time.perf_counter() - analyze_start

                # Send results (grouped by token_id, non-blocking)
                send_start = time.perf_counter()
                if results:
                    asyncio.create_task(self.messenger.send_results(results))
                send_time = time.perf_counter() - send_start

                total_time = time.perf_counter() - start
                if self.perf_config.enable_profiling:
                    logger.info(
                        f"Analyzed {len(tokens)} tokens in {total_time:.2f}s (load: {load_time:.2f}s, "
                        f"analyze: {analyze_time:.2f}s, send: {send_time:.2f}s), found {len(results)} results"
                    )
                else:
                    logger.info(
                        f"Analyzed {len(tokens)} tokens in {total_time:.2f}s, found {len(results)} results"
                    )

                elapsed = time.perf_counter() - start
                if self.interval > elapsed:
                    await asyncio.sleep(self.interval - elapsed)

            except Exception as e:
                logger.error(f"Failed to analyze arbitrage results: {e}", exc_info=True)

    async def _analyze_token_group(self, token: dict):
        # Parse orderbooks
        parse_start = time.perf_counter()
        spot_sources, futures_sources, dex_sources, tickers_data = {}, {}, {}, {"spot": {}, "futures": {}, "dex": {}}
        for price in token["prices"]:
            orderbook = price.get("orderbook")
            if not orderbook:
                continue

            price = normalize_orderbook(price["token"]["base_asset"], price)

            source = price["source"]
            source_name = source["name"]
            source_type = source["type"].lower()
            market_type = price["market_type"].lower()
            item = {
                "orderbook": price["orderbook"],
                "contract_size": price["token"].get("contract_size") or 1,
                "source_name": source_name,
                "base_asset": price["token"]["base_asset"],
                "quote_asset": price["token"]["quote_asset"],
                "symbol": price["token"]["symbol"],
                "updated_at": price.get("updated_at") or price.get("created_at"),
                "get_at": price.get("get_at"),
                "chains": price.get("chains", [])
            }
            # Apply price correction for tokens with known artificial price offsets
            # (e.g. GATE prices TQQQX spot at 2x real value)
            base_symbol = price["token"]["base_asset"].upper()
            token_corrections = PRICE_CORRECTION_MAP.get(base_symbol) or PRICE_CORRECTION_MAP.get(token.get("symbol", "").upper())
            if token_corrections:
                correction_key = f"{source_name.lower()}_{market_type.lower()}"
                factor = token_corrections.get(correction_key)
                if factor and factor != 1.0:
                    ob = item["orderbook"]
                    ob["asks"] = [[p * factor, v] for p, v in ob["asks"]]
                    ob["bids"] = [[p * factor, v] for p, v in ob["bids"]]
                    logger.info(
                        f"Price correction applied: {base_symbol} {correction_key} x{factor} "
                        f"(ask[0]={ob['asks'][0][0]:.6g} bids[0]={ob['bids'][0][0]:.6g})"
                        if ob["asks"] and ob["bids"] else
                        f"Price correction applied: {base_symbol} {correction_key} x{factor}"
                    )

            if source_type == "dex":
                # Skip stale DEX prices to avoid phantom delays
                get_at = item.get("get_at")
                if get_at:
                    age_s = (time.time() * 1000 - get_at) / 1000
                    if age_s > DEFAULT_SPOT_CONFIG.max_dex_price_age:
                        continue
                dex_sources[source_name] = item
                tickers_data["dex"][source_name] = item
            elif source_type == "cex":
                if market_type == "spot":
                    spot_sources[source_name] = item
                    tickers_data["spot"][source_name] = item
                elif market_type == "futures":
                    futures_sources[source_name] = item
                    tickers_data["futures"][source_name] = item
        parse_time = time.perf_counter() - parse_start

        # Analyze arbitrage opportunities
        analyze_start = time.perf_counter()
        results = []
        results += self.analyzer_service.analyze_same_type("spot", spot_sources, depth_analyze=True)
        results += self.analyzer_service.analyze_same_type("futures", futures_sources)
        results += self.analyzer_service.analyze_cross_type("spot", spot_sources, "futures", futures_sources)
        results += self.analyzer_service.analyze_cross_type("spot", spot_sources, "dex", dex_sources,
                                                            depth_analyze=True)
        results += self.analyzer_service.analyze_cross_type("futures", futures_sources, "dex", dex_sources)
        analyze_time = time.perf_counter() - analyze_start

        # Process and filter results
        process_start = time.perf_counter()
        processed_results = []
        for r in results:
            r["tickers_data"] = tickers_data
            # if r.get("spread_percent", 0) > 10000:
            #     continue
            pr = await self.process_result(r, tickers_data, token, self.filters)
            if pr:
                processed_results.append(pr)
        process_time = time.perf_counter() - process_start

        # Token-level cache filter: decide once per token whether to broadcast ALL its directions.
        # This guarantees the grouped signal is always sent as a single atomic unit,
        # preventing split messages that break downstream cache/spam logic.
        cache_start = time.perf_counter()
        filtered_results = []
        if processed_results:
            # Determine filter type by majority: if any result is futures, use futures filter
            has_futures = any(
                "futures" in r["buy_from"] or "futures" in r["sell_to"]
                for r in processed_results
            )
            result_type = "futures" if has_futures else "spot"
            token_id = token["id"]
            should_broadcast = await self.filters[result_type].should_broadcast_token_group(
                token_id, processed_results
            )
            if should_broadcast:
                filtered_results = processed_results
        cache_time = time.perf_counter() - cache_start

        # Log detailed timing if profiling is enabled
        if self.perf_config.enable_profiling:
            total_time = parse_time + analyze_time + process_time + cache_time
            if results and total_time > 0.1:  # Log only if processing took more than 100ms
                logger.debug(
                    f"Token {token.get('symbol', 'unknown')} timing: "
                    f"parse={parse_time*1000:.1f}ms, analyze={analyze_time*1000:.1f}ms, "
                    f"process={process_time*1000:.1f}ms, cache={cache_time*1000:.1f}ms, "
                    f"total={total_time*1000:.1f}ms"
                )

        return filtered_results

    @staticmethod
    def prepare_result_chains(seller_chains, buyer_chains):
        """Prepare chains for result with deduplication by (chain_name, contract_address, token_source_name)."""
        seen = set()
        all_chains = []
        for c in seller_chains + buyer_chains:
            chain_name = (c.get("chain_name") or "").lower()
            contract = (c.get("contract_address") or "").lower()
            source = (c.get("token_source_name") or "").lower()
            key = (chain_name, contract, source)
            if key in seen:
                continue
            seen.add(key)
            all_chains.append({
                "chain_name": c.get("chain_name"),
                "contract_address": c.get("contract_address"),
                "withdrawal_fee": c.get("withdrawal_fee"),
                "withdrawal_fee_in_usdt": c.get("withdrawal_fee_in_usdt"),
                "cex_withdrawal_fee": c.get("cex_withdrawal_fee"),
                "cex_deposit_fee": c.get("cex_deposit_fee"),
                "total_tax_fee_in_token": c.get("total_tax_fee_in_token"),
                "deposit_enabled": c.get("deposit_enabled"),
                "withdraw_enabled": c.get("withdraw_enabled"),
                "token_source_name": c.get("token_source_name"),
            })
        return all_chains

    @staticmethod
    def compute_realizable_fee(seller_chains, buyer_chains):
        """
        Compute realizable withdrawal fee in USDT.

        Uses seller's withdrawal_fee_in_usdt (not total_tax_fee) because:
        - Seller pays withdrawal fee when sending from their exchange
        - Use withdrawal_fee_in_usdt (already converted to USDT)
        - Choose minimum fee among compatible chains
        """
        fees = []

        # Find compatible chains (same chain, both enabled)
        for s in seller_chains:
            for b in buyer_chains:
                if s["chain_name"].lower() == b["chain_name"].lower():
                    if s.get("withdraw_enabled") and b.get("deposit_enabled"):
                        # Use seller's withdrawal fee in USDT
                        fee_usdt = s.get("withdrawal_fee_in_usdt")
                        if fee_usdt is not None:
                            fees.append(fee_usdt)

        # Fallback: if no compatible chains with both enabled, check any common chain
        if not fees:
            seller_set = {s["chain_name"].lower(): s for s in seller_chains}
            buyer_set = {b["chain_name"].lower(): b for b in buyer_chains}
            intersect = set(seller_set.keys()) & set(buyer_set.keys())

            for ch in intersect:
                fee_usdt = seller_set[ch].get("withdrawal_fee_in_usdt")
                if fee_usdt is not None:
                    fees.append(fee_usdt)

        # Return minimum fee (best chain option)
        return min(fees) if fees else 0

    async def process_result(self, r, tickers_data, token, filters):
        seller_info = tickers_data.get(r["buy_from"].split("_", 1)[0], {}).get(r["buy_from"].split("_", 1)[1], {})
        buyer_info = tickers_data.get(r["sell_to"].split("_", 1)[0], {}).get(r["sell_to"].split("_", 1)[1], {})

        seller_chains = normalize_chains(seller_info.get("chains", []), r["sell_price"], r["volume"])
        buyer_chains = normalize_chains(buyer_info.get("chains", []), r["sell_price"], r["volume"])

        r["chains"] = self.prepare_result_chains(seller_chains, buyer_chains)
        min_fee = self.compute_realizable_fee(seller_chains, buyer_chains)

        r["profit_before_fee"] = r["profit"]
        r["applied_fee"] = min_fee
        r["profit"] -= min_fee

        result_type = "futures" if "futures" in r["buy_from"] or "futures" in r["sell_to"] else "spot"
        r["level"] = filters[result_type].get_level(r["spread_percent"], r["profit"], symbol=token["symbol"]).value
        r["symbol"] = token["symbol"]
        r["global_token_id"] = token["id"]
        r["analyzed_at"] = int(time.time() * 1000)  # timestamp when result exits analyzer

        if r["level"] == SignalLevelType.NONE:
            return None

        return r

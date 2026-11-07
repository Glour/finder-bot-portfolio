import asyncio
import json
from typing import Coroutine
from uuid import UUID

from infrastructure.database.models.base import SourceType, MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.interfaces import SupportsTokenParsing
from infrastructure.services.collectors.structures.maps import dexscreener_chains_list
from infrastructure.services.collectors.structures.normalizer import safe_float, normalize_dex_pair_item, \
    ChainNormalizer
from infrastructure.services.collectors.structures.schemas import CachedResult
from infrastructure.services.collectors.token_collector.base import BaseTokenParser
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.common.request import make_request
from settings import logger


class DexScreenerTokenParser(BaseTokenParser, SupportsTokenParsing):
    def __init__(self, repo: RequestsRepo, cache: RedisCache, chain_normalizer: ChainNormalizer,
                 tokens_load_limit: int = 50000):
        super().__init__(
            cache=cache,
            chain_normalizer=chain_normalizer,
            repo=repo,
            source_name="dexscreener",
            source_type=SourceType.DEX,
        )
        self.repo = repo
        self.cache = cache
        self.base_host = "api.dexscreener.com"
        self.chains: list[str] = dexscreener_chains_list
        self.token_addresses: dict[str, list[str]] = {}
        self.tokens_load_limit = tokens_load_limit

    async def _collect_token_addresses(self) -> None:
        cache_key = "dexscreener:token_addresses_by_chain"
        cached = await self.cache.get(cache_key)
        if cached:
            self.token_addresses = json.loads(cached)
            if len(self.token_addresses) > 0:
                return

        addresses = await self.repo.token_on_chain.get_all_contract_addresses_grouped()
        normalized_chains_set = set(dexscreener_chains_list)
        filtered_addresses = {
            chain: addr_list
            for chain, addr_list in addresses.items()
            if self.chain_normalizer.normalize_from_cex(chain) in normalized_chains_set
        }

        self.token_addresses = filtered_addresses

        await self.cache.set(
            cache_key,
            json.dumps(filtered_addresses, separators=(",", ":"), ensure_ascii=False),
            timeout_secs=300
        )

    async def fetch_tokens(self) -> list[dict]:
        tasks: list[Coroutine[None, None, list[dict]]] = []

        for chain_id, addresses in self.token_addresses.items():
            filtered = self._filter_addresses(addresses)
            if not filtered:
                continue
            chunks = self._get_chunks(filtered, size=30)
            tasks.extend([self._fetch_chunk(chain_id, chunk) for chunk in chunks])

        results = await asyncio.gather(*tasks, return_exceptions=False)
        return [item for sublist in results for item in sublist if item]

    async def _fetch_chunk(self, chain_id: str, token_chunk: list[str]) -> list[dict]:
        if not token_chunk:
            return []

        joined = ",".join(token_chunk)
        route = f"/tokens/v1/{chain_id}/{joined}"
        data = await make_request("GET", route, host=self.base_host, source_name=self.source_name)
        if isinstance(data, list):
            return data
        return []

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        await self._collect_token_addresses()
        data = await self.fetch_tokens()
        return await super().fetch_and_check_internal("dexscreener:token_map", data)

    async def save_to_db(self, token_map: list[dict]):
        tokens: dict[str, dict] = {}
        dex_pairs: list[dict] = []
        for pair in token_map:
            pair_address = pair.get("pairAddress")
            base = pair.get("baseToken", {})
            quote = pair.get("quoteToken", {})
            addr = base.get("address", "")  # preserve original case
            chain = pair.get("chainId")
            price_usd = pair.get("priceUsd")
            liquidity_usd = pair.get("liquidity", {}).get("usd", 0)

            if not base or not pair_address:
                continue

            chain_name = self.chain_normalizer.normalize_from_dex(chain)
            if not chain_name:
                logger.warn(f"Unknown chain from dex: {chain}")
                continue

            # Use lowercase key for deduplication, but preserve original address
            key = f"{chain_name}:{addr.lower()}"

            if key not in tokens:
                tokens[key] = {
                    "symbol": base.get("symbol"),
                    "base_asset": base.get("symbol"),
                    "quote_asset": quote.get("symbol"),
                    "market_type": MarketType.SPOT,
                    "is_active": True,
                    "source_id": self.source.id,
                    "price_usd": price_usd,
                    "liquidity_usd": liquidity_usd,
                    "chains": [{
                        "chain_name": chain_name,
                        "contract_address": addr,
                        "deposit_enabled": True,
                        "withdraw_enabled": True
                    }]
                }

            pair["chain_name"] = chain_name
            pair["base_address"] = base.get("address", "")  # preserve original case
            pair["quote_address"] = quote.get("address", "")  # preserve original case
            pair["price_usd"] = price_usd
            pair["liquidity_usd"] = liquidity_usd
            dex_pairs.append(pair)

        token_key_map = await self.save_token_data_dex(list(tokens.values()))
        await self._save_dex_pairs(dex_pairs)
        await self._save_token_prices(token_key_map)

        return token_key_map

    async def _save_token_prices(self, token_key_map: list[dict[str, dict]]) -> list[UUID] | None:
        prices: list[dict] = []

        for token in token_key_map:
            price = safe_float(token.get("price_usd"))
            liquidity_usd = safe_float(token.get("liquidity_usd", 0))
            volume = liquidity_usd / 100 / 2 / price if price else 0

            orderbook = {
                "asks": [[price, volume]],
                "bids": [[price, volume]]
            }

            prices.append({
                "price_usd": price,
                "orderbook": orderbook,
                "market_type": MarketType.SPOT,
                "token_id": token["token_id"],
                "global_token_id": token["global_token_id"],
                "global_token_source_id": token["global_token_source_id"],
            })

        return await self.repo.token_prices.bulk_upsert(prices)

    async def _save_dex_pairs(self, items: list[dict]):
        if not items:
            return

        chain_names = list({p["chain_name"] for p in items})
        chain_map = await self.repo.chains.get_or_create_bulk_by_names(chain_names)
        token_map = await self.repo.token_on_chain.get_token_on_chain_id_map_by_chain_and_address()

        rows = []

        for item in items:
            normalized = normalize_dex_pair_item(item)
            if not normalized:
                continue

            chain_obj = chain_map.get(normalized["chain_name"])
            if not chain_obj:
                continue

            base_pair = token_map.get((str(chain_obj.id), normalized["base_address"]))
            quote_pair = token_map.get((str(chain_obj.id), normalized["quote_address"]))

            if not base_pair or not quote_pair:
                continue

            base_on_chain_id, base_token_id = base_pair
            quote_on_chain_id, quote_token_id = quote_pair

            normalized["chain_id"] = chain_obj.id
            normalized["source_id"] = self.source.id
            normalized["token_base_id"] = base_token_id
            normalized["token_quote_id"] = quote_token_id

            rows.append(normalized)

        await self.repo.dex_pairs.bulk_upsert_pairs(rows)

    @staticmethod
    def _filter_addresses(addresses: list[str]) -> list[str]:
        return [addr.strip() for addr in addresses if addr and addr.strip()]

    @staticmethod
    def _get_chunks(items: list[str], size: int) -> list[list[str]]:
        return [items[i:i + size] for i in range(0, len(items), size)]

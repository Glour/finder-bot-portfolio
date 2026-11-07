import asyncio
import hashlib
import hmac
import time
import urllib.parse

from infrastructure.database.models.base import MarketType
from infrastructure.database.models.base import SourceType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.interfaces import SupportsTokenParsing
from infrastructure.services.collectors.structures.normalizer import safe_float, parse_multiplier, \
    ChainNormalizer, parse_tax_from_tip
from infrastructure.services.collectors.structures.schemas import CachedResult
from infrastructure.services.collectors.token_collector.base import BaseTokenParser
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.common.request import make_request
from settings import settings


class MexcSignedRequestMixin:
    base_host = "api.mexc.com"
    futures_host = "contract.mexc.com"

    @staticmethod
    def prepare_signed_params(params: dict[str, str | int]) -> dict[str, str | int]:
        params["timestamp"] = int(time.time() * 1000)
        params.setdefault("recvWindow", 10000)
        qs = urllib.parse.urlencode(params)
        signature = hmac.new(settings.auth.mexc_secret_key.encode(), qs.encode(), hashlib.sha256).hexdigest()
        params["signature"] = signature
        return params


class MexcTokenParser(MexcSignedRequestMixin, BaseTokenParser, SupportsTokenParsing):
    def __init__(self, repo: RequestsRepo, cache: RedisCache, chain_normalizer: ChainNormalizer,
                 tokens_load_limit: int = 50000):
        super().__init__(
            cache=cache,
            chain_normalizer=chain_normalizer,
            repo=repo,
            source_name="mexc",
            source_type=SourceType.CEX,
        )
        self.repo = repo
        self.tokens_load_limit = tokens_load_limit

    async def fetch_tokens(self) -> list[dict]:
        tasks = [
            self.fetch_spot_data(),
            self.fetch_futures_data(),
            self.fetch_spot_metadata()
        ]
        spot_pairs, futures_contracts, spot_meta = await asyncio.gather(*tasks, return_exceptions=False)
        if not all([spot_pairs, futures_contracts, spot_meta]):
            return []

        return self.build_token_map(
            spot_meta or [],
            spot_pairs or [],
            futures_contracts or [],
        )

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        data = await self.fetch_tokens()
        if not data:
            return CachedResult([], False)
        return await super().fetch_and_check_internal(f"{self.source_name}:token_map", data)

    async def fetch_spot_metadata(self) -> list[dict] | None:
        route = "api/v3/capital/config/getall"
        params = self.prepare_signed_params({})
        data = await make_request(
            method="GET",
            route=route,
            host=self.base_host,
            params=params,
            headers={"X-MEXC-APIKEY": settings.auth.mexc_access_key},
            use_proxy=False,
            source_name=self.source_name
        )
        return data if data and data != "invalid_symbol" else []

    async def fetch_spot_data(self) -> list[dict] | None:
        route = "api/v3/exchangeInfo"
        data = await make_request("GET", route, host=self.base_host, use_proxy=False, source_name=self.source_name)
        return data.get("symbols", []) if data else []

    async def fetch_futures_data(self) -> list[dict] | None:
        route = "api/v1/contract/detail"
        data = await make_request("GET", route, host=self.futures_host, use_proxy=False, source_name=self.source_name)
        return data.get("data", []) if data else []

    def build_token_map(
            self,
            spot_meta: list[dict],
            spot_data: list[dict],
            futures_data: list[dict]
    ) -> list[dict]:
        result: list[dict] = []
        meta_map = {m["coin"]: m for m in spot_meta}
        spot_map = {}

        # собираем spot токены и их сети
        for token in spot_data:
            symbol = token["symbol"]
            base_asset = token["baseAsset"]
            quote_asset = token["quoteAsset"]

            if quote_asset != "USDT":
                continue

            multiplier, clear_ticker = parse_multiplier(base_asset)
            meta = meta_map.get(clear_ticker)
            networks = meta.get("networkList") if meta else []

            chains = []
            for net in networks:
                chain_name = net["network"]
                if chain_name.lower() == "none":
                    continue

                chains.append({
                    "chain_name": chain_name,
                    "contract_address": net["contract"] or "",  # preserve original case
                    "deposit_enabled": net.get("depositEnable", False),
                    "min_deposit_amount": None,
                    "deposit_confirm": net.get("minConfirm"),
                    "deposit_tax": safe_float(parse_tax_from_tip(net.get("depositTips", ""))),
                    "withdraw_enabled": net.get("withdrawEnable", False),
                    "min_withdraw_amount": safe_float(net.get("withdrawMin", 0)),
                    "withdrawal_fee": safe_float(net.get("withdrawFee", 0)),
                    "withdraw_tax": safe_float(parse_tax_from_tip(net.get("withdrawTips", ""))),
                })

            token_entry = {
                "symbol": symbol,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "base_coin_name": clear_ticker,
                "contract_size": None,
                "market_type": MarketType.SPOT,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains
            }

            result.append(token_entry)
            spot_map[clear_ticker] = chains

        # собираем futures токены, копируя chains со спота
        for contract in futures_data:
            symbol = contract["symbol"]
            base_asset = contract["baseCoin"]
            quote_asset = contract["quoteCoin"]
            base_coin_name = contract["baseCoinName"]

            if quote_asset != "USDT":
                continue

            # For synthetic tokens (STOCK, SOL, etc.), use baseCoin instead of baseCoinName
            # to prevent incorrect merging with different tokens
            # Example: NOWSTOCK (synthetic) != NOW (spot token)
            if base_asset.endswith("STOCK") or base_asset.endswith("SOL") or base_asset != base_coin_name:
                # Use baseCoin as-is for synthetic/wrapper tokens
                multiplier, clear_ticker = parse_multiplier(base_asset)
            else:
                # Use baseCoinName for regular tokens
                multiplier, clear_ticker = parse_multiplier(base_coin_name)

            if clear_ticker == "ALT":
                chains = spot_map.get("ALTLAYER", [])
                clear_ticker = "ALTLAYER"
            else:
                # When baseCoinName differs from baseCoin, prefer baseCoinName for chain lookup
                # because it maps to the actual spot token (e.g. FUN futures → FUNTOKEN spot,
                # not FUN spot which may have wrong contract address like SPORTFUN)
                if base_coin_name != base_asset and base_coin_name in spot_map:
                    chains = spot_map.get(base_coin_name, [])
                else:
                    chains = spot_map.get(clear_ticker, [])

            token_entry = {
                "symbol": symbol,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "base_coin_name": clear_ticker,
                "contract_size": safe_float(contract["contractSize"]),
                "market_type": MarketType.FUTURES,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains
            }

            result.append(token_entry)

        return result

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

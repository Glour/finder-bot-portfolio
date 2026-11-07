import asyncio

from infrastructure.database.models.base import MarketType
from infrastructure.database.models.base import SourceType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.interfaces import SupportsTokenParsing
from infrastructure.services.collectors.structures.normalizer import ChainNormalizer, safe_float, parse_multiplier
from infrastructure.services.collectors.structures.schemas import CachedResult
from infrastructure.services.collectors.token_collector.base import BaseTokenParser
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.common.request import make_request


class XtTokenParser(BaseTokenParser, SupportsTokenParsing):
    base_host_spot = "sapi.xt.com"
    base_host_futures = "fapi.xt.com"

    def __init__(
            self,
            repo: RequestsRepo,
            cache: RedisCache,
            chain_normalizer: ChainNormalizer,
            tokens_load_limit: int = 50000
    ):
        super().__init__(
            cache=cache,
            chain_normalizer=chain_normalizer,
            repo=repo,
            source_name="xt",
            source_type=SourceType.CEX,
        )
        self.repo = repo
        self.tokens_load_limit = tokens_load_limit

    async def fetch_tokens(self) -> list[dict]:
        tasks = [
            self.fetch_spot_data(),
            self.fetch_futures_data(),
        ]
        spot_pairs, futures_contracts = await asyncio.gather(*tasks, return_exceptions=False)
        if not all([spot_pairs, futures_contracts]):
            return []

        return self.build_token_map(
            spot_pairs or [],
            futures_contracts or [],
        )

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        data = await self.fetch_tokens()
        if not data:
            return CachedResult([], False)
        return await super().fetch_and_check_internal(f"{self.source_name}:token_map", data)

    async def fetch_spot_data(self) -> list[dict] | None:
        route = "v4/public/wallet/support/currency"
        data = await make_request("GET", route, host=self.base_host_spot, use_proxy=True, source_name=self.source_name)
        return data.get("result", []) if data else []

    async def fetch_futures_data(self) -> list[dict] | None:
        route = "future/market/v3/public/symbol/list"
        data = await make_request("GET", route, host=self.base_host_futures, use_proxy=True, source_name=self.source_name)
        return data.get("result", {}).get("symbols", []) if data else []

    def build_token_map(
            self,
            spot_data: list[dict],
            futures_data: list[dict]
    ) -> list[dict]:
        result: list[dict] = []
        spot_map = {}

        # собираем spot токены и их сети
        for token in spot_data:
            base_asset = token["currency"].upper()
            quote_asset = "USDT"
            symbol = f"{base_asset}_{quote_asset}"

            if quote_asset != "USDT":
                continue

            networks = token.get("supportChains", [])

            chains = []
            for net in networks:
                chain_name = net.get("chain")
                contract = net.get("contract", "")
                chains.append({
                    "chain_name": chain_name,
                    "deposit_enabled": net.get("depositEnabled", False),
                    "withdraw_enabled": net.get("withdrawEnabled", False),
                    "contract_address": contract or "",  # preserve original case
                    "min_deposit_amount": safe_float(net.get("depositMinAmount", 0)),
                    "deposit_tax": safe_float(net.get("depositFeeRate", 0)),
                    "min_withdraw_amount": safe_float(net.get("withdrawMinAmount", 0)),
                    "withdrawal_fee": safe_float(net.get("withdrawFeeAmount", 0)),
                    "withdraw_tax": 0,
                })

            # Parse multiplier for spot tokens too (e.g. 1000000MOG -> MOG)
            multiplier_spot, clear_ticker_spot = parse_multiplier(base_asset)
            spot_base_coin_name = clear_ticker_spot if multiplier_spot > 1 else base_asset

            token_entry = {
                "symbol": symbol,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "base_coin_name": spot_base_coin_name,
                "contract_size": None,
                "market_type": MarketType.SPOT,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains
            }

            result.append(token_entry)
            spot_map[base_asset] = chains
            if multiplier_spot > 1:
                spot_map[clear_ticker_spot] = chains  # Also map clear ticker for futures lookup

        # собираем futures токены, копируя chains со спота
        for contract in futures_data:
            symbol = contract["symbol"].upper()
            base_asset = contract.get("baseCoin", "").upper()
            quote_asset = contract.get("quoteCoin", "").upper()

            if not contract.get("isDisplay"):
                continue

            if quote_asset != "USDT":
                continue

            # spotCoin tells us which spot token the futures contract maps to
            # Use it for chain lookup when it differs from baseCoin
            # e.g. fun_usdt futures has baseCoin="fun", spotCoin="funtoken"
            spot_coin = contract.get("spotCoin", "").upper()

            if base_asset == "YZY":
                symbol = "YZYMONEY_USDT"
                base_asset = "YZYMONEY"
                base_coin_name = "YZYMONEY"
                chains = spot_map.get("YZYMONEY", [])
            else:
                # Parse multiplier to get clear ticker (e.g. 1000MOG -> MOG)
                # This ensures futures tokens match their spot counterparts
                multiplier, clear_ticker = parse_multiplier(base_asset)
                base_coin_name = clear_ticker if multiplier > 1 else base_asset
                # Prefer spotCoin for chain lookup when it differs from baseCoin
                # This prevents mismatched contract addresses (e.g. FUN vs SPORTFUN)
                if spot_coin and spot_coin != base_asset:
                    chains = spot_map.get(spot_coin, spot_map.get(base_asset, []))
                else:
                    chains = spot_map.get(base_asset, [])

            token_entry = {
                "symbol": symbol,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "base_coin_name": base_coin_name,
                "contract_size": safe_float(contract.get("contractSize", 1)),
                "market_type": MarketType.FUTURES,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains
            }

            result.append(token_entry)

        return result

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

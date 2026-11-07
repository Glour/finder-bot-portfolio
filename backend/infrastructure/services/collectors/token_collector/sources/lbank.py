import asyncio
import time
import uuid

from infrastructure.database.models.base import MarketType
from infrastructure.database.models.base import SourceType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.interfaces import SupportsTokenParsing
from infrastructure.services.collectors.structures.normalizer import ChainNormalizer, safe_float
from infrastructure.services.collectors.structures.schemas import CachedResult
from infrastructure.services.collectors.token_collector.base import BaseTokenParser
from infrastructure.services.collectors.token_collector.supports import lbank_contract_addresses
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.common.request import make_request


class LbankTokenParser(BaseTokenParser, SupportsTokenParsing):
    base_host_spot = "api.lbkex.com"
    base_host_futures = "lbkperp.lbank.com"

    def __init__(
            self,
            repo: RequestsRepo,
            cache: RedisCache,
            chain_normalizer: ChainNormalizer,
    ):
        super().__init__(
            cache=cache,
            chain_normalizer=chain_normalizer,
            repo=repo,
            source_name="lbank",
            source_type=SourceType.CEX,
        )
        self.repo = repo
        self.contract_addresses: dict[str, list[str]] = lbank_contract_addresses
        # Limit concurrent requests to LBank API to prevent ConnectTimeout
        self._semaphore = asyncio.Semaphore(15)

    async def fetch_tokens(self) -> list[dict]:
        tasks = [
            self.fetch_spot_pairs(),
            self.fetch_futures_data(),
        ]
        spot_pairs, futures_contracts = await asyncio.gather(*tasks, return_exceptions=False)
        if not all([spot_pairs, futures_contracts]):
            return []

        return await self.build_token_map(
            spot_pairs or [],
            futures_contracts or []
        )

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        data = await self.fetch_tokens()
        if not data:
            return CachedResult([], False)
        return await super().fetch_and_check_internal(f"{self.source_name}:token_map", data)

    async def fetch_spot_pairs(self) -> list[dict]:
        route = "/v2/accuracy.do"
        data = await make_request("GET", route, host=self.base_host_spot, use_proxy=True, source_name=self.source_name)
        return data.get("data", []) if data else []

    async def fetch_spot_meta(self, asset: str) -> list[dict] | None:
        route = "v2/assetConfigs.do"
        async with self._semaphore:
            data = await make_request("GET", route, params={"assetCode": asset}, host=self.base_host_spot, use_proxy=True, source_name=self.source_name)
            return data.get("data", []) if data else []

    async def fetch_futures_data(self) -> list[dict] | None:
        route = "cfd/openApi/v1/pub/instrument"
        headers = {
            "Content-Type": "application/json",
            "timestamp": str(int(time.time() * 1000)),
            "signature_method": "HmacSHA256",
            "echostr": uuid.uuid4().hex[:32],
        }
        params = {
            "productGroup": "SwapU"
        }

        data = await make_request(
            "GET",
            route,
            host=self.base_host_futures,
            headers=headers,
            params=params,
            use_proxy=True,
            source_name=self.source_name
        )
        return data.get("data", []) if data else []

    async def build_token_map(
            self,
            spot_pairs: list[dict],
            futures_data: list[dict]
    ) -> list[dict]:
        result: list[dict] = []
        spot_map: dict[str, list[dict]] = {}

        # собираем уникальные baseAsset из пар
        assets = set()
        for item in spot_pairs:
            base, quote = item["symbol"].upper().split("_")
            if self.check_valid_symbol(base, quote):
                assets.add(base)

        tasks = {
            asset: asyncio.create_task(self.fetch_spot_meta(asset.lower()))
            for asset in assets
        }
        responses = await asyncio.gather(*tasks.values(), return_exceptions=True)

        # склеиваем результат
        for asset, metas in zip(tasks.keys(), responses):
            if isinstance(metas, Exception):
                spot_map.setdefault(asset.upper(), [])
                continue

            if not metas:
                # сохраняем токен, но без сетей
                spot_map.setdefault(asset.upper(), [])
                continue

            for meta in metas:
                chain_name = (meta.get("chainName") or "").lower()
                fee_info = meta.get("assetFee", {})

                if chain_name:
                    address_data = (
                        self.contract_addresses
                        .get(asset.upper(), {})
                        .get(chain_name, {})
                    )

                    chain_entry = {
                        "chain_name": chain_name,
                        "contract_address": address_data.get("contractInfo", ""),  # preserve original case
                        "deposit_enabled": meta.get("canDeposit", False),
                        "min_deposit_amount": safe_float(fee_info.get("minDepositAmt")),
                        "deposit_confirm": safe_float(address_data.get("confirmation")),
                        "withdraw_enabled": meta.get("canDraw", False),
                        "min_withdraw_amount": safe_float(fee_info.get("minAmt")),
                        "withdrawal_fee": safe_float(fee_info.get("feeAmt")),
                    }

                    spot_map.setdefault(asset.upper(), []).append(chain_entry)
                else:
                    spot_map.setdefault(asset.upper(), [])
        # собираем токены спота
        for asset, chains in spot_map.items():
            if asset.upper() == "BTTC":
                base_coin_name = "BTTC2"
            else:
                base_coin_name = asset

            token_entry = {
                "symbol": f"{asset}_USDT",
                "base_asset": asset,
                "quote_asset": "USDT",
                "base_coin_name": base_coin_name,
                "contract_size": None,
                "market_type": MarketType.SPOT,
                "is_active": True,
                "source_id": str(self.source.id),
                "chains": chains,
            }
            result.append(token_entry)

        # добавляем фьючерсы
        for contract in futures_data or []:
            base_asset = contract["baseCurrency"]
            quote_asset = contract["clearCurrency"]

            if not self.check_valid_symbol(base_asset, quote_asset):
                continue

            chains = spot_map.get(base_asset.upper(), [])
            token_entry = {
                "symbol": contract["symbol"],
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "base_coin_name": base_asset,
                "contract_size": 1,
                "market_type": MarketType.FUTURES,
                "is_active": True,
                "source_id": str(self.source.id),
                "chains": chains,
            }
            result.append(token_entry)

        return result

    @staticmethod
    def check_valid_symbol(base_asset: str, quote_asset: str) -> bool:
        if base_asset[-2:] in ["1S", "1L", "2S", "2L", "3S", "3L", "4S", "4L", "5S", "5L"]:
            return False

        if quote_asset != "USDT":
            return False

        if "TEST" in base_asset and base_asset != "TEST":
            return False

        return True

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

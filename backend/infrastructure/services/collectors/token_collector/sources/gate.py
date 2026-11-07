import asyncio
import httpx

from infrastructure.database.models.base import MarketType
from infrastructure.database.models.base import SourceType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.interfaces import SupportsTokenParsing
from infrastructure.services.collectors.structures.normalizer import ChainNormalizer, safe_float
from infrastructure.services.collectors.structures.schemas import CachedResult
from infrastructure.services.collectors.token_collector.base import BaseTokenParser
from infrastructure.services.collectors.token_collector.supports import gate_chain_fee_info
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.common.request import make_request


class GateTokenParser(BaseTokenParser, SupportsTokenParsing):
    base_host = "api.gateio.ws/api/v4"

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
            source_name="gate",
            source_type=SourceType.CEX,
        )
        self.repo = repo
        self.tokens_load_limit = tokens_load_limit

    async def fetch_tokens(self) -> list[dict]:
        # Fetch sequentially with delays to avoid rate limiting
        spot_meta = await self.fetch_spot_metadata()
        await asyncio.sleep(0.5)  # 500ms delay between requests

        spot_pairs = await self.fetch_spot_data()
        await asyncio.sleep(0.5)

        futures_contracts = await self.fetch_futures_data()

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

    async def _make_gate_request(self, route: str) -> list[dict] | None:
        """Make request to Gate.io with fresh client to avoid 403 errors."""
        url = f"https://{self.base_host}/{route}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
                return response.json()
            except Exception:
                return None

    async def fetch_spot_metadata(self) -> list[dict] | None:
        return await self._make_gate_request("spot/currencies")

    async def fetch_spot_data(self) -> list[dict] | None:
        return await self._make_gate_request("spot/currency_pairs")

    async def fetch_futures_data(self) -> list[dict] | None:
        return await self._make_gate_request("futures/usdt/contracts")

    def build_token_map(
            self,
            spot_meta: list[dict],
            spot_data: list[dict],
            futures_data: list[dict]
    ) -> list[dict]:
        result: list[dict] = []
        meta_map = {m["currency"]: m for m in spot_meta}
        gate_fee_map = {}
        spot_map = {}

        for entry in gate_chain_fee_info:
            symbol = entry["symbol"]
            chain = entry["chain"]
            gate_fee_map[(symbol, chain.lower())] = entry

        # собираем spot токены и их сети
        for token in spot_data:
            symbol = token["id"]
            base_asset = token["base"]
            quote_asset = token["quote"]

            if quote_asset != "USDT":
                continue

            meta = meta_map.get(base_asset)
            networks = meta.get("chains") if meta else []

            if meta.get("delisted") or meta.get("trade_disabled"):
                continue

            if base_asset.endswith(("3S", "3L", "5S", "5L")):
                continue

            chains = []
            for net in networks:
                chain_name = net.get("name")
                fee_entry = gate_fee_map.get((base_asset, chain_name.lower()), {})

                chains.append({
                    "chain_name": chain_name,
                    "contract_address": net.get("addr", ""),  # preserve original case
                    "withdraw_enabled": not net.get("withdraw_disabled", True),
                    "deposit_enabled": not net.get("deposit_disabled", True),
                    "min_deposit_amount": safe_float(fee_entry.get("deposit_amount_mini", 0)),
                    "deposit_tax": safe_float(fee_entry.get("deposit_txfee", 0)),
                    "min_withdraw_amount": safe_float(fee_entry.get("withdraw_amount_mini", 0)),
                    "withdrawal_fee": safe_float(fee_entry.get("withdraw_txfee", 0)),
                    "withdraw_tax": 0,
                })

            token_entry = {
                "symbol": symbol,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "base_coin_name": base_asset,
                "contract_size": None,
                "market_type": MarketType.SPOT,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains
            }

            result.append(token_entry)
            spot_map[base_asset] = chains

        # собираем futures токены, копируя chains со спота
        for contract in futures_data:
            symbol = contract["name"]
            base_asset, quote_asset = symbol.split("_")

            if quote_asset != "USDT":
                continue

            if base_asset.endswith(("3S", "3L", "5S", "5L")):
                continue

            chains = spot_map.get(base_asset, [])

            token_entry = {
                "symbol": symbol,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "base_coin_name": base_asset,
                "contract_size": 1.0,
                "market_type": MarketType.FUTURES,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains
            }

            result.append(token_entry)

        return result

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

import asyncio
import httpx

from infrastructure.database.models.base import MarketType, SourceType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.interfaces import SupportsTokenParsing
from infrastructure.services.collectors.structures.normalizer import ChainNormalizer
from infrastructure.services.collectors.structures.schemas import CachedResult
from infrastructure.services.collectors.token_collector.base import BaseTokenParser
from infrastructure.services.common.cache import RedisCache
from settings import logger


class BybitTokenParser(BaseTokenParser, SupportsTokenParsing):
    """Token parser for Bybit exchange.

    API docs: https://bybit-exchange.github.io/docs/v5/
    Public endpoints (no auth):
      GET /v5/market/instruments-info?category=spot   — spot pairs
      GET /v5/market/instruments-info?category=linear — linear perpetual contracts
    Chain info requires auth, so chains are empty — merging via chainless global_token logic.
    Bybit is a TRUSTED_SOURCE so price-based merge applies automatically.
    """

    BASE_URL = "https://api.bybit.com"
    MAX_LIMIT = 1000

    def __init__(
            self,
            repo: RequestsRepo,
            cache: RedisCache,
            chain_normalizer: ChainNormalizer,
            tokens_load_limit: int = 50000,
    ):
        super().__init__(
            cache=cache,
            chain_normalizer=chain_normalizer,
            repo=repo,
            source_name="bybit",
            source_type=SourceType.CEX,
        )
        self.tokens_load_limit = tokens_load_limit

    async def fetch_tokens(self) -> list[dict]:
        spot_data, linear_data = await asyncio.gather(
            self._fetch_instruments("spot"),
            self._fetch_instruments("linear"),
            return_exceptions=True,
        )

        if isinstance(spot_data, Exception):
            logger.error(f"Bybit: failed to fetch spot instruments: {spot_data}")
            spot_data = []
        if isinstance(linear_data, Exception):
            logger.error(f"Bybit: failed to fetch linear instruments: {linear_data}")
            linear_data = []

        return self._build_token_map(spot_data or [], linear_data or [])

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        data = await self.fetch_tokens()
        if not data:
            return CachedResult([], False)
        return await super().fetch_and_check_internal(f"{self.source_name}:token_map", data)

    async def _fetch_instruments(self, category: str) -> list[dict]:
        url = f"{self.BASE_URL}/v5/market/instruments-info"
        results = []
        cursor = ""
        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params = {"category": category, "limit": self.MAX_LIMIT}
                if cursor:
                    params["cursor"] = cursor
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()
                if data.get("retCode") != 0:
                    raise ValueError(f"Bybit API error: {data.get('retMsg')}")
                result = data.get("result", {})
                results.extend(result.get("list", []))
                cursor = result.get("nextPageCursor", "")
                if not cursor:
                    break
        return results

    def _build_token_map(
            self,
            spot_instruments: list[dict],
            linear_instruments: list[dict],
    ) -> list[dict]:
        result: list[dict] = []

        # ── Spot ──────────────────────────────────────────────────────────────
        for inst in spot_instruments:
            if inst.get("status") != "Trading":
                continue
            base = inst.get("baseCoin", "").upper()
            quote = inst.get("quoteCoin", "").upper()
            if quote != "USDT":
                continue
            if base.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            result.append({
                "symbol": inst.get("symbol", ""),
                "base_asset": base,
                "quote_asset": quote,
                "base_coin_name": base,
                "contract_size": None,
                "market_type": MarketType.SPOT,
                "is_active": True,
                "source_id": self.source.id,
                "chains": [],
            })

        # ── Linear Perpetual ──────────────────────────────────────────────────
        for inst in linear_instruments:
            if inst.get("status") != "Trading":
                continue
            if inst.get("contractType") != "LinearPerpetual":
                continue
            base = inst.get("baseCoin", "").upper()
            quote = inst.get("quoteCoin", "").upper()
            settle = inst.get("settleCoin", "").upper()
            if quote != "USDT" or settle != "USDT":
                continue
            if base.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            result.append({
                "symbol": inst.get("symbol", ""),
                "base_asset": base,
                "quote_asset": quote,
                "base_coin_name": base,
                "contract_size": 1.0,
                "market_type": MarketType.FUTURES,
                "is_active": True,
                "source_id": self.source.id,
                "chains": [],
            })

        logger.info(
            f"Bybit token map built: "
            f"{sum(1 for t in result if t['market_type'] == MarketType.SPOT)} spot, "
            f"{sum(1 for t in result if t['market_type'] == MarketType.FUTURES)} futures"
        )
        return result

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

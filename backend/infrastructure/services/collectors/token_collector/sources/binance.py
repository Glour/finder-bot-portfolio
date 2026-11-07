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


class BinanceTokenParser(BaseTokenParser, SupportsTokenParsing):
    """Token parser for Binance exchange.

    API docs: https://binance-docs.github.io/apidocs/
    Public endpoints (no auth):
      GET https://api.binance.com/api/v3/exchangeInfo   — spot
      GET https://fapi.binance.com/fapi/v1/exchangeInfo — usdt-m futures
    Chain info requires auth → chainless merge (trusted source).
    """

    SPOT_URL = "https://api.binance.com/api/v3/exchangeInfo"
    FUTURES_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

    # Leveraged token suffixes on Binance spot
    LEVERAGED_SUFFIXES = ("UP", "DOWN", "BEAR", "BULL", "3S", "3L", "5S", "5L", "2S", "2L")

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
            source_name="binance",
            source_type=SourceType.CEX,
        )
        self.tokens_load_limit = tokens_load_limit

    async def fetch_tokens(self) -> list[dict]:
        spot_data, futures_data = await asyncio.gather(
            self._fetch_exchange_info(self.SPOT_URL),
            self._fetch_exchange_info(self.FUTURES_URL),
            return_exceptions=True,
        )
        if isinstance(spot_data, Exception):
            logger.error(f"Binance: failed to fetch spot info: {spot_data}")
            spot_data = []
        if isinstance(futures_data, Exception):
            logger.error(f"Binance: failed to fetch futures info: {futures_data}")
            futures_data = []
        return self._build_token_map(spot_data or [], futures_data or [])

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        data = await self.fetch_tokens()
        if not data:
            return CachedResult([], False)
        return await super().fetch_and_check_internal(f"{self.source_name}:token_map", data)

    async def _fetch_exchange_info(self, url: str) -> list[dict]:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
        return data.get("symbols", [])

    def _build_token_map(
            self,
            spot_symbols: list[dict],
            futures_symbols: list[dict],
    ) -> list[dict]:
        result: list[dict] = []

        # ── Spot ──────────────────────────────────────────────────────────────
        for sym in spot_symbols:
            if sym.get("status") != "TRADING":
                continue
            if not sym.get("isSpotTradingAllowed", True):
                continue
            base = sym.get("baseAsset", "").upper()
            quote = sym.get("quoteAsset", "").upper()
            if quote != "USDT":
                continue
            if base.endswith(self.LEVERAGED_SUFFIXES):
                continue

            result.append({
                "symbol": sym.get("symbol", ""),
                "base_asset": base,
                "quote_asset": quote,
                "base_coin_name": base,
                "contract_size": None,
                "market_type": MarketType.SPOT,
                "is_active": True,
                "source_id": self.source.id,
                "chains": [],
            })

        # ── USDT-M Futures (perpetual) ─────────────────────────────────────
        for sym in futures_symbols:
            if sym.get("status") != "TRADING":
                continue
            if sym.get("contractType") != "PERPETUAL":
                continue
            base = sym.get("baseAsset", "").upper()
            quote = sym.get("quoteAsset", "").upper()
            if quote != "USDT":
                continue
            if base.endswith(self.LEVERAGED_SUFFIXES):
                continue

            result.append({
                "symbol": sym.get("symbol", ""),
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
            f"Binance token map built: "
            f"{sum(1 for t in result if t['market_type'] == MarketType.SPOT)} spot, "
            f"{sum(1 for t in result if t['market_type'] == MarketType.FUTURES)} futures"
        )
        return result

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

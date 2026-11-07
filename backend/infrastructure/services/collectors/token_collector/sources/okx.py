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


class OkxTokenParser(BaseTokenParser, SupportsTokenParsing):
    """Token parser for OKX exchange.

    API docs: https://www.okx.com/docs-v5/en/
    Public endpoints (no auth required):
      GET /api/v5/public/instruments?instType=SPOT
      GET /api/v5/public/instruments?instType=SWAP
    Chain info requires auth, so chains list is empty for OKX.
    Merging with other exchanges happens via price-based merge (price field).
    """

    BASE_URL = "https://www.okx.com"
    SPOT_ENDPOINT = "/api/v5/public/instruments?instType=SPOT"
    SWAP_ENDPOINT = "/api/v5/public/instruments?instType=SWAP"

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
            source_name="okx",
            source_type=SourceType.CEX,
        )
        self.tokens_load_limit = tokens_load_limit

    async def fetch_tokens(self) -> list[dict]:
        spot_data, swap_data = await asyncio.gather(
            self._fetch_instruments(self.SPOT_ENDPOINT),
            self._fetch_instruments(self.SWAP_ENDPOINT),
            return_exceptions=True,
        )

        if isinstance(spot_data, Exception):
            logger.error(f"OKX: failed to fetch spot instruments: {spot_data}")
            spot_data = []
        if isinstance(swap_data, Exception):
            logger.error(f"OKX: failed to fetch swap instruments: {swap_data}")
            swap_data = []

        return self._build_token_map(spot_data or [], swap_data or [])

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        data = await self.fetch_tokens()
        if not data:
            return CachedResult([], False)
        return await super().fetch_and_check_internal(f"{self.source_name}:token_map", data)

    async def _fetch_instruments(self, endpoint: str) -> list[dict]:
        url = f"{self.BASE_URL}{endpoint}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            result = resp.json()
            if result.get("code") != "0":
                raise ValueError(f"OKX API error: {result.get('msg')}")
            return result.get("data", [])

    def _build_token_map(
            self,
            spot_instruments: list[dict],
            swap_instruments: list[dict],
    ) -> list[dict]:
        result: list[dict] = []

        # ── Spot ──────────────────────────────────────────────────────────────
        for inst in spot_instruments:
            if inst.get("state") != "live":
                continue

            base = inst.get("baseCcy", "").upper()
            quote = inst.get("quoteCcy", "").upper()

            if quote != "USDT":
                continue

            # Skip leveraged tokens
            if base.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            inst_id = inst.get("instId", "")  # e.g. "BTC-USDT"

            result.append({
                "symbol": inst_id,
                "base_asset": base,
                "quote_asset": quote,
                "base_coin_name": base,
                "contract_size": None,
                "market_type": MarketType.SPOT,
                "is_active": True,
                "source_id": self.source.id,
                "chains": [],  # No public chain info on OKX
            })

        # ── Perpetual Swaps ───────────────────────────────────────────────────
        for inst in swap_instruments:
            if inst.get("state") != "live":
                continue

            # Only linear (USDT-margined) swaps
            if inst.get("ctType") != "linear":
                continue
            if inst.get("settleCcy", "").upper() != "USDT":
                continue

            # Base asset from instFamily: "BTC-USDT" → "BTC"
            inst_family = inst.get("instFamily", "")
            if not inst_family or "-" not in inst_family:
                continue
            base = inst_family.split("-")[0].upper()

            if base.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            inst_id = inst.get("instId", "")  # e.g. "BTC-USDT-SWAP"
            ct_val = inst.get("ctVal")
            try:
                contract_size = float(ct_val) if ct_val else 1.0
            except (ValueError, TypeError):
                contract_size = 1.0

            result.append({
                "symbol": inst_id,
                "base_asset": base,
                "quote_asset": "USDT",
                "base_coin_name": base,
                "contract_size": contract_size,
                "market_type": MarketType.FUTURES,
                "is_active": True,
                "source_id": self.source.id,
                "chains": [],
            })

        logger.info(
            f"OKX token map built: {sum(1 for t in result if t['market_type'] == MarketType.SPOT)} spot, "
            f"{sum(1 for t in result if t['market_type'] == MarketType.FUTURES)} futures"
        )
        return result

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

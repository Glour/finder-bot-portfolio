import asyncio
import httpx

from infrastructure.database.models.base import MarketType, SourceType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.interfaces import SupportsTokenParsing
from infrastructure.services.collectors.structures.normalizer import ChainNormalizer, safe_float
from infrastructure.services.collectors.structures.schemas import CachedResult
from infrastructure.services.collectors.token_collector.base import BaseTokenParser
from infrastructure.services.common.cache import RedisCache
from settings import logger


class BitgetTokenParser(BaseTokenParser, SupportsTokenParsing):
    """Token parser for Bitget exchange.

    API docs: https://www.bitget.com/api-doc/
    All endpoints are public (no auth required):
      GET /api/v2/spot/public/symbols       — spot pairs
      GET /api/v2/mix/market/contracts      — futures contracts
      GET /api/v2/spot/public/coins         — chain / deposit / withdraw info
    """

    BASE_URL = "https://api.bitget.com"

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
            source_name="bitget",
            source_type=SourceType.CEX,
        )
        self.tokens_load_limit = tokens_load_limit

    async def fetch_tokens(self) -> list[dict]:
        spot_data, futures_data, coins_data = await asyncio.gather(
            self._get("/api/v2/spot/public/symbols"),
            self._get("/api/v2/mix/market/contracts", params={"productType": "USDT-FUTURES"}),
            self._get("/api/v2/spot/public/coins"),
            return_exceptions=True,
        )

        if isinstance(spot_data, Exception):
            logger.error(f"Bitget: failed to fetch spot symbols: {spot_data}")
            spot_data = []
        if isinstance(futures_data, Exception):
            logger.error(f"Bitget: failed to fetch futures contracts: {futures_data}")
            futures_data = []
        if isinstance(coins_data, Exception):
            logger.error(f"Bitget: failed to fetch coins: {coins_data}")
            coins_data = []

        return self._build_token_map(
            spot_data or [],
            futures_data or [],
            coins_data or [],
        )

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        data = await self.fetch_tokens()
        if not data:
            return CachedResult([], False)
        return await super().fetch_and_check_internal(f"{self.source_name}:token_map", data)

    async def _get(self, path: str, params: dict | None = None) -> list[dict]:
        url = f"{self.BASE_URL}{path}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            result = resp.json()
            if result.get("code") != "00000":
                raise ValueError(f"Bitget API error: {result.get('msg')}")
            return result.get("data", [])

    def _build_chain_map(self, coins_data: list[dict]) -> dict[str, list[dict]]:
        """Build coin → chains lookup from /api/v2/spot/public/coins."""
        chain_map: dict[str, list[dict]] = {}
        for coin in coins_data:
            symbol = coin.get("coin", "").upper()
            if not symbol:
                continue
            chains = []
            for net in coin.get("chains", []):
                raw_chain = net.get("chain", "")
                normalized = self.chain_normalizer.normalize_from_cex(raw_chain)
                if not normalized:
                    continue
                chains.append({
                    "chain_name": normalized,
                    "contract_address": net.get("contractAddress") or "",
                    "deposit_enabled": net.get("rechargeable") == "true",
                    "withdraw_enabled": net.get("withdrawable") == "true",
                    "withdrawal_fee": safe_float(net.get("withdrawFee", 0)),
                    "min_withdraw_amount": safe_float(net.get("minWithdrawAmount", 0)),
                    "min_deposit_amount": safe_float(net.get("minDepositAmount", 0)),
                })
            if chains:
                chain_map[symbol] = chains
        return chain_map

    def _build_token_map(
            self,
            spot_symbols: list[dict],
            futures_contracts: list[dict],
            coins_data: list[dict],
    ) -> list[dict]:
        result: list[dict] = []
        chain_map = self._build_chain_map(coins_data)
        spot_chain_map: dict[str, list[dict]] = {}

        # ── Spot ──────────────────────────────────────────────────────────────
        for sym in spot_symbols:
            if sym.get("status") != "online":
                continue
            base = sym.get("baseCoin", "").upper()
            quote = sym.get("quoteCoin", "").upper()
            if quote != "USDT":
                continue
            if base.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            symbol = sym.get("symbol", "")
            chains = chain_map.get(base, [])
            spot_chain_map[base] = chains

            result.append({
                "symbol": symbol,
                "base_asset": base,
                "quote_asset": quote,
                "base_coin_name": base,
                "contract_size": None,
                "market_type": MarketType.SPOT,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains,
            })

        # ── USDT-FUTURES perpetual ────────────────────────────────────────────
        for contract in futures_contracts:
            if contract.get("symbolStatus") != "normal":
                continue
            if contract.get("symbolType") != "perpetual":
                continue
            base = contract.get("baseCoin", "").upper()
            quote = contract.get("quoteCoin", "").upper()
            if quote != "USDT":
                continue
            if base.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            symbol = contract.get("symbol", "")
            chains = spot_chain_map.get(base) or chain_map.get(base, [])

            result.append({
                "symbol": symbol,
                "base_asset": base,
                "quote_asset": quote,
                "base_coin_name": base,
                "contract_size": 1.0,
                "market_type": MarketType.FUTURES,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains,
            })

        logger.info(
            f"Bitget token map built: "
            f"{sum(1 for t in result if t['market_type'] == MarketType.SPOT)} spot, "
            f"{sum(1 for t in result if t['market_type'] == MarketType.FUTURES)} futures"
        )
        return result

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

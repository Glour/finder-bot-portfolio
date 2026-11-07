import asyncio
import httpx

from infrastructure.database.models.base import MarketType, SourceType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.interfaces import SupportsTokenParsing
from infrastructure.services.collectors.structures.normalizer import ChainNormalizer, normalize_contract_address
from infrastructure.services.collectors.structures.schemas import CachedResult
from infrastructure.services.collectors.token_collector.base import BaseTokenParser
from infrastructure.services.common.cache import RedisCache
from settings import logger


class KucoinTokenParser(BaseTokenParser, SupportsTokenParsing):
    """Token parser for KuCoin exchange.

    API docs: https://docs.kucoin.com/
    Public endpoints (no auth):
      GET /api/v2/symbols                — spot pairs
      GET /api/v3/currencies             — all currencies with chains (bulk)
      futures: GET /api/v1/contracts/active  (api-futures.kucoin.com)

    Symbol formats:
      Spot:    BTC-USDT   (baseCurrency-quoteCurrency)
      Futures: XBTUSDTM   (BTC is XBT on KuCoin; base_asset normalized to BTC)
    """

    SPOT_URL = "https://api.kucoin.com/api/v2/symbols"
    CURRENCIES_URL = "https://api.kucoin.com/api/v3/currencies"
    FUTURES_URL = "https://api-futures.kucoin.com/api/v1/contracts/active"

    LEVERAGED_SUFFIXES = ("3S", "3L", "5S", "5L", "2S", "2L", "UP", "DOWN")

    # KuCoin futures uses XBT for Bitcoin
    BASE_REMAP = {"XBT": "BTC"}

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
            source_name="kucoin",
            source_type=SourceType.CEX,
        )
        self.tokens_load_limit = tokens_load_limit

    async def fetch_tokens(self) -> list[dict]:
        spot_data, currencies_data, futures_data = await asyncio.gather(
            self._fetch_json(self.SPOT_URL, key="data"),
            self._fetch_json(self.CURRENCIES_URL, key="data"),
            self._fetch_json(self.FUTURES_URL, key="data", base_url="futures"),
            return_exceptions=True,
        )
        for name, result in [("spot", spot_data), ("currencies", currencies_data), ("futures", futures_data)]:
            if isinstance(result, Exception):
                logger.error(f"KuCoin: failed to fetch {name}: {result}")
        spot_data = spot_data if not isinstance(spot_data, Exception) else []
        currencies_data = currencies_data if not isinstance(currencies_data, Exception) else []
        futures_data = futures_data if not isinstance(futures_data, Exception) else []

        chain_map = self._build_chain_map(currencies_data or [])
        return self._build_token_map(spot_data or [], futures_data or [], chain_map)

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        data = await self.fetch_tokens()
        if not data:
            return CachedResult([], False)
        return await super().fetch_and_check_internal(f"{self.source_name}:token_map", data)

    async def _fetch_json(self, url: str, key: str = "data", base_url: str = "spot") -> list[dict]:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
        if data.get("code") != "200000":
            raise ValueError(f"KuCoin API error: {data.get('msg')}")
        return data.get(key, [])

    def _build_chain_map(self, currencies: list[dict]) -> dict[str, list[dict]]:
        """Build {currency: [{chain_name, contract_address, ...}]} map."""
        result: dict[str, list[dict]] = {}
        for cur in currencies:
            currency = cur.get("currency", "").upper()
            if not currency:
                continue
            chains_raw = cur.get("chains") or []
            parsed_chains = []
            for ch in chains_raw:
                if not ch.get("isDepositEnabled") and not ch.get("isWithdrawEnabled"):
                    continue
                raw_chain = ch.get("chainName") or ch.get("chainId") or ""
                normalized = self.chain_normalizer.normalize_from_cex(raw_chain)
                if not normalized:
                    continue
                contract = normalize_contract_address(ch.get("contractAddress"))
                parsed_chains.append({
                    "chain_name": normalized,
                    "contract_address": contract,
                    "is_deposit_enabled": ch.get("isDepositEnabled", False),
                    "is_withdraw_enabled": ch.get("isWithdrawEnabled", False),
                })
            if parsed_chains:
                result[currency] = parsed_chains
        return result

    def _build_token_map(
            self,
            spot_symbols: list[dict],
            futures_contracts: list[dict],
            chain_map: dict[str, list[dict]],
    ) -> list[dict]:
        result: list[dict] = []

        # ── Spot ──────────────────────────────────────────────────────────────
        for sym in spot_symbols:
            if not sym.get("enableTrading"):
                continue
            base = sym.get("baseCurrency", "").upper()
            quote = sym.get("quoteCurrency", "").upper()
            if quote != "USDT":
                continue
            if base.endswith(self.LEVERAGED_SUFFIXES):
                continue

            chains = chain_map.get(base, [])
            result.append({
                "symbol": sym.get("symbol", ""),
                "base_asset": base,
                "quote_asset": quote,
                "base_coin_name": base,
                "contract_size": None,
                "market_type": MarketType.SPOT,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains,
            })

        # ── Futures (USDT linear perpetual) ──────────────────────────────────
        for con in futures_contracts:
            if con.get("status") != "Open":
                continue
            if con.get("isInverse"):
                continue
            if con.get("type") != "FFWCSX":  # perpetual swap
                continue
            if con.get("settleCurrency") != "USDT":
                continue

            base_raw = con.get("baseCurrency", "").upper()
            base = self.BASE_REMAP.get(base_raw, base_raw)  # XBT → BTC
            if base.endswith(self.LEVERAGED_SUFFIXES):
                continue

            chains = chain_map.get(base, [])
            result.append({
                "symbol": con.get("symbol", ""),
                "base_asset": base,
                "quote_asset": "USDT",
                "base_coin_name": base,
                "contract_size": float(con.get("multiplier", 1) or 1),
                "market_type": MarketType.FUTURES,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains,
            })

        logger.info(
            f"KuCoin token map built: "
            f"{sum(1 for t in result if t['market_type'] == MarketType.SPOT)} spot, "
            f"{sum(1 for t in result if t['market_type'] == MarketType.FUTURES)} futures"
        )
        return result

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

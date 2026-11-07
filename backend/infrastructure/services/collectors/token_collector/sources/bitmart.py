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


class BitmartTokenParser(BaseTokenParser, SupportsTokenParsing):
    """Token parser for Bitmart exchange.

    REST API:
    - Spot: https://api-cloud.bitmart.com
    - Futures: https://api-cloud-v2.bitmart.com
    """

    SPOT_HOST = "api-cloud.bitmart.com"
    FUTURES_HOST = "api-cloud-v2.bitmart.com"

    # Network name mapping to standard chain names
    NETWORK_MAP = {
        "BTC": "BTC",
        "ETH": "ETH",
        "ERC20": "ETH",
        "TRC20": "TRX",
        "TRX": "TRX",
        "BSC": "BSC",
        "BEP20": "BSC",
        "SOL": "SOL",
        "SOLANA": "SOL",
        "ARB": "ARB",
        "ARBITRUM": "ARB",
        "MATIC": "POLYGON",
        "POLYGON": "POLYGON",
        "AVAX": "AVAX",
        "AVAX_CCHAIN": "AVAX",
        "OP": "OPTIMISM",
        "OPTIMISM": "OPTIMISM",
        "BASE": "BASE",
        "TON": "TON",
        "SUI": "SUI",
        "APT": "APT",
        "APTOS": "APT",
    }

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
            source_name="bitmart",
            source_type=SourceType.CEX,
        )
        self.repo = repo
        self.tokens_load_limit = tokens_load_limit

    async def fetch_tokens(self) -> list[dict]:
        """Fetch all tokens from Bitmart API."""
        # Fetch sequentially with delays to avoid rate limiting
        currencies = await self._fetch_currencies()
        await asyncio.sleep(0.3)

        spot_pairs = await self._fetch_spot_symbols()
        await asyncio.sleep(0.3)

        futures_contracts = await self._fetch_futures_contracts()

        if not spot_pairs:
            logger.warning("Bitmart: failed to fetch spot pairs")
            return []

        return self._build_token_map(
            currencies or [],
            spot_pairs,
            futures_contracts or [],
        )

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        data = await self.fetch_tokens()
        if not data:
            return CachedResult([], False)
        return await super().fetch_and_check_internal(f"{self.source_name}:token_map", data)

    async def _make_request(self, host: str, route: str) -> dict | list | None:
        """Make HTTP request to Bitmart API."""
        url = f"https://{host}{route}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()

                # Bitmart returns {"code": 1000, "message": "OK", "data": {...}}
                if data.get("code") != 1000:
                    logger.warning(f"Bitmart API error: {data.get('message')}")
                    return None

                return data.get("data")
            except Exception as e:
                logger.error(f"Bitmart request failed [{url}]: {e}")
                return None

    async def _fetch_currencies(self) -> list[dict] | None:
        """Fetch currency info with networks."""
        data = await self._make_request(self.SPOT_HOST, "/account/v1/currencies")
        if isinstance(data, dict) and "currencies" in data:
            return data["currencies"]
        return None

    async def _fetch_spot_symbols(self) -> list[dict] | None:
        """Fetch spot trading pairs."""
        data = await self._make_request(self.SPOT_HOST, "/spot/v1/symbols/details")
        if isinstance(data, dict) and "symbols" in data:
            return data["symbols"]
        return None

    async def _fetch_futures_contracts(self) -> list[dict] | None:
        """Fetch futures/perpetual contracts."""
        data = await self._make_request(self.FUTURES_HOST, "/contract/public/details")
        if isinstance(data, dict) and "symbols" in data:
            return data["symbols"]
        return None

    def _build_token_map(
            self,
            currencies: list[dict],
            spot_pairs: list[dict],
            futures_contracts: list[dict]
    ) -> list[dict]:
        """Build unified token map from all data sources."""
        result: list[dict] = []

        # Build currency info lookup: symbol -> network info
        currency_map: dict[str, dict] = {}
        for curr in currencies:
            symbol = curr.get("currency", curr.get("id", "")).upper()
            if not symbol:
                continue

            network = curr.get("network", "").upper()
            chain_name = self.NETWORK_MAP.get(network)

            if symbol not in currency_map:
                currency_map[symbol] = {
                    "chains": [],
                    "withdraw_enabled": curr.get("withdraw_enabled", True),
                    "deposit_enabled": curr.get("deposit_enabled", True),
                }

            if chain_name:
                currency_map[symbol]["chains"].append({
                    "chain_name": chain_name,
                    "contract_address": curr.get("contract_address") or "",  # preserve original case
                    "withdraw_enabled": curr.get("withdraw_enabled", True),
                    "deposit_enabled": curr.get("deposit_enabled", True),
                    "withdrawal_fee": safe_float(curr.get("withdraw_fee", 0)),
                    "min_withdraw_amount": safe_float(curr.get("withdraw_minsize", 0)),
                    "min_deposit_amount": safe_float(curr.get("recharge_minsize", 0)),
                })

        spot_map: dict[str, list[dict]] = {}

        # Process spot pairs
        for pair in spot_pairs:
            symbol = pair.get("symbol", "")
            base_asset = pair.get("base_currency", "").upper()
            quote_asset = pair.get("quote_currency", "").upper()

            # Filter: only USDT/USDC pairs
            if quote_asset != "USDT":
                continue

            # Filter: only trading pairs
            trade_status = pair.get("trade_status", "").lower()
            if trade_status != "trading":
                continue

            # Skip leveraged tokens
            if base_asset.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            # Get chains from currency info
            currency_info = currency_map.get(base_asset, {})
            chains = currency_info.get("chains", [])

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

        # Process futures contracts
        for contract in futures_contracts:
            symbol = contract.get("symbol", "")
            base_asset = contract.get("base_currency", "").upper()
            quote_asset = contract.get("quote_currency", "").upper()

            # Filter: only USDT/USDC pairs
            if quote_asset != "USDT":
                continue

            # Filter: only trading contracts
            status = contract.get("status", "").lower()
            if status != "trading":
                continue

            # Skip leveraged tokens
            if base_asset.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            # Get contract size (Bitmart uses 0.001 for BTC, etc.)
            contract_size = safe_float(contract.get("contract_size", 1.0))
            if contract_size == 0:
                contract_size = 1.0

            # Get chains from spot or currency info
            chains = spot_map.get(base_asset, currency_map.get(base_asset, {}).get("chains", []))

            token_entry = {
                "symbol": symbol,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "base_coin_name": base_asset,
                "contract_size": contract_size,
                "market_type": MarketType.FUTURES,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains
            }

            result.append(token_entry)

        logger.info(f"Bitmart: built {len(result)} tokens (spot + futures)")
        return result

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

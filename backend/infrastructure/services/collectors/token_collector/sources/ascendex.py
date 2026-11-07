import asyncio
import httpx

from infrastructure.database.models.base import MarketType
from infrastructure.database.models.base import SourceType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.interfaces import SupportsTokenParsing
from infrastructure.services.collectors.structures.normalizer import ChainNormalizer
from infrastructure.services.collectors.structures.schemas import CachedResult
from infrastructure.services.collectors.token_collector.base import BaseTokenParser
from infrastructure.services.common.cache import RedisCache


class AscendexTokenParser(BaseTokenParser, SupportsTokenParsing):
    """Token parser for AscendEX (formerly BitMax) CEX exchange.

    API Documentation: https://ascendex.github.io/ascendex-pro-api/

    Endpoints:
    - Spot: https://ascendex.com/api/pro/v1/cash/products
    - Futures: https://ascendex.com/api/pro/v2/futures/contract
    - Assets: https://ascendex.com/api/pro/v2/assets
    """

    spot_host = "ascendex.com"
    futures_host = "ascendex.com"
    assets_host = "ascendex.com"

    # Mapping chain names from AscendEX to standard names
    CHAIN_NAME_MAP = {
        "ERC20": "ETH",
        "Solana": "SOL",
        "BEP20": "BSC",
        "Arbitrum": "ARB",
        "Polygon": "MATIC",
        "Avalanche": "AVAX",
        "Optimism": "OP",
        "TRC20": "TRX",
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
            source_name="ascendex",
            source_type=SourceType.CEX,
        )
        self.repo = repo
        self.tokens_load_limit = tokens_load_limit

    async def fetch_tokens(self) -> list[dict]:
        """Fetch all tokens from AscendEX API."""
        # Fetch sequentially with delays to avoid rate limiting
        spot_pairs = await self.fetch_spot_data()
        await asyncio.sleep(0.3)

        futures_contracts = await self.fetch_futures_data()
        await asyncio.sleep(0.3)

        assets = await self.fetch_assets()

        if not spot_pairs and not futures_contracts:
            return []

        return self.build_token_map(
            assets or [],
            spot_pairs or [],
            futures_contracts or [],
        )

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        data = await self.fetch_tokens()
        if not data:
            return CachedResult([], False)
        return await super().fetch_and_check_internal(f"{self.source_name}:token_map", data)

    async def _make_request(self, host: str, route: str) -> dict | list | None:
        """Make HTTP request with fresh client to avoid connection issues."""
        url = f"https://{host}/{route}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
                return response.json()
            except Exception:
                return None

    async def fetch_spot_data(self) -> list[dict] | None:
        """Fetch spot trading pairs from AscendEX Spot API."""
        data = await self._make_request(self.spot_host, "api/pro/v1/cash/products")
        if not data or not isinstance(data, dict):
            return []
        return data.get("data", [])

    async def fetch_futures_data(self) -> list[dict] | None:
        """Fetch futures contracts from AscendEX Futures API."""
        data = await self._make_request(self.futures_host, "api/pro/v2/futures/contract")
        if not data or not isinstance(data, dict):
            return []
        return data.get("data", [])

    async def fetch_assets(self) -> list[dict] | None:
        """Fetch assets with chain information."""
        data = await self._make_request(self.assets_host, "api/pro/v2/assets")
        if not data or not isinstance(data, dict):
            return []
        return data.get("data", [])

    def _parse_symbol(self, symbol: str) -> tuple[str, str]:
        """Parse symbol to extract base and quote assets.

        Examples:
        - "BTC/USDT" -> ("BTC", "USDT")
        - "1KPEPE/USDT" -> ("1KPEPE", "USDT")
        """
        if "/" in symbol:
            parts = symbol.split("/", 1)
            return parts[0].upper(), parts[1].upper() if len(parts) > 1 else ""
        return symbol.upper(), ""

    def _build_chain_map(self, assets: list[dict]) -> dict[str, list[dict]]:
        """Build mapping from asset code to chain information."""
        chain_map: dict[str, list[dict]] = {}

        for asset in assets:
            asset_code = asset.get("assetCode", "").upper()
            if not asset_code:
                continue

            blockchains = asset.get("blockChain", [])
            if not blockchains:
                continue

            for chain in blockchains:
                chain_name_raw = chain.get("chainName", "")
                chain_name = self.CHAIN_NAME_MAP.get(chain_name_raw, chain_name_raw)

                if not chain_name:
                    continue

                chain_info = {
                    "chain_name": chain_name,
                    "contract_address": "",  # AscendEX doesn't provide contract addresses in this endpoint
                    "deposit_enabled": chain.get("allowDeposit", False),
                    "withdraw_enabled": chain.get("allowWithdraw", False),
                    "min_deposit_amount": None,
                    "deposit_tax": 0,
                    "min_withdraw_amount": float(chain.get("minWithdrawal", 0)) if chain.get("minWithdrawal") else None,
                    "withdrawal_fee": float(chain.get("withdrawFee", 0)) if chain.get("withdrawFee") else 0,
                    "withdraw_tax": 0,
                }

                if asset_code not in chain_map:
                    chain_map[asset_code] = []

                # Avoid duplicates
                existing_chains = [c["chain_name"] for c in chain_map[asset_code]]
                if chain_name not in existing_chains:
                    chain_map[asset_code].append(chain_info)

        return chain_map

    def build_token_map(
            self,
            assets: list[dict],
            spot_data: list[dict],
            futures_data: list[dict]
    ) -> list[dict]:
        """Build unified token map from all data sources."""
        result: list[dict] = []
        chain_map = self._build_chain_map(assets)
        spot_map = {}

        # Process spot tokens
        for token in spot_data:
            symbol = token.get("symbol", "")
            status_code = token.get("statusCode", "")

            if not symbol:
                continue

            # Only active trading pairs
            if status_code != "Normal":
                continue

            base_asset, quote_asset = self._parse_symbol(symbol)

            # Only USDT and USDC pairs
            if quote_asset != "USDT":
                continue

            # Skip leveraged tokens (3S, 3L, 5S, 5L, etc.)
            if base_asset.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            # CRITICAL FIX (2026-02-19): AscendEX doesn't provide contract_address
            # Use synthetic "ascendex" chain like Hyperliquid to avoid GT contamination
            # Example: AI (ascendex, ethereum, "") polluted GT "MYX" with GALA (gate, ethereum, 0xd1d2...)
            chains = [{
                "chain_name": "ascendex",  # Synthetic chain for chainless tokens
                "contract_address": "",
                "deposit_enabled": False,
                "withdraw_enabled": False,
                "min_deposit_amount": None,
                "deposit_tax": 0,
                "min_withdraw_amount": None,
                "withdrawal_fee": 0,
                "withdraw_tax": 0,
            }]

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
        for contract in futures_data:
            symbol = contract.get("symbol", "")
            status = contract.get("status", "")
            underlying = contract.get("underlying", "")

            if not symbol or not underlying:
                continue

            # Only active contracts
            if status != "Normal":
                continue

            # Only perpetual contracts (with -PERP suffix)
            if "-PERP" not in symbol:
                continue

            # Parse underlying to get base and quote assets
            # Example: "BTC/USDT" or "1KPEPE/USDT"
            base_asset, quote_asset = self._parse_symbol(underlying)

            # Only USDT and USDC pairs
            if quote_asset != "USDT":
                continue

            # Skip leveraged tokens
            if base_asset.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            # CRITICAL FIX (2026-02-19): Same as SPOT - use synthetic "ascendex" chain
            # Futures contracts also don't have contract addresses
            chains = [{
                "chain_name": "ascendex",  # Synthetic chain for chainless tokens
                "contract_address": "",
                "deposit_enabled": False,
                "withdraw_enabled": False,
                "min_deposit_amount": None,
                "deposit_tax": 0,
                "min_withdraw_amount": None,
                "withdrawal_fee": 0,
                "withdraw_tax": 0,
            }]

            token_entry = {
                "symbol": symbol,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "base_coin_name": base_asset,  # "1KPEPE" stays as is, won't merge with "PEPE"
                "contract_size": 1.0,  # AscendEX uses lotSize, but usually 1
                "market_type": MarketType.FUTURES,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains
            }

            result.append(token_entry)

        return result

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

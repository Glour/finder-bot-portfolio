import asyncio
import httpx

from infrastructure.database.models.base import MarketType
from infrastructure.database.models.base import SourceType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.interfaces import SupportsTokenParsing
from infrastructure.services.collectors.structures.normalizer import ChainNormalizer, safe_float
from infrastructure.services.collectors.structures.schemas import CachedResult
from infrastructure.services.collectors.token_collector.base import BaseTokenParser
from infrastructure.services.common.cache import RedisCache


class AsterdexTokenParser(BaseTokenParser, SupportsTokenParsing):
    """Token parser for Asterdex CEX exchange.

    API Documentation: https://github.com/asterdex/api-docs

    Endpoints:
    - Spot: https://sapi.asterdex.com/api/v1/exchangeInfo
    - Futures: https://fapi.asterdex.com/fapi/v3/exchangeInfo
    - Assets: https://www.asterdex.com/bapi/futures/v1/public/future/aster/deposit/assets
    """

    spot_host = "sapi.asterdex.com"
    futures_host = "fapi.asterdex.com"
    assets_host = "www.asterdex.com"

    # Mapping chainId to standard chain names
    CHAIN_ID_MAP = {
        1: "ETH",
        42161: "ARB",
        56: "BSC",
        101: "SOL",
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
            source_name="asterdex",
            source_type=SourceType.CEX,
        )
        self.repo = repo
        self.tokens_load_limit = tokens_load_limit

    async def fetch_tokens(self) -> list[dict]:
        # Fetch sequentially with delays to avoid rate limiting
        spot_pairs = await self.fetch_spot_data()
        await asyncio.sleep(0.3)

        futures_contracts = await self.fetch_futures_data()
        await asyncio.sleep(0.3)

        deposit_assets = await self.fetch_deposit_assets()

        if not spot_pairs and not futures_contracts:
            return []

        return self.build_token_map(
            deposit_assets or [],
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
        """Fetch spot trading pairs from Asterdex Spot API."""
        data = await self._make_request(self.spot_host, "api/v1/exchangeInfo")
        if not data or not isinstance(data, dict):
            return []
        return data.get("symbols", [])

    async def fetch_futures_data(self) -> list[dict] | None:
        """Fetch futures contracts from Asterdex Futures API."""
        data = await self._make_request(self.futures_host, "fapi/v3/exchangeInfo")
        if not data or not isinstance(data, dict):
            return []
        return data.get("symbols", [])

    async def fetch_deposit_assets(self) -> list[dict] | None:
        """Fetch deposit assets with chain information.

        Uses accountType=spot and all supported chainIds to get full list of tokens.
        API docs: https://github.com/asterdex/api-docs/blob/master/aster-deposit-withdrawal.md
        """
        # Fetch for all supported chains: ETH(1), BSC(56), Arbitrum(42161), Solana(101)
        chain_ids = "1,56,42161,101"
        route = f"bapi/futures/v1/public/future/aster/deposit/assets?chainIds={chain_ids}&accountType=spot"
        data = await self._make_request(self.assets_host, route)
        if not data or not isinstance(data, dict):
            return []
        return data.get("data", [])

    def _build_chain_map(self, deposit_assets: list[dict]) -> dict[str, list[dict]]:
        """Build mapping from token name to chain information."""
        chain_map: dict[str, list[dict]] = {}

        for asset in deposit_assets:
            token_name = asset.get("name", "").upper()
            if not token_name:
                continue

            chain_id = asset.get("chainId")
            chain_name = self.CHAIN_ID_MAP.get(chain_id)
            if not chain_name:
                continue

            contract_address = asset.get("contractAddress", "")  # preserve original case

            chain_info = {
                "chain_name": chain_name,
                "contract_address": contract_address,
                "deposit_enabled": True,  # Public endpoint doesn't have this info
                "withdraw_enabled": True,
                "min_deposit_amount": None,
                "deposit_tax": 0,
                "min_withdraw_amount": None,
                "withdrawal_fee": 0,
                "withdraw_tax": 0,
            }

            if token_name not in chain_map:
                chain_map[token_name] = []

            # Avoid duplicates
            existing_chains = [c["chain_name"] for c in chain_map[token_name]]
            if chain_name not in existing_chains:
                chain_map[token_name].append(chain_info)

        return chain_map

    def build_token_map(
            self,
            deposit_assets: list[dict],
            spot_data: list[dict],
            futures_data: list[dict]
    ) -> list[dict]:
        result: list[dict] = []
        chain_map = self._build_chain_map(deposit_assets)
        spot_map = {}

        # Process spot tokens
        for token in spot_data:
            symbol = token.get("symbol", "")
            base_asset = token.get("baseAsset", "")
            quote_asset = token.get("quoteAsset", "")
            status = token.get("status", "")

            if not symbol or not base_asset:
                continue

            if quote_asset != "USDT":
                continue

            if status != "TRADING":
                continue

            # Skip leveraged tokens
            if base_asset.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            chains = chain_map.get(base_asset, [])

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
            base_asset = contract.get("baseAsset", "")
            quote_asset = contract.get("quoteAsset", "")
            status = contract.get("status", "")
            contract_type = contract.get("contractType", "")

            if not symbol or not base_asset:
                continue

            if quote_asset != "USDT":
                continue

            # Only active perpetual contracts
            if status not in ["TRADING"]:
                continue

            if contract_type != "PERPETUAL":
                continue

            # Skip leveraged tokens
            if base_asset.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            # Reuse chains from spot if available
            chains = spot_map.get(base_asset, chain_map.get(base_asset, []))

            token_entry = {
                "symbol": symbol,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "base_coin_name": base_asset,
                "contract_size": 1.0,
                "market_type": MarketType.FUTURES,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains,
                "skip_futures_suffix": True,  # Don't add _FUTURES suffix - allow merge with SPOT via price validation
            }

            result.append(token_entry)

        return result

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

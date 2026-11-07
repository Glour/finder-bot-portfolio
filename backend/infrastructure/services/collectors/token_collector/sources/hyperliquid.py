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
from settings import logger


class HyperliquidTokenParser(BaseTokenParser, SupportsTokenParsing):
    """Token parser for Hyperliquid DEX.

    API Documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api

    Endpoints (POST to https://api.hyperliquid.xyz/info):
    - Perpetuals: {"type": "meta"} -> universe with all perp contracts
    - Spot: {"type": "spotMeta"} -> tokens and universe with spot pairs
    - Prices: {"type": "allMids"} -> mid prices for all assets

    Special notes:
    - Hyperliquid is a native L1 chain, tokens are on Hyperliquid EVM
    - Uses kPEPE, kSHIB format for 1000x multiplied tokens
    - Spot pairs use format like "PURR/USDC" or "@123" (index notation)
    - Perpetuals use simple coin names like "BTC", "ETH"
    """

    API_URL = "https://api.hyperliquid.xyz/info"

    # Hyperliquid uses kPEPE for 1000 PEPE, kSHIB for 1000 SHIB etc
    MULTIPLIER_PREFIXES = {
        "k": 1000,      # kPEPE = 1000 PEPE
        "M": 1000000,   # MPEPE = 1000000 PEPE (if exists)
    }

    # Known quote currency suffixes to strip from base token names.
    # Some Hyperliquid spot tokens encode the quote in the token name itself,
    # e.g. "XAGUSDT" (silver/USDT synthetic) → base_asset should be "XAG" not "XAGUSDT"
    KNOWN_QUOTE_SUFFIXES = ("USDT", "USDC", "BTC", "ETH", "BNB")

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
            source_name="hyperliquid",
            source_type=SourceType.CEX,  # Register as CEX for spread analysis
        )
        self.repo = repo
        self.tokens_load_limit = tokens_load_limit

    async def fetch_tokens(self) -> list[dict]:
        # Fetch perpetuals and spot data
        perp_meta = await self._fetch_perp_meta()
        await asyncio.sleep(0.2)

        spot_meta = await self._fetch_spot_meta()
        await asyncio.sleep(0.2)

        all_mids = await self._fetch_all_mids()

        if not perp_meta and not spot_meta:
            return []

        return self._build_token_map(
            perp_meta or {},
            spot_meta or {},
            all_mids or {},
        )

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        data = await self.fetch_tokens()
        if not data:
            return CachedResult([], False)
        return await super().fetch_and_check_internal(f"{self.source_name}:token_map", data)

    async def _make_request(self, payload: dict) -> dict | list | None:
        """Make POST request to Hyperliquid API."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(
                    self.API_URL,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Hyperliquid API error for {payload.get('type')}: {e}")
                return None

    async def _fetch_perp_meta(self) -> dict | None:
        """Fetch perpetual contracts metadata.

        Response format:
        {
            "universe": [
                {"name": "BTC", "szDecimals": 5, "maxLeverage": 40, ...},
                {"name": "ETH", "szDecimals": 4, "maxLeverage": 25, ...},
                ...
            ]
        }
        """
        return await self._make_request({"type": "meta"})

    async def _fetch_spot_meta(self) -> dict | None:
        """Fetch spot tokens and pairs metadata.

        Response format:
        {
            "tokens": [
                {"name": "USDC", "index": 0, "evmContract": {...}, ...},
                {"name": "PURR", "index": 1, "evmContract": {...}, ...},
                ...
            ],
            "universe": [
                {"name": "PURR/USDC", "tokens": [1, 0], "index": 0, "isCanonical": true},
                {"name": "@1", "tokens": [2, 0], "index": 1, "isCanonical": false},
                ...
            ]
        }
        """
        return await self._make_request({"type": "spotMeta"})

    async def _fetch_all_mids(self) -> dict | None:
        """Fetch mid prices for all assets.

        Response format:
        {"BTC": "77500.0", "ETH": "2300.0", "@1": "0.123", ...}
        """
        return await self._make_request({"type": "allMids"})

    def _get_base_symbol(self, symbol: str) -> tuple[str, float]:
        """Extract base symbol and multiplier from Hyperliquid notation.

        Examples:
        - "kPEPE" -> ("PEPE", 1000)
        - "kSHIB" -> ("SHIB", 1000)
        - "BTC" -> ("BTC", 1)
        """
        for prefix, multiplier in self.MULTIPLIER_PREFIXES.items():
            if symbol.startswith(prefix) and len(symbol) > len(prefix):
                return symbol[len(prefix):], multiplier
        return symbol, 1.0

    def _strip_quote_suffix(self, base_name: str) -> str:
        """Strip quote currency suffix from base token name if it ends with one.

        Some Hyperliquid spot tokens encode the quote in their name:
        - "XAGUSDT" → "XAG"  (silver synthetic denominated in USDT)
        - "XAUUSDT" → "XAU"  (gold synthetic)
        - "BTCUSDT" → "BTC"  (should not normally happen, but handled)

        Only strips if the remaining base has at least 1 character.
        """
        upper = base_name.upper()
        for suffix in self.KNOWN_QUOTE_SUFFIXES:
            if upper.endswith(suffix) and len(base_name) > len(suffix):
                return base_name[:-len(suffix)]
        return base_name

    def _build_token_map(
            self,
            perp_meta: dict,
            spot_meta: dict,
            all_mids: dict,
    ) -> list[dict]:
        result: list[dict] = []
        seen_perps = set()

        # Process perpetual contracts
        perp_universe = perp_meta.get("universe", [])
        for asset in perp_universe:
            name = asset.get("name", "")
            if not name:
                continue

            # Skip delisted assets
            if asset.get("isDelisted"):
                continue

            # Check if this is a multiplied token (kPEPE, kBONK, etc.)
            base_symbol, multiplier = self._get_base_symbol(name)
            is_multiplied = multiplier > 1

            # Skip if no price available
            if name not in all_mids:
                continue

            # Symbol for perps is just the coin name (e.g., "BTC", "kPEPE")
            # Keep multiplied tokens as-is to prevent merging with regular tokens
            symbol = name

            # Get price from allMids
            # Note: for multiplied tokens (kPEPE = 1000 PEPE), keep the actual trading price
            # Do NOT divide by multiplier - this would create phantom spreads
            try:
                price = float(all_mids.get(name, 0))
            except (ValueError, TypeError):
                price = None

            # For multiplied tokens, use the original symbol as base_asset to prevent merging
            # e.g., kPEPE stays kPEPE, not PEPE
            effective_base = name if is_multiplied else base_symbol

            token_entry = {
                "symbol": symbol,
                "base_asset": effective_base,
                "quote_asset": "USDC",  # Hyperliquid uses USDC
                "base_coin_name": base_symbol,  # Use real base symbol (e.g. BTC, not kPEPE) for cross-market merge
                "contract_size": multiplier if is_multiplied else 1.0,
                "market_type": MarketType.FUTURES,
                "is_active": True,
                "source_id": self.source.id,
                "chains": [],  # Hyperliquid is its own L1, no external chains
                "price": price,  # For merge validation with other sources
                "skip_futures_suffix": True,  # Don't add _FUTURES suffix - we want cross-market arb
            }

            result.append(token_entry)
            seen_perps.add(name)

        # Process spot tokens
        tokens = spot_meta.get("tokens", [])
        spot_universe = spot_meta.get("universe", [])

        # Build token index -> token info map
        token_by_index = {t.get("index"): t for t in tokens}

        for pair in spot_universe:
            pair_name = pair.get("name", "")
            token_indices = pair.get("tokens", [])

            if not pair_name or len(token_indices) != 2:
                continue

            # For non-canonical pairs (@1, @2...), check if they have price
            pair_index = pair.get("index")
            index_key = f"@{pair_index}" if pair_index is not None else None

            # Skip pairs without price (neither pair_name nor index has price)
            if pair_name not in all_mids and (index_key is None or index_key not in all_mids):
                continue

            # Get base and quote tokens
            base_idx, quote_idx = token_indices
            base_token = token_by_index.get(base_idx, {})
            quote_token = token_by_index.get(quote_idx, {})

            base_name = base_token.get("name", "")
            quote_name = quote_token.get("name", "")

            if not base_name or not quote_name:
                continue

            # Only USDC pairs for now
            if quote_name not in ("USDC", "USDT"):
                continue

            # Get base symbol (some spot tokens might have multipliers too)
            # First strip quote suffix: "XAGUSDT" → "XAG" so it merges with other sources
            clean_base_name = self._strip_quote_suffix(base_name)
            base_symbol, _ = self._get_base_symbol(clean_base_name)

            # Build chain info from evmContract if available
            chains = []
            evm_contract = base_token.get("evmContract")
            if evm_contract and evm_contract.get("address"):
                chains.append({
                    "chain_name": "HYPE",  # Hyperliquid EVM
                    "contract_address": evm_contract["address"],  # preserve original case
                    "deposit_enabled": True,
                    "withdraw_enabled": True,
                    "min_deposit_amount": None,
                    "deposit_tax": 0,
                    "min_withdraw_amount": None,
                    "withdrawal_fee": 0,
                    "withdraw_tax": 0,
                })

            # For non-canonical pairs, use token name instead of @index
            if pair_name.startswith("@"):
                symbol = f"{base_name}{quote_name}"
            else:
                symbol = pair_name.replace("/", "")  # PURR/USDC -> PURRUSDC

            # Get price from allMids
            try:
                # Try pair_name first, then index_key
                price_str = all_mids.get(pair_name) or all_mids.get(index_key)
                price = float(price_str) if price_str else None
            except (ValueError, TypeError):
                price = None

            token_entry = {
                "symbol": symbol,
                "base_asset": base_symbol,
                "quote_asset": quote_name,
                "base_coin_name": base_symbol,
                "contract_size": None,
                "market_type": MarketType.SPOT,
                "is_active": True,
                "source_id": self.source.id,
                "chains": chains,
                "price": price,  # For merge validation with other sources
            }

            result.append(token_entry)

        logger.info(f"Hyperliquid: parsed {len([r for r in result if r['market_type'] == MarketType.FUTURES])} perps, "
                    f"{len([r for r in result if r['market_type'] == MarketType.SPOT])} spot")

        return result

    async def save_to_db(self, token_map: list[dict]) -> None:
        return await self.save_token_data_cex(token_map)

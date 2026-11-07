import httpx

from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base import BaseWSCexPriceParser
from infrastructure.services.collectors.price_collector.ws.hyperliquid import HyperliquidWSClient
from infrastructure.services.common.cache import RedisCache
from settings import logger


class HyperliquidPriceParser(BaseWSCexPriceParser):
    """Price parser for Hyperliquid DEX.

    Uses WebSocket streams to collect real-time orderbook data.
    Inherits from BaseWSCexPriceParser which handles:
    - Token collection from Redis
    - WebSocket client lifecycle management
    - Batched Redis writes
    - Connection monitoring and recovery

    Note: Hyperliquid uses same WebSocket for both perps and spot,
    differentiated by coin name format (perps: "BTC", spot: "@0" or "PURR/USDC").
    """

    API_URL = "https://api.hyperliquid.xyz/info"

    def __init__(self, repo: RequestsRepo, cache: RedisCache, market_type=MarketType.FUTURES):
        stream_key = "prices:spot" if market_type == MarketType.SPOT else "prices:futures"
        super().__init__(
            repo=repo,
            cache=cache,
            source_name="hyperliquid",
            ws_client_cls=HyperliquidWSClient,
            symbols_per_ws=50,  # Hyperliquid can handle more per connection
            market_type=market_type,
            stream_key=stream_key
        )
        # For spot: map our symbol (HFUNUSDC) -> HL format (@1)
        self._spot_symbol_map: dict[str, str] = {}
        self._spot_map_loaded = False

    async def _load_spot_symbol_map(self):
        """Load mapping from our symbols to Hyperliquid spot format (@0, @1...).

        For spot, Hyperliquid uses index notation (@0, @1) for non-canonical pairs.
        We need to map our symbol format (HFUNUSDC) to that.
        """
        if self._spot_map_loaded or self.market_type != MarketType.SPOT:
            return

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    self.API_URL,
                    json={"type": "spotMeta"},
                    headers={"Content-Type": "application/json"}
                )
                response.raise_for_status()
                data = response.json()

            tokens = data.get("tokens", [])
            universe = data.get("universe", [])

            # Build token index -> name map
            token_by_index = {t.get("index"): t.get("name", "") for t in tokens}

            for pair in universe:
                pair_name = pair.get("name", "")
                pair_index = pair.get("index")
                token_indices = pair.get("tokens", [])

                if len(token_indices) != 2:
                    continue

                base_idx, quote_idx = token_indices
                base_name = token_by_index.get(base_idx, "")
                quote_name = token_by_index.get(quote_idx, "")

                if not base_name or not quote_name:
                    continue

                # Only USDC/USDT pairs
                if quote_name not in ("USDC", "USDT"):
                    continue

                # Our symbol format: BASEUSDC (no slash)
                our_symbol = f"{base_name}{quote_name}"

                # HL format: @index for non-canonical, pair_name for canonical
                if pair_name.startswith("@"):
                    hl_format = f"@{pair_index}"
                else:
                    hl_format = pair_name  # "PURR/USDC"

                self._spot_symbol_map[our_symbol] = hl_format

            self._spot_map_loaded = True
            logger.info(f"Hyperliquid: loaded {len(self._spot_symbol_map)} spot symbol mappings")

        except Exception as e:
            logger.error(f"Failed to load Hyperliquid spot symbol map: {e}")

    async def start(self) -> None:
        """Override to load spot symbol map before starting."""
        if self.market_type == MarketType.SPOT:
            await self._load_spot_symbol_map()
        await super().start()

    def _create_ws_client(self, batch: list[str], callback, proxy_url: str | None = None) -> HyperliquidWSClient:
        """Create WebSocket client with spot symbol map."""
        return HyperliquidWSClient(
            symbols=batch,
            update_callback=callback,
            market_type=self.market_type,
            proxy_url=proxy_url,
            spot_symbol_map=self._spot_symbol_map if self.market_type == MarketType.SPOT else None
        )

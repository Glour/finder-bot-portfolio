"""Base class for DEX price parsers using WebSocket."""

import asyncio
from abc import ABC, abstractmethod
from typing import Any

from infrastructure.database.models.base import SourceType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.base import BaseParser
from infrastructure.services.collectors.structures.interfaces import SupportsPriceParsing
from infrastructure.services.common.cache import RedisCache
from settings import logger


class BaseDexPriceParser(BaseParser, SupportsPriceParsing, ABC):
    """
    Base class for DEX price collectors.

    Key differences from CEX:
    - No market_type (spot/futures), uses chains instead
    - Tokens loaded from token_on_chains by contract_address
    - WebSocket subscriptions per chain, not per symbol
    - Price data saved to dex_prices:{source} stream
    """

    def __init__(
        self,
        cache: RedisCache,
        repo: RequestsRepo,
        *,
        source_name: str,
        supported_chains: list[str],
    ):
        """
        Initialize DEX price parser.

        Args:
            cache: Redis cache instance
            repo: Database repository
            source_name: Exchange name (e.g., "gmgn")
            supported_chains: List of chain names this DEX supports (e.g., ["ETH", "BSC", "SOL"])
        """
        super().__init__(
            cache=cache,
            repo=repo,
            source_name=source_name,
            source_type=SourceType.DEX,
        )
        self.supported_chains = supported_chains
        self.ws_clients: list[Any] = []
        self._running = False

    async def load_tokens_by_chain(self) -> dict[str, list[tuple[str, str, str]]]:
        """
        Load tokens from token_on_chains filtered by supported chains.

        Returns:
            Dictionary mapping chain_name to list of (contract_address, symbol, token_id, global_token_id) tuples
            Example: {
                "ethereum": [("0xA0b86991...", "USDC", "token-uuid-1", "global-uuid-1"), ...],
                "solana": [("So11111...", "SOL", "token-uuid-2", "global-uuid-2"), ...]
            }
        """
        from sqlalchemy import text

        query_text = text("""
        SELECT DISTINCT c.name as chain_name,
                        tc.contract_address,
                        t.symbol,
                        t.id as token_id,
                        tc.global_token_id
        FROM token_chains tc
        JOIN chains c ON tc.chain_id = c.id
        JOIN tokens t ON tc.token_id = t.id
        WHERE c.name = ANY(:chains)
          AND tc.contract_address IS NOT NULL
          AND tc.contract_address != ''
        ORDER BY c.name, t.symbol
        """)

        async with self.repo.sessionmaker() as session:
            result = await session.execute(query_text, {"chains": self.supported_chains})
            rows = result.fetchall()

        tokens_by_chain = {}
        for chain_name, contract_address, symbol, token_id, global_token_id in rows:
            if chain_name not in tokens_by_chain:
                tokens_by_chain[chain_name] = []
            tokens_by_chain[chain_name].append((contract_address, symbol, token_id, str(global_token_id) if global_token_id else ""))

        # Log tokens per chain including chains with zero tokens
        chain_counts = {chain: len(tokens) for chain, tokens in tokens_by_chain.items()}
        missing_chains = [ch for ch in self.supported_chains if ch not in tokens_by_chain]
        logger.info(
            f"DEX {self.source_name} tokens by chain: {chain_counts}"
            + (f", no tokens for: {missing_chains}" if missing_chains else "")
        )

        return tokens_by_chain

    async def save_price_update(self, symbol: str, price_data: dict, timestamp: int) -> None:
        """
        Save price update to Redis stream.

        Args:
            symbol: Token symbol
            price_data: Price data dict with price, volume, etc.
            timestamp: Update timestamp in milliseconds
        """
        # Build orderbook from single price (same format as dexscreener)
        price = price_data.get("price", 0)
        volume = price_data.get("volume_24h", 0) / 100 / 2 / price if price > 0 else 0

        orderbook = {
            "asks": [[price, volume]],
            "bids": [[price, volume]]
        }

        # Save to Redis stream: dex_prices:{source_name}
        stream_key = f"dex_prices:{self.source_name}"
        data = {
            "symbol": symbol,
            "price": str(price),
            "orderbook": str(orderbook),
            "market_cap": str(price_data.get("market_cap", 0)),
            "liquidity": str(price_data.get("liquidity", 0)),
            "volume_24h": str(price_data.get("volume_24h", 0)),
            "chain": price_data.get("chain", ""),
            "contract_address": price_data.get("contract_address", ""),
            "token_id": str(price_data.get("token_id", "")),
            "global_token_id": str(price_data.get("global_token_id", "")),
            "timestamp": str(timestamp),
        }

        await self.cache.client.xadd(stream_key, data)

    async def _create_ws_client(
        self,
        chain: str,
        tokens: list[tuple[str, str, str]]
    ) -> Any:
        """
        Create WebSocket client for specific chain.

        Optional method - only needed for WebSocket-based collectors.
        REST-based collectors can override start() instead.

        Args:
            chain: Chain name (e.g., "ETH", "SOL")
            tokens: List of (contract_address, symbol, token_id) tuples for this chain

        Returns:
            WebSocket client instance
        """
        raise NotImplementedError("WebSocket client not implemented for this collector")

    async def start(self) -> None:
        """Start price collection for all supported chains."""
        await self.setup()

        if not self.source:
            raise ValueError(f"Source {self.source_name} not found in database")

        # Load tokens from DB
        tokens_by_chain = await self.load_tokens_by_chain()

        if not tokens_by_chain:
            logger.warning(
                f"No tokens found for {self.source_name} in supported chains: {self.supported_chains}"
            )
            return

        # Create WS client per chain
        self._running = True
        for chain, tokens in tokens_by_chain.items():
            if not tokens:
                logger.warning(f"No tokens for {self.source_name}/{chain}, skipping")
                continue

            try:
                client = await self._create_ws_client(chain, tokens)
                self.ws_clients.append(client)
                logger.info(f"Created WS client for {self.source_name}/{chain} with {len(tokens)} tokens")
            except Exception as e:
                logger.error(f"Failed to create WS client for {self.source_name}/{chain}: {e}")

        if not self.ws_clients:
            logger.error(f"No WS clients created for {self.source_name}")
            return

        # Start all clients
        logger.info(f"Starting {len(self.ws_clients)} WS clients for {self.source_name}")
        await asyncio.gather(
            *[client.connect() for client in self.ws_clients],
            return_exceptions=True
        )

    async def stop(self) -> None:
        """Stop all WebSocket clients."""
        self._running = False

        if not self.ws_clients:
            return

        logger.info(f"Stopping {len(self.ws_clients)} WS clients for {self.source_name}")

        await asyncio.gather(
            *[client.close() for client in self.ws_clients if hasattr(client, 'close')],
            return_exceptions=True
        )

        self.ws_clients.clear()

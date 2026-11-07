from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base import BaseWSCexPriceParser
from infrastructure.services.collectors.price_collector.ws.ascendex import AscendexWSClient
from infrastructure.services.common.cache import RedisCache


class AscendexPriceParser(BaseWSCexPriceParser):
    """Price parser for AscendEX (formerly BitMax) CEX exchange.

    Uses WebSocket streams to collect real-time orderbook data.
    Inherits from BaseWSCexPriceParser which handles:
    - Token collection from Redis
    - WebSocket client lifecycle management
    - Batched Redis writes
    - Connection monitoring and recovery
    """

    def __init__(self, repo: RequestsRepo, cache: RedisCache, market_type=MarketType.SPOT):
        stream_key = "prices:spot" if market_type == MarketType.SPOT else "prices:futures"
        super().__init__(
            repo=repo,
            cache=cache,
            source_name="ascendex",
            ws_client_cls=AscendexWSClient,
            symbols_per_ws=30,  # Conservative value due to unknown limits
            market_type=market_type,
            stream_key=stream_key
        )

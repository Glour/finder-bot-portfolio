from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base import BaseWSCexPriceParser
from infrastructure.services.collectors.price_collector.ws.bitmart import BitmartWSClient
from infrastructure.services.common.cache import RedisCache


class BitmartPriceParser(BaseWSCexPriceParser):
    """Price parser for Bitmart using WebSocket.

    Collects orderbook data from Bitmart spot and futures markets.
    """

    def __init__(self, repo: RequestsRepo, cache: RedisCache, market_type=MarketType.SPOT):
        stream_key = "prices:spot" if market_type == MarketType.SPOT else "prices:futures"
        super().__init__(
            repo=repo,
            cache=cache,
            source_name="bitmart",
            ws_client_cls=BitmartWSClient,
            symbols_per_ws=80,  # Bitmart: max 20 connections per IP, 20 topics per sub message
            market_type=market_type,
            stream_key=stream_key
        )

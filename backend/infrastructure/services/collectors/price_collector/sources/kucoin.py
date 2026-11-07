from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base import BaseWSCexPriceParser
from infrastructure.services.collectors.price_collector.ws.kucoin import KucoinWSClient
from infrastructure.services.common.cache import RedisCache


class KucoinPriceParser(BaseWSCexPriceParser):
    """Price parser for KuCoin exchange using WebSocket level2Depth5."""

    def __init__(self, repo: RequestsRepo, cache: RedisCache, market_type=MarketType.SPOT):
        stream_key = "prices:spot" if market_type == MarketType.SPOT else "prices:futures"
        super().__init__(
            repo=repo,
            cache=cache,
            source_name="kucoin",
            ws_client_cls=KucoinWSClient,
            symbols_per_ws=50,   # Spot: 50 per batch (comma-separated); Futures: 50/conn
            market_type=market_type,
            stream_key=stream_key,
        )

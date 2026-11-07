from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base import BaseWSCexPriceParser
from infrastructure.services.collectors.price_collector.ws.binance import BinanceWSClient
from infrastructure.services.common.cache import RedisCache


class BinancePriceParser(BaseWSCexPriceParser):
    """Price parser for Binance exchange using WebSocket bookTicker."""

    def __init__(self, repo: RequestsRepo, cache: RedisCache, market_type=MarketType.SPOT):
        stream_key = "prices:spot" if market_type == MarketType.SPOT else "prices:futures"
        super().__init__(
            repo=repo,
            cache=cache,
            source_name="binance",
            ws_client_cls=BinanceWSClient,
            symbols_per_ws=200,  # Binance supports up to 1024 streams per connection
            market_type=market_type,
            stream_key=stream_key,
        )

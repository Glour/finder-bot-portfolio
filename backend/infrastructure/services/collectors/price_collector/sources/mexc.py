from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base import BaseWSCexPriceParser
from infrastructure.services.collectors.price_collector.ws.mexc import MexcWSClient
from infrastructure.services.common.cache import RedisCache


class MexcPriceParser(BaseWSCexPriceParser):
    def __init__(self, repo: RequestsRepo, cache: RedisCache, market_type=MarketType.SPOT):
        stream_key = "prices:spot" if market_type == MarketType.SPOT else "prices:futures"
        super().__init__(
            repo=repo,
            cache=cache,
            source_name="mexc",
            ws_client_cls=MexcWSClient,
            symbols_per_ws=30,
            market_type=market_type,
            stream_key=stream_key
        )

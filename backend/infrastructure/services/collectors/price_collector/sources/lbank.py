from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base import BaseWSCexPriceParser
from infrastructure.services.collectors.price_collector.ws.lbank import LbankWSClient
from infrastructure.services.common.cache import RedisCache


class LbankPriceParser(BaseWSCexPriceParser):
    def __init__(self, repo: RequestsRepo, cache: RedisCache, market_type=MarketType.SPOT):
        stream_key = "prices:spot" if market_type == MarketType.SPOT else "prices:futures"
        super().__init__(
            repo=repo,
            cache=cache,
            source_name="lbank",
            ws_client_cls=LbankWSClient,
            symbols_per_ws=10,  # Reduced from 30 to prevent subscription timeouts
            market_type=market_type,
            stream_key=stream_key,
            use_proxies=True  # Enable proxy support to bypass IP blocking
        )

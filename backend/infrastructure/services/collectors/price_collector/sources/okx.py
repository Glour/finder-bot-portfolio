from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base import BaseWSCexPriceParser
from infrastructure.services.collectors.price_collector.ws.okx import OkxWSClient
from infrastructure.services.common.cache import RedisCache


class OkxPriceParser(BaseWSCexPriceParser):
    """Price parser for OKX exchange using WebSocket books5 channel."""

    def __init__(self, repo: RequestsRepo, cache: RedisCache, market_type=MarketType.SPOT):
        stream_key = "prices:spot" if market_type == MarketType.SPOT else "prices:futures"
        super().__init__(
            repo=repo,
            cache=cache,
            source_name="okx",
            ws_client_cls=OkxWSClient,
            symbols_per_ws=50,   # OKX supports large batches, 50 per WS connection
            market_type=market_type,
            stream_key=stream_key,
        )

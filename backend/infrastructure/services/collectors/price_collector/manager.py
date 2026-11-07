import asyncio

from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.sources.ascendex import AscendexPriceParser
from infrastructure.services.collectors.price_collector.sources.asterdex import AsterdexPriceParser
from infrastructure.services.collectors.price_collector.sources.bitmart import BitmartPriceParser
from infrastructure.services.collectors.price_collector.sources.gate import GatePriceParser
from infrastructure.services.collectors.price_collector.sources.gmgn_native_ws import GmgnNativeWSCollector
from infrastructure.services.collectors.price_collector.sources.hyperliquid import HyperliquidPriceParser
from infrastructure.services.collectors.price_collector.sources.lbank import LbankPriceParser
from infrastructure.services.collectors.price_collector.sources.mexc import MexcPriceParser
from infrastructure.services.collectors.price_collector.sources.bitget import BitgetPriceParser
from infrastructure.services.collectors.price_collector.sources.bybit import BybitPriceParser
from infrastructure.services.collectors.price_collector.sources.binance import BinancePriceParser
from infrastructure.services.collectors.price_collector.sources.kucoin import KucoinPriceParser
from infrastructure.services.collectors.price_collector.sources.okx import OkxPriceParser
from infrastructure.services.collectors.price_collector.sources.xt import XtPriceParser
from infrastructure.services.collectors.structures.interfaces import SupportsPriceParsing
from infrastructure.services.common.cache import RedisCache


class PriceCollectorManager:
    def __init__(
            self,
            repo: RequestsRepo,
            sources: list[SupportsPriceParsing],
            market_type: MarketType | None = None,
            scale_count: int | None = None,
    ):
        self.sources = sources
        self.market_type = market_type
        self.scale_count = scale_count
        self.repo = repo
        self._tasks: list[asyncio.Task] = []

    async def run(self):
        for source in self.sources:
            await source.setup()
            task = asyncio.create_task(source.start())
            self._tasks.append(task)

        await asyncio.gather(*self._tasks)


def get_price_collector_manager(
        repo: RequestsRepo,
        cache: RedisCache,
        market_type: MarketType | None = None,
        scale_count: int | None = None,
        source_name: str | None = None,
) -> PriceCollectorManager:
    available_sources: dict[str, type[SupportsPriceParsing]] = {
        "mexc": MexcPriceParser,
        "gate": GatePriceParser,
        "xt": XtPriceParser,
        "lbank": LbankPriceParser,
        "asterdex": AsterdexPriceParser,
        "bitmart": BitmartPriceParser,
        "hyperliquid": HyperliquidPriceParser,
        "ascendex": AscendexPriceParser,
        "bitget": BitgetPriceParser,
        "bybit": BybitPriceParser,
        "binance": BinancePriceParser,
        "kucoin": KucoinPriceParser,
        "okx": OkxPriceParser,
        "gmgn": GmgnNativeWSCollector,
    }

    if source_name is None:
        raise ValueError("SOURCE_NAME env var must be set")

    source_name = source_name.lower()
    if source_name not in available_sources:
        raise ValueError(f"Unsupported source_name '{source_name}'. Available: {list(available_sources.keys())}")

    source_class = available_sources[source_name]

    # DEX sources don't use market_type (they use chains instead)
    # Check if this is a DEX source by trying to instantiate without market_type
    try:
        import inspect
        sig = inspect.signature(source_class.__init__)
        if 'market_type' in sig.parameters:
            # CEX source - needs market_type
            source_instance = source_class(cache=cache, repo=repo, market_type=market_type)  # type: ignore
        else:
            # DEX source - doesn't need market_type
            source_instance = source_class(cache=cache, repo=repo)  # type: ignore
    except Exception as e:
        raise ValueError(f"Failed to instantiate {source_name}: {e}")

    return PriceCollectorManager(
        repo=repo,
        sources=[source_instance],
        market_type=market_type,
        scale_count=scale_count
    )

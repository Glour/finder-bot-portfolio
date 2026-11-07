import asyncio
import time

from fastapi import Depends

from infrastructure.database.requests import RequestsRepo, get_database_repo
from infrastructure.services.collectors.structures.interfaces import SupportsTokenParsing
from infrastructure.services.collectors.token_collector.supports import chain_normalizer_instance
from infrastructure.services.collectors.token_collector.sources.ascendex import AscendexTokenParser
from infrastructure.services.collectors.token_collector.sources.asterdex import AsterdexTokenParser
from infrastructure.services.collectors.token_collector.sources.bitmart import BitmartTokenParser
from infrastructure.services.collectors.token_collector.sources.gate import GateTokenParser
from infrastructure.services.collectors.token_collector.sources.hyperliquid import HyperliquidTokenParser
from infrastructure.services.collectors.token_collector.sources.lbank import LbankTokenParser
from infrastructure.services.collectors.token_collector.sources.mexc import MexcTokenParser
from infrastructure.services.collectors.token_collector.sources.bitget import BitgetTokenParser
from infrastructure.services.collectors.token_collector.sources.bybit import BybitTokenParser
from infrastructure.services.collectors.token_collector.sources.binance import BinanceTokenParser
from infrastructure.services.collectors.token_collector.sources.kucoin import KucoinTokenParser
from infrastructure.services.collectors.token_collector.sources.okx import OkxTokenParser
from infrastructure.services.collectors.token_collector.sources.xt import XtTokenParser
from infrastructure.services.common.cache import RedisCache, get_redis_cache
from settings import logger


class TokenCollectorManager:
    def __init__(self, sources: list[SupportsTokenParsing], interval: float = 1.0):
        self.sources = sources
        self.interval = interval

    async def run(self):
        import gc
        import psutil
        import os
        
        first_run = True
        process = psutil.Process(os.getpid())

        while True:
            # MEMORY FIX: Monitor memory usage at start of each cycle
            memory_start = process.memory_info().rss / 1024 / 1024  # MB
            
            start = time.perf_counter()

            try:
                if first_run:
                    for source in self.sources:
                        await self._collect_source(source)
                    first_run = False
                else:
                    tasks = [self._collect_source(source) for source in self.sources]
                    await asyncio.gather(*tasks)

                elapsed = time.perf_counter() - start
                
                # MEMORY FIX: Force garbage collection after each cycle
                gc.collect()
                
                # MEMORY FIX: Monitor memory usage after processing
                memory_end = process.memory_info().rss / 1024 / 1024  # MB
                memory_growth = memory_end - memory_start
                
                delay = max(0.0, self.interval - elapsed)
                logger.info(
                    f"Collected tokens in {elapsed:.2f} seconds. "
                    f"Memory: {memory_end:.1f}MB ({memory_growth:+.1f}MB). "
                    f"Next run in {delay:.2f} seconds."
                )
                
                # MEMORY WARNING: Alert if significant memory growth
                if memory_growth > 50:  # More than 50MB growth in one cycle
                    logger.warning(f"⚠️ High memory growth in collection cycle: {memory_growth:.1f}MB")
                
                # MEMORY CRITICAL: Alert if total memory usage is very high
                if memory_end > 2000:  # More than 2GB
                    logger.error(f"🚨 CRITICAL: High memory usage: {memory_end:.1f}MB - potential memory leak!")
                
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error in collection cycle: {e}", exc_info=True)
                # Still force cleanup on error
                gc.collect()
                await asyncio.sleep(60)  # Wait 1 minute before retry

    @staticmethod
    async def _collect_source(source: SupportsTokenParsing):
        try:
            total_start = time.perf_counter()
            await source.setup()

            result = await source.fetch_and_check()
            if result.updated and result.data:
                await source.save_to_db(result.data)

            total_elapsed = time.perf_counter() - total_start
            logger.info(f"{source.source_appellation()} - total time {total_elapsed:.2f}s")

        except Exception as e:
            logger.error(f"Error collecting {source.source_appellation()}: {e}", exc_info=True)


def get_token_collector_manager(
        repo: RequestsRepo = Depends(get_database_repo),
        cache: RedisCache = Depends(get_redis_cache),
        interval: float | None = None
) -> TokenCollectorManager:
    sources: list[SupportsTokenParsing] = [
        MexcTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        GateTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        XtTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        LbankTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        AsterdexTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        BitmartTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        HyperliquidTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        AscendexTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        BitgetTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        BybitTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        BinanceTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        KucoinTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        OkxTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
        # DexScreenerTokenParser(repo=repo, cache=cache, chain_normalizer=chain_normalizer_instance),
    ]
    return TokenCollectorManager(sources=sources, interval=interval)

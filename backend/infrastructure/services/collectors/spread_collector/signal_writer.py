"""
Background worker for persisting arbitrage signals to PostgreSQL.

Design goals:
- Zero impact on analyzer speed (non-blocking queue put)
- Batched writes (one INSERT per flush, not per signal)
- Graceful degradation (drops signals if queue full, logs errors without crashing)
- Token blacklist filtering (before enqueue)
"""
import asyncio
import json
import time
from datetime import datetime, UTC

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from settings import logger


class SignalHistoryWriter:
    """Background worker that batches signal writes to PostgreSQL."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker,
        repo=None,
        flush_interval: float = 30.0,
        max_batch: int = 500,
        max_queue: int = 10000,
        blacklist_refresh_interval: float = 600.0,
    ):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue)
        self._sessionmaker = sessionmaker
        self._repo = repo
        self._flush_interval = flush_interval
        self._max_batch = max_batch
        self._blacklist_refresh_interval = blacklist_refresh_interval
        self._task = None
        self._stats = {"enqueued": 0, "flushed": 0, "dropped": 0, "errors": 0, "blacklisted": 0}
        
        # Token blacklist cache (global_token_id set)
        self._blacklisted_gt_ids: set[str] = set()
        self._blacklist_last_refresh: float = 0

    def enqueue(self, result: dict) -> None:
        """
        Non-blocking put into queue. Drops signal if queue full or blacklisted.

        This is called from the hot path (process_result).
        Must never block or raise.
        """
        try:
            # Check token blacklist (non-blocking in-memory check)
            global_token_id = str(result.get("global_token_id", ""))
            if global_token_id in self._blacklisted_gt_ids:
                self._stats["blacklisted"] += 1
                return
            
            row = self._extract(result)
            self._queue.put_nowait(row)
            self._stats["enqueued"] += 1
        except asyncio.QueueFull:
            self._stats["dropped"] += 1
        except Exception:
            self._stats["errors"] += 1

    def start(self) -> None:
        """Start background flush worker as asyncio task."""
        self._task = asyncio.create_task(self._flush_loop())
        logger.info(
            f"SignalHistoryWriter started (flush every {self._flush_interval}s, "
            f"max batch {self._max_batch}, queue size {self._queue.maxsize})"
        )

    async def _flush_loop(self) -> None:
        """Periodically drain queue, write to DB, and refresh blacklist."""
        # Initial blacklist load
        if self._repo:
            try:
                await self._refresh_blacklist()
            except Exception as e:
                logger.error(f"SignalHistoryWriter: initial blacklist load failed: {e}")
        
        while True:
            try:
                await asyncio.sleep(self._flush_interval)
                await self._flush()
                
                # Refresh blacklist periodically
                if self._repo:
                    now = time.time()
                    if now - self._blacklist_last_refresh >= self._blacklist_refresh_interval:
                        await self._refresh_blacklist()
            except asyncio.CancelledError:
                # Final flush on shutdown
                await self._flush()
                break
            except Exception as e:
                logger.error(f"SignalHistoryWriter flush error: {e}", exc_info=True)
                self._stats["errors"] += 1

    async def _flush(self) -> None:
        """Drain queue and batch insert into PostgreSQL."""
        batch = []
        while not self._queue.empty() and len(batch) < self._max_batch:
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        if not batch:
            return

        start = time.perf_counter()
        try:
            await self._bulk_insert(batch)
            elapsed = time.perf_counter() - start
            self._stats["flushed"] += len(batch)
            logger.info(
                f"SignalHistoryWriter: flushed {len(batch)} signals in {elapsed:.2f}s "
                f"(total: {self._stats['flushed']}, dropped: {self._stats['dropped']})"
            )
        except Exception as e:
            elapsed = time.perf_counter() - start
            self._stats["errors"] += 1
            logger.error(
                f"SignalHistoryWriter: bulk insert failed ({len(batch)} signals, {elapsed:.2f}s): {e}",
                exc_info=True,
            )

    async def _refresh_blacklist(self) -> None:
        """Refresh token blacklist from database."""
        try:
            blacklist = await self._repo.token_blacklist.get_all()
            
            # Filter expired entries
            now = datetime.now(UTC).replace(tzinfo=None)
            valid_blacklist = [
                t for t in blacklist
                if not t.expires_at or t.expires_at > now
            ]
            
            # Build set of blacklisted global_token_ids
            blacklisted_gt_ids = {
                str(t.global_token_id)
                for t in valid_blacklist
                if t.global_token_id
            }
            
            self._blacklisted_gt_ids = blacklisted_gt_ids
            self._blacklist_last_refresh = time.time()
            
            logger.info(
                f"SignalHistoryWriter: refreshed blacklist ({len(blacklisted_gt_ids)} tokens, "
                f"filtered {self._stats['blacklisted']} signals so far)"
            )
        except Exception as e:
            logger.error(f"SignalHistoryWriter: blacklist refresh failed: {e}", exc_info=True)
            raise

    async def _bulk_insert(self, batch: list[dict]) -> None:
        """Raw SQL batch insert for maximum performance."""
        query = text("""
            INSERT INTO signal_history (
                global_token_id, symbol, buy_from, sell_to,
                buy_price, sell_price, spread_percent, profit,
                profit_before_fee, applied_fee, volume,
                level, analyzed_at, details,
                buy_base_asset, sell_base_asset, telegram_link
            ) VALUES (
                :global_token_id, :symbol, :buy_from, :sell_to,
                :buy_price, :sell_price, :spread_percent, :profit,
                :profit_before_fee, :applied_fee, :volume,
                :level, :analyzed_at, :details,
                :buy_base_asset, :sell_base_asset, :telegram_link
            )
        """)

        async with self._sessionmaker() as session:
            await session.execute(query, batch)
            await session.commit()

    @staticmethod
    def _extract(result: dict) -> dict:
        """Extract minimal data from result dict for DB storage."""
        # Compact chain info (max 5 chains)
        chains = []
        for c in result.get("chains", [])[:5]:
            chains.append({
                "name": c.get("chain_name", ""),
                "addr": c.get("contract_address", ""),
                "w_fee": c.get("withdrawal_fee_in_usdt", 0),
                "deposit": c.get("deposit_enabled"),
                "withdraw": c.get("withdraw_enabled"),
            })

        # Extract source timestamps from tickers_data
        tickers = result.get("tickers_data", {})
        buyer_get_at = None
        seller_get_at = None

        buy_from = result.get("buy_from", "")
        sell_to = result.get("sell_to", "")

        if buy_from and "_" in buy_from:
            btype, bsource = buy_from.split("_", 1)
            seller_info = tickers.get(btype, {}).get(bsource, {})
            seller_get_at = seller_info.get("get_at")

        if sell_to and "_" in sell_to:
            stype, ssource = sell_to.split("_", 1)
            buyer_info = tickers.get(stype, {}).get(ssource, {})
            buyer_get_at = buyer_info.get("get_at")

        details = {
            "chains": chains,
            "seller_get_at": seller_get_at,
            "buyer_get_at": buyer_get_at,
        }

        # Extract base_asset from tickers_data
        buy_base_asset = None
        sell_base_asset = None
        
        if buy_from and "_" in buy_from:
            btype, bsource = buy_from.split("_", 1)
            seller_info = tickers.get(btype, {}).get(bsource, {})
            buy_base_asset = seller_info.get("base_asset")
        
        if sell_to and "_" in sell_to:
            stype, ssource = sell_to.split("_", 1)
            buyer_info = tickers.get(stype, {}).get(ssource, {})
            sell_base_asset = buyer_info.get("base_asset")

        return {
            "global_token_id": str(result["global_token_id"]),
            "symbol": result.get("symbol", "")[:50],
            "buy_from": buy_from[:100],
            "sell_to": sell_to[:100],
            "buy_price": float(result.get("buy_price", 0)),
            "sell_price": float(result.get("sell_price", 0)),
            "spread_percent": float(result.get("spread_percent", 0)),
            "profit": float(result.get("profit", 0)),
            "profit_before_fee": float(result.get("profit_before_fee", 0)) if result.get("profit_before_fee") is not None else None,
            "applied_fee": float(result.get("applied_fee", 0)),
            "volume": float(result.get("volume", 0)) if result.get("volume") is not None else None,
            "level": str(result.get("level", ""))[:20],
            "analyzed_at": int(result.get("analyzed_at", 0)),
            "details": json.dumps(details),
            "buy_base_asset": buy_base_asset[:50] if buy_base_asset else None,
            "sell_base_asset": sell_base_asset[:50] if sell_base_asset else None,
            "telegram_link": result.get("telegram_link"),
        }

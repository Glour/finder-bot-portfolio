"""GMGN WebSocket collector using native curl_cffi for TLS fingerprint impersonation.

Architecture: Single WebSocket connection subscribes to token_stat for all chains/tokens.
GMGN supports multi-chain on one connection.
"""

import asyncio
import json
import time
import uuid
from typing import Any
from urllib.parse import urlencode

from curl_cffi.requests import AsyncSession

from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base_dex import BaseDexPriceParser
from infrastructure.services.collectors.structures.maps import GMGN_CHAIN_MAP
from infrastructure.services.common.cache import RedisCache
from settings import logger


# Protocol constants
WS_URL = "wss://gmgn.ai/ws"
APP_VERSION = "20260202-10623-98faccb"
IMPERSONATE_BROWSER = "chrome124"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 OPR/126.0.0.0",
    "Origin": "https://gmgn.ai",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
}


class GmgnNativeWSCollector(BaseDexPriceParser):
    """GMGN WebSocket collector with single connection and token_stat subscriptions."""

    MIN_POOL_LIQUIDITY = 50_000
    RECONNECT_DELAYS = [5, 10, 20, 40, 60]
    RECV_TIMEOUT = 15  # seconds — triggers heartbeat on timeout
    BATCH_INTERVAL = 2.0  # Send to Redis every 2 seconds (like MEXC)

    def __init__(self, cache: RedisCache, repo: RequestsRepo):
        supported_chains = list(GMGN_CHAIN_MAP.keys())
        super().__init__(cache=cache, repo=repo, source_name="gmgn", supported_chains=supported_chains)
        self._running = False
        self._token_map: dict[tuple[str, str], tuple[str, int, str]] = {}
        self._subscription_list: list[dict] = []  # [{chain, addresses}, ...]
        self._chain_stats: dict[str, dict] = {}
        self._total_updates = 0
        self._last_logged_milestone = 0

        # Batching like MEXC - keep latest update per token
        self._pending_updates: dict[str, tuple[str, dict, int]] = {}  # symbol -> (symbol, price_data, timestamp)
        self._pending_lock = asyncio.Lock()

    @staticmethod
    def _build_connection_url() -> str:
        """Build WebSocket connection URL with query parameters."""
        params = {
            "device_id": str(uuid.uuid4()),
            "fp_did": uuid.uuid4().hex,
            "client_id": f"gmgn_web_{APP_VERSION}",
            "from_app": "gmgn",
            "app_ver": APP_VERSION,
            "tz_name": "Europe/Paris",
            "tz_offset": "3600",
            "app_lang": "en-US",
            "os": "web",
            "worker": "0",
            "uuid": uuid.uuid4().hex[:16],
            "reconnect": "0",
        }
        return f"{WS_URL}?{urlencode(params)}"

    async def start(self) -> None:
        """Main entry point — load tokens and start single WS connection."""
        await self.setup()

        if not self.source:
            raise ValueError(f"Source {self.source_name} not found in database")

        tokens_by_chain = await self.load_tokens_by_chain()

        # Build token map and subscription list
        # Group addresses by chain for efficient subscription
        addresses_by_chain = {}

        for chain_name, tokens in tokens_by_chain.items():
            gmgn_chain = GMGN_CHAIN_MAP.get(chain_name)
            if not gmgn_chain:
                logger.warning(f"GMGN WS: Skipping chain '{chain_name}' - not in GMGN_CHAIN_MAP")
                continue

            self._chain_stats[gmgn_chain] = {
                "messages": 0,
                "saved": 0,
                "skipped_low_liq": 0,
                "skipped_no_match": 0,
                "errors": 0,
            }

            addresses_by_chain[gmgn_chain] = []

            for addr, symbol, token_id, global_token_id in tokens:
                # Use lowercase key for lookup (GMGN may return any case)
                key = (gmgn_chain, addr.lower())
                # Store original case address for subscriptions and links
                self._token_map[key] = (symbol, token_id, addr, global_token_id)
                # Collect addresses for this chain
                addresses_by_chain[gmgn_chain].append(addr)

        # Build subscription list: each token as separate object (deduplicated)
        # Format from test_gmgn_ws.py: [{"chain": "sol", "addresses": "addr1"}, {"chain": "sol", "addresses": "addr2"}, ...]
        seen = set()
        for gmgn_chain, addresses in addresses_by_chain.items():
            for addr in addresses:
                # Deduplicate to prevent memory leaks
                key = (gmgn_chain, addr.lower())
                if key not in seen:
                    self._subscription_list.append({"chain": gmgn_chain, "addresses": addr})
                    seen.add(key)

        total_tokens = len(self._token_map)
        total_chains = len(self._chain_stats)
        total_subscriptions = len(self._subscription_list)

        # Count subscriptions per chain
        subs_per_chain = {}
        for sub in self._subscription_list:
            chain = sub["chain"]
            subs_per_chain[chain] = subs_per_chain.get(chain, 0) + 1

        subs_summary = ", ".join(f"{ch}:{cnt}" for ch, cnt in sorted(subs_per_chain.items()))
        logger.info(
            f"GMGN WS: Loaded {total_tokens} unique tokens from {total_chains} chains, "
            f"subscribing to {total_subscriptions} tokens [{subs_summary}]"
        )

        self._running = True

        # Start batch sender task (like MEXC)
        asyncio.create_task(self._batch_sender())

        # Start monitoring task
        asyncio.create_task(self._monitor_stats())

        # Split subscription list into groups by chain to avoid GMGN per-connection limits.
        # Testing showed that when >750 mixed-chain tokens are sent in one message,
        # GMGN silently drops later chains (SOL/ETH get no data after BASE+BSC).
        # Fix: open separate WS connections per chain group, each with ≤700 tokens.
        MAX_PER_CONNECTION = 700
        subs_by_chain: dict[str, list] = {}
        for sub in self._subscription_list:
            ch = sub["chain"]
            subs_by_chain.setdefault(ch, []).append(sub)

        # Build connection groups so no single connection exceeds the limit
        connection_groups: list[list] = []
        current_group: list = []
        for ch, subs in subs_by_chain.items():
            if len(current_group) + len(subs) > MAX_PER_CONNECTION and current_group:
                connection_groups.append(current_group)
                current_group = []
            current_group.extend(subs)
        if current_group:
            connection_groups.append(current_group)

        if len(connection_groups) > 1:
            chains_in_groups = []
            for g in connection_groups:
                c = {}
                for s in g:
                    c[s["chain"]] = c.get(s["chain"], 0) + 1
                chains_in_groups.append(c)
            logger.info(
                f"GMGN WS: Splitting into {len(connection_groups)} connections "
                f"to avoid per-connection limit: {chains_in_groups}"
            )
            await asyncio.gather(*[self._connection_loop(subs=group) for group in connection_groups])
        else:
            await self._connection_loop()

    async def _connection_loop(self, subs: list | None = None) -> None:
        """Single WS connection with reconnect logic.

        Args:
            subs: Subscription list for this connection. If None, uses self._subscription_list.
        """
        subscription_list = subs if subs is not None else self._subscription_list
        # Identify chains in this connection for logging
        chain_label = ", ".join(sorted({s["chain"] for s in subscription_list}))
        reconnect_attempt = 0

        while self._running:
            session = None
            ws = None
            try:
                url = self._build_connection_url()
                session = AsyncSession(impersonate=IMPERSONATE_BROWSER)
                ws = await session.ws_connect(url, headers=HEADERS)
                logger.info(f"GMGN WS [{chain_label}]: Connected")

                # Subscribe to new_pair_update for chains in this connection
                chains_in_group = sorted({s["chain"] for s in subscription_list})
                new_pair_msg = json.dumps({
                    "action": "subscribe",
                    "channel": "new_pair_update",
                    "f": "w",
                    "id": uuid.uuid4().hex[:16],
                    "data": [{"chain": chain} for chain in chains_in_group],
                })
                await ws.send(new_pair_msg)
                logger.info(f"GMGN WS [{chain_label}]: Subscribed to new_pair_update for {len(chains_in_group)} chains")

                # Subscribe to token_stat for this group's tokens
                total_subs = len(subscription_list)
                sub_msg = json.dumps({
                    "action": "subscribe",
                    "channel": "token_stat",
                    "f": "w",
                    "id": uuid.uuid4().hex[:16],
                    "data": subscription_list,
                })
                await ws.send(sub_msg)
                logger.info(f"GMGN WS [{chain_label}]: Subscribed to token_stat for {total_subs} tokens")

                # NOTE: Do NOT clear subscription_list - it's needed for reconnects!
                reconnect_attempt = 0

                # Message loop
                await self._recv_loop(ws)

            except Exception as e:
                if not self._running:
                    break
                delay = self.RECONNECT_DELAYS[min(reconnect_attempt, len(self.RECONNECT_DELAYS) - 1)]
                reconnect_attempt += 1
                logger.warning(f"GMGN WS [{chain_label}]: Error: {e}. Reconnecting in {delay}s (attempt {reconnect_attempt})")
                await asyncio.sleep(delay)
            finally:
                if ws:
                    try:
                        await ws.close()
                    except Exception:
                        pass
                if session:
                    try:
                        await session.close()
                    except Exception:
                        pass

    async def _recv_loop(self, ws) -> None:
        """Receive messages with heartbeat on timeout."""
        logger.info("GMGN WS: Entering recv_loop")
        last_heartbeat = time.time()

        while self._running:
            # Check if we need to send heartbeat
            if time.time() - last_heartbeat > self.RECV_TIMEOUT:
                hb = json.dumps({"action": "heartbeat", "client_ts": int(time.time() * 1000)})
                await ws.send(hb)
                logger.debug(f"GMGN WS: Heartbeat sent (last message {time.time() - last_heartbeat:.1f}s ago)")
                last_heartbeat = time.time()

            try:
                # Non-blocking recv with short timeout
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                last_heartbeat = time.time()  # Reset heartbeat timer
            except asyncio.TimeoutError:
                # No message in 1s - continue loop to check heartbeat
                continue

            # curl_cffi returns (data, opcode) tuple
            if isinstance(raw, tuple):
                raw = raw[0]

            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"GMGN WS: Failed to decode message: {raw[:100] if raw else 'empty'}")
                continue

            if not isinstance(msg, dict):
                logger.warning(f"GMGN WS: Message is not dict: {type(msg)}")
                continue

            channel = msg.get("channel", "")

            # Skip ack and heartbeat silently
            if channel in ("ack", "heartbeat"):
                continue

            data = msg.get("data", [])
            if not isinstance(data, list) or not data:
                logger.warning(f"GMGN WS: Empty or invalid data for channel '{channel}'")
                continue

            self._total_updates += 1

            if channel == "token_stat":
                await self._handle_token_stat(data)
            elif channel == "new_pair_update":
                # Log only new pairs count (rare event)
                logger.debug(f"GMGN WS: new_pair_update with {len(data)} new pairs")
            else:
                logger.warning(f"GMGN WS: Unknown channel '{channel}' with {len(data)} items")

    async def _handle_token_stat(self, data: list[dict]) -> None:
        """Handle token_stat messages: {"n":"sol","a":"addr","p":"0.001",...}."""
        for item in data:
            if not isinstance(item, dict):
                continue

            chain = item.get("n", "")
            address_raw = item.get("a", "")
            address_lower = address_raw.lower()
            price = self._safe_float(item.get("p"))

            if not chain or not address_lower or price <= 0:
                continue

            # Skip chains we didn't subscribe to (shouldn't happen)
            if chain not in self._chain_stats:
                continue

            self._chain_stats[chain]["messages"] += 1

            key = (chain, address_lower)
            if key not in self._token_map:
                self._chain_stats[chain]["skipped_no_match"] += 1
                continue

            symbol, token_id, address_original, global_token_id = self._token_map[key]
            liquidity = self._safe_float(item.get("lq", 0))

            # DEBUG: Log very small prices (2026-02-20) - potential precision issues
            # Extended to log all chains and include full item for diagnosis
            if price < 1e-9:
                logger.warning(
                    f"GMGN tiny price [{chain}]: symbol={symbol} | raw_p={item.get('p')} | "
                    f"parsed={price:.6e} | addr={address_lower[:10]}... | liq={liquidity} | "
                    f"full_item={item}"
                )

            if 0 < liquidity < self.MIN_POOL_LIQUIDITY:
                if chain in self._chain_stats:
                    self._chain_stats[chain]["skipped_low_liq"] += 1
                continue

            # Add to pending batch (like MEXC) - only latest update per symbol
            price_update = {
                "price": price,
                "volume_24h": self._safe_float(item.get("v24h", 0)),
                "market_cap": self._safe_float(item.get("mc", 0)),
                "liquidity": liquidity,
                "chain": chain,
                "contract_address": address_original,
                "token_id": token_id,
                "global_token_id": global_token_id,
            }
            timestamp = int(time.time() * 1000)

            async with self._pending_lock:
                self._pending_updates[symbol] = (symbol, price_update, timestamp)

            if chain in self._chain_stats:
                self._chain_stats[chain]["saved"] += 1

    async def _batch_sender(self) -> None:
        """Periodically send pending updates to Redis in batches (like MEXC)."""
        while self._running:
            await asyncio.sleep(self.BATCH_INTERVAL)

            async with self._pending_lock:
                if not self._pending_updates:
                    continue

                batch = list(self._pending_updates.values())
                self._pending_updates.clear()

            # Send batch to Redis concurrently
            try:
                tasks = [
                    self.save_price_update(symbol, price_data, timestamp)
                    for symbol, price_data, timestamp in batch
                ]
                await asyncio.gather(*tasks, return_exceptions=True)
                logger.debug(f"GMGN WS: Sent batch of {len(batch)} updates to Redis")
            except Exception as e:
                logger.error(f"GMGN WS: Batch send error: {e}")

    async def _monitor_stats(self) -> None:
        """Log stats every 60 seconds with prices/min calculation."""
        await asyncio.sleep(60)

        while self._running:
            # Calculate totals for current minute
            total_saved = sum(s["saved"] for s in self._chain_stats.values())
            total_messages = sum(s["messages"] for s in self._chain_stats.values())
            total_skipped_liq = sum(s["skipped_low_liq"] for s in self._chain_stats.values())
            total_skipped_match = sum(s["skipped_no_match"] for s in self._chain_stats.values())

            # Build chain summary
            parts = []
            for chain, s in sorted(self._chain_stats.items()):
                if s["messages"] > 0:
                    parts.append(f"{chain}:{s['saved']}/{s['messages']}")
            chain_summary = ", ".join(parts) if parts else "no data"

            # Log with prices/min
            logger.info(
                f"GMGN WS monitor: saved={total_saved} ({total_saved} prices/min), msgs={total_messages}, "
                f"skip_liq={total_skipped_liq}, skip_match={total_skipped_match} | [{chain_summary}]"
            )

            # Reset counters for next minute
            for chain_stats in self._chain_stats.values():
                chain_stats["messages"] = 0
                chain_stats["saved"] = 0
                chain_stats["skipped_low_liq"] = 0
                chain_stats["skipped_no_match"] = 0
                chain_stats["errors"] = 0

            await asyncio.sleep(60)

    async def stop(self) -> None:
        """Stop WS connection and log final stats."""
        self._running = False

        # Flush remaining updates
        async with self._pending_lock:
            if self._pending_updates:
                batch = list(self._pending_updates.values())
                self._pending_updates.clear()
                try:
                    tasks = [
                        self.save_price_update(symbol, price_data, timestamp)
                        for symbol, price_data, timestamp in batch
                    ]
                    await asyncio.gather(*tasks, return_exceptions=True)
                    logger.info(f"GMGN WS: Flushed {len(batch)} pending updates on stop")
                except Exception as e:
                    logger.error(f"GMGN WS: Error flushing on stop: {e}")

        total_saved = sum(s["saved"] for s in self._chain_stats.values())
        total_errors = sum(s["errors"] for s in self._chain_stats.values())
        logger.info(
            f"GMGN WS: Stopped. {self._total_updates} total updates, "
            f"{total_saved} saved, {total_errors} errors"
        )

        for chain, s in sorted(self._chain_stats.items()):
            logger.info(
                f"  {chain}: msgs={s['messages']} saved={s['saved']} "
                f"skip_liq={s['skipped_low_liq']} skip_match={s['skipped_no_match']} err={s['errors']}"
            )

    @staticmethod
    def _safe_float(value: Any) -> float:
        """Safely convert value to float, returning 0.0 on error."""
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

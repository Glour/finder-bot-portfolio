import asyncio
import time
from collections import defaultdict
from datetime import datetime, timezone, UTC, timedelta

from aiogram.utils.markdown import hlink, hcode, hbold

from bot.services.broadcaster import send_message
from infrastructure.services.bots import dev_bot, prod_bot, en_prod_bot, tech_prod_bot
from infrastructure.services.collectors.spread_collector.config import SignalLevelType, DEFAULT_BROADCASTER_CONFIG
from infrastructure.services.collectors.structures.normalizer import safe_float
from infrastructure.services.collectors.structures.utils import generate_market_link, format_significant, \
    generate_asset_link
from settings import settings


class ArbitrageMessenger:
    def __init__(self, chain_normalizer, config=None, signal_writer=None):
        self.chain_normalizer = chain_normalizer
        self.config = config or DEFAULT_BROADCASTER_CONFIG
        self.signal_writer = signal_writer

    @staticmethod
    def make_market_link(result: dict, market_type: str, source_name: str) -> str:
        symbols_map = result.get("symbols", {})
        key = f"{market_type}_{source_name}"
        ticker = symbols_map.get(key, "")

        if market_type == "dex":
            chains = [
                (c["chain_name"].lower(), c["contract_address"])
                for c in result.get("chains", [])
                if c.get("token_source_name") == source_name and c.get("contract_address")
            ]
            if not chains:
                return ""
            chain, contract = chains[0]
            return generate_market_link(market_type, ticker, contract, chain, source_name)

        return generate_market_link(market_type, ticker, "", "", source_name)

    def get_realizable_chains(self, seller_info: dict, buyer_info: dict) -> tuple[bool, set[str]]:
        seller_chains = seller_info.get("chains", [])
        buyer_chains = buyer_info.get("chains", [])

        realizable = set()

        # цепочка реализуема, если вывод seller и депозит buyer открыты
        for s in seller_chains:
            for b in buyer_chains:
                if s["chain_name"].lower() == b["chain_name"].lower():
                    if s.get("withdraw_enabled") and b.get("deposit_enabled"):
                        realizable.add(self.chain_normalizer.normalize_to_signal(s["chain_name"]).upper())

        return len(realizable) > 0, realizable

    def format_status(
            self,
            source_info: dict,
            realizable_chains: set[str],
            intersecting_chains: set[str] = None
    ) -> str:
        base_asset = source_info.get("base_asset", "")
        source_name = source_info.get("source_name", "").lower()

        intersecting_chains = intersecting_chains or set()

        chains = []
        for ch in source_info.get("chains", []):
            chain_name = self.chain_normalizer.normalize_to_signal(ch['chain_name']).upper()
            chains.append({
                "name": chain_name,
                "deposit_enabled": ch.get("deposit_enabled"),
                "withdraw_enabled": ch.get("withdraw_enabled"),
                "deposit_enabled_changed_at": ch.get("deposit_enabled_changed_at"),
                "withdraw_enabled_changed_at": ch.get("withdraw_enabled_changed_at"),
                "deposit_link": generate_asset_link(source_name, base_asset, "deposit"),
                "withdraw_link": generate_asset_link(source_name, base_asset, "withdraw"),
                "is_realizable": chain_name in realizable_chains,
                "is_intersecting": (chain_name in intersecting_chains and chain_name not in realizable_chains),
                "withdrawal_fee_in_usdt": ch.get("withdrawal_fee_in_usdt", 0),
                "cex_withdrawal_fee": ch.get("cex_withdrawal_fee", 0),
                "cex_deposit_fee": ch.get("cex_deposit_fee", 0),
                "total_tax_fee_in_token": ch.get("total_tax_fee_in_token", float("inf")),
                "withdrawal_fee": ch.get("withdrawal_fee", 0),
            })

        # Сортировка:
        # 1. реализуемые
        # 2. пересекающиеся
        # 3. остальные
        # внутри группы - по возрастанию комиссии
        def sort_key(chain):
            if chain["is_realizable"]:
                group = 0
            elif chain["is_intersecting"]:
                group = 1
            else:
                group = 2
            return group, chain["total_tax_fee_in_token"]

        chains.sort(key=sort_key)
        # выводим только первые 3 сети
        chains = chains[:3]

        def _fmt_changed(ts) -> str:
            """Format elapsed time since deposit/withdraw status changed. Returns e.g. '3d', '5h', '12m'."""
            if not ts:
                return ""
            try:
                if isinstance(ts, str):
                    # ISO string from msgpack deserialization — parse it
                    ts = datetime.fromisoformat(ts)
                if not isinstance(ts, datetime):
                    return ""
                now = datetime.now()
                delta = now - ts
                total_seconds = int(delta.total_seconds())
                if total_seconds < 0:
                    return ""
                if total_seconds < 3600:
                    return f" ({total_seconds // 60}m)"
                if total_seconds < 86400:
                    return f" ({total_seconds // 3600}h)"
                return f" ({total_seconds // 86400}d)"
            except Exception:
                return ""

        lines = []
        for c in chains:
            d_icon = "🟢" if c["deposit_enabled"] else "🔴"
            w_icon = "🟢" if c["withdraw_enabled"] else "🔴"
            d_age = _fmt_changed(c.get("deposit_enabled_changed_at"))
            w_age = _fmt_changed(c.get("withdraw_enabled_changed_at"))
            fee_str = ""
            if c.get("total_tax_fee_in_token") is not None and c["total_tax_fee_in_token"] != float("inf"):
                fee_str = f" Fee: {safe_float(c.get('withdrawal_fee'))} ({safe_float(c.get('withdrawal_fee_in_usdt')):.2f}$)"

            lines.append(
                f"┞ {c['name']} │ "
                f"{hlink(f'D:{d_icon}{d_age}', c['deposit_link'])} {hlink(f'W:{w_icon}{w_age}', c['withdraw_link'])}{fee_str}"
            )

        return "\n".join(lines) if lines else ""

    def format_chains(self, result: dict) -> str:
        unique = {
            (c.get("chain_name", "").lower(), c.get("contract_address", "").lower())
            for c in result.get("chains", [])
            if c.get("chain_name") and c.get("contract_address")
        }
        return "\n".join(
            f"{hbold(self.chain_normalizer.normalize_to_signal(chain).upper())}: {hcode(contract.lower())}"
            for chain, contract in unique
        )

    def format_grouped_message(self, symbol: str, spread_group: list[dict], is_dev: bool) -> tuple[str, str, bool, float, float]:
        """
        Format grouped message for multiple spreads of the same token.
        Spreads are grouped by sell route — each sell market appears exactly ONCE
        with a consistent price (from the highest-spread pair in that group).

        Returns: (dev_message, prod_message, is_realizable, max_delay, top_delay)
        top_delay — delay of the top (highest-spread) pair only; used to gate prod sending.
        max_delay — max delay across all sibling pairs; used for dev annotations only.
        """
        now = datetime.now(UTC).replace(tzinfo=None)

        # Sort spreads: futures first by spread_percent DESC, then spot by profit DESC
        def sort_key(r):
            is_futures = "futures" in r["buy_from"] or "futures" in r["sell_to"]
            if is_futures:
                return (0, -r["spread_percent"])
            else:
                return (1, -r["profit"])

        sorted_spreads = sorted(spread_group, key=sort_key)

        # Build "freshest tickers" map: for each market (type_source), use the most
        # recent tickers_data entry across all sibling pairs.
        freshest_tickers: dict[str, dict] = {}
        for r in sorted_spreads:
            for mtype, sources in r.get("tickers_data", {}).items():
                for src_name, info in sources.items():
                    key = f"{mtype}_{src_name}"
                    current = freshest_tickers.get(key)
                    if not current or (info.get("get_at") or 0) > (current.get("get_at") or 0):
                        freshest_tickers[key] = info

        def get_fresh_info(market_type: str, source_name: str, result: dict) -> dict:
            """Return freshest known ticker info for a market, with fallback."""
            key = f"{market_type}_{source_name}"
            fresh = freshest_tickers.get(key)
            if fresh:
                return fresh
            return result.get("tickers_data", {}).get(market_type, {}).get(source_name, {})

        # Group spreads by sell_to, preserving sorted order (first-seen ordering for groups)
        sell_groups: dict[str, list[dict]] = {}
        for r in sorted_spreads:
            sk = r["sell_to"]
            if sk not in sell_groups:
                sell_groups[sk] = []
            sell_groups[sk].append(r)

        # Collect all unique chains from all spreads (case-insensitive dedup)
        all_chains = {}
        for r in sorted_spreads:
            for c in r.get("chains", []):
                chain_name = c.get("chain_name", "").lower()
                contract = c.get("contract_address", "")
                if chain_name and contract:
                    all_chains[(chain_name, contract.lower())] = c

        # Build message parts
        dev_lines = []
        prod_lines = []
        max_delay = 0.0
        top_delay = 0.0
        any_realizable = False
        overall_idx = 0  # tracks the global pair index for top_delay

        # Header with top spread info (first pair across all groups)
        top_result = sorted_spreads[0]
        profit_str = f" ({int(top_result['profit'])}$)"
        header = f"{hcode(symbol)} {int(top_result['spread_percent'])}%{profit_str}"

        analyzed_time = ""
        if top_result.get("analyzed_at"):
            analyzed_dt = datetime.fromtimestamp(int(top_result["analyzed_at"]) / 1000)
            analyzed_time = f"\n⚙️ Analyzed at: {analyzed_dt.strftime('%H:%M:%S')}"

        dev_lines.append(f"{header}\nUTC time: {now.strftime('%H:%M:%S')}{analyzed_time}\n")
        prod_lines.append(f"{header}\n")

        # ── Process each sell group ──────────────────────────────────────────
        for sell_key, group in sell_groups.items():
            buyer_type, buyer_source_name = sell_key.split("_", 1)
            top_of_group = group[0]  # highest-spread pair in this group (already sorted)

            # Sell block — shown ONCE per sell route, using top pair's price
            buyer_info = get_fresh_info(buyer_type.lower(), buyer_source_name, top_of_group)
            sell_side_link = self.make_market_link(top_of_group, buyer_type.lower(), buyer_source_name)
            buyer_get_time = datetime.fromtimestamp(int(buyer_info.get("get_at", 0)) / 1000)
            buyer_ws_time = f"⏳ Get ws time: {buyer_get_time.strftime('%H:%M:%S')}"
            sell_volume_usdt = int(top_of_group["sell_price"] * top_of_group["volume"])

            prod_sell_block = (
                f"{hlink(f'{buyer_source_name.upper()}_{buyer_type.upper()}', sell_side_link)}: "
                f"${format_significant(top_of_group['sell_price'], 3)} | Sell: {sell_volume_usdt}$"
            )
            dev_sell_block = prod_sell_block + buyer_ws_time

            # D/W status for sell side (use top pair's seller for realizable chain detection)
            top_seller_type, top_seller_source_name = top_of_group["buy_from"].split("_", 1)
            top_seller_info = get_fresh_info(top_seller_type.lower(), top_seller_source_name, top_of_group)
            _, top_realizable_chains = self.get_realizable_chains(top_seller_info, buyer_info)

            if buyer_info:
                sell_status = self.format_status(buyer_info, top_realizable_chains, set())
                prod_sell_block += "\n" + sell_status
                dev_sell_block += "\n" + sell_status

            dev_lines.append("\n" + dev_sell_block + "\n")
            prod_lines.append("\n" + prod_sell_block + "\n")

            # ── Buy sides for this sell group ────────────────────────────────
            for result in group:
                seller_key = result["buy_from"]
                seller_type, seller_source_name = seller_key.split("_", 1)

                buy_side_link = self.make_market_link(result, seller_type.lower(), seller_source_name)
                seller_info = get_fresh_info(seller_type.lower(), seller_source_name, result)
                buyer_info_r = get_fresh_info(buyer_type.lower(), buyer_source_name, result)

                seller_get_time = datetime.fromtimestamp(int(seller_info.get("get_at", 0)) / 1000)
                seller_ws_time = f"⏳ Get ws time: {seller_get_time.strftime('%H:%M:%S')}"

                is_realizable, realizable_chains = self.get_realizable_chains(seller_info, buyer_info_r)
                any_realizable = any_realizable or is_realizable

                seller_names = {
                    self.chain_normalizer.normalize_to_signal(c.get("chain_name")).upper()
                    for c in (seller_info.get("chains") or [])
                    if c.get("chain_name")
                }
                buyer_names_r = {
                    self.chain_normalizer.normalize_to_signal(c.get("chain_name")).upper()
                    for c in (buyer_info_r.get("chains") or [])
                    if c.get("chain_name")
                }
                intersecting_chains = seller_names & buyer_names_r

                # Delay calculation
                seller_delay = now - seller_get_time
                buyer_delay_r = now - buyer_get_time  # buyer_get_time from sell block above

                seller_threshold = (self.config.max_lbank_delay
                                    if seller_source_name.lower() == "lbank"
                                    else self.config.max_other_delay)
                buyer_threshold = (self.config.max_lbank_delay
                                   if buyer_source_name.lower() == "lbank"
                                   else self.config.max_other_delay)

                seller_exceeds = seller_delay.total_seconds() > seller_threshold
                buyer_exceeds = buyer_delay_r.total_seconds() > buyer_threshold
                delay = max(seller_delay, buyer_delay_r) if (seller_exceeds or buyer_exceeds) else timedelta(seconds=0)

                max_delay = max(max_delay, delay.total_seconds())
                if overall_idx == 0:
                    top_delay = delay.total_seconds()
                overall_idx += 1

                buy_volume_usdt = int(result["buy_price"] * result["volume"])
                profit_str_r = f" ({int(result['profit'])}$)"
                spread_prefix = f"{int(result['spread_percent'])}%{profit_str_r} "

                if result["buy_price"] < 1e-9:
                    from settings import logger
                    logger.warning(
                        f"Broadcaster tiny buy_price: {result['buy_price']:.6e} | "
                        f"symbol={symbol} | source={seller_source_name} | "
                        f"spread={result.get('spread_percent', 0):.1f}%"
                    )

                prod_buy_msg = (
                    f"{spread_prefix}"
                    f"{hlink(f'{seller_source_name.upper()}_{seller_type.upper()}', buy_side_link)}: "
                    f"${format_significant(result['buy_price'], 3)} | Buy: {buy_volume_usdt}$"
                )
                dev_buy_msg = prod_buy_msg + seller_ws_time

                if seller_info:
                    seller_status = self.format_status(seller_info, realizable_chains, intersecting_chains)
                    prod_buy_msg += "\n" + seller_status
                    dev_buy_msg += "\n" + seller_status

                dev_lines.append("\n" + dev_buy_msg + "\n")
                prod_lines.append("\n" + prod_buy_msg + "\n")

        # Add all unique chains at the end
        if all_chains:
            chains_str = "\n".join(
                f"{hbold(self.chain_normalizer.normalize_to_signal(chain).upper())}: {hcode(contract.lower())}"
                for (chain, contract) in sorted(all_chains.keys())
            )
            dev_lines.append("\n" + chains_str)
            prod_lines.append("\n" + chains_str)

        # Dev extras
        if is_dev:
            applied_fee = top_result.get("applied_fee", 0)
            dev_lines.append(f"\n\nУчтена комиссия в анализаторе: {applied_fee:.2f}$")
            if max_delay >= 5:
                dev_lines.append(f"\n\n#delay: {max_delay:.2f}s")

        dev_message = "".join(dev_lines)
        prod_message = "".join(prod_lines)
        return dev_message, prod_message, any_realizable, max_delay, top_delay
    async def send_results(self, results: list[dict]):
        start = time.perf_counter()
        if self.config.use_grouped_template:
            await self._send_grouped_results(results)
        else:
            await self._send_single_results(results)
        elapsed = time.perf_counter() - start

        # Log if broadcasting took significant time
        if elapsed > 0.5:
            from settings import logger
            logger.warning(
                f"Broadcaster took {elapsed:.2f}s to send {len(results)} results"
            )

    async def _send_grouped_results(self, results: list[dict]):
        """Send results grouped by token."""
        from settings import logger

        # Group results by global_token_id
        group_start = time.perf_counter()
        grouped = defaultdict(list)
        for result in results:
            token_id = result.get("global_token_id")
            if token_id:
                grouped[token_id].append(result)
        group_time = time.perf_counter() - group_start

        # Format messages
        format_start = time.perf_counter()
        tasks = []

        for token_id, spread_group in grouped.items():
            # Get symbol from first result
            symbol = spread_group[0].get("symbol", "")
            level = spread_group[0]["level"]  # Use highest priority level

            # Determine environment-specific settings
            is_dev_env = settings.environment in ["test", "dev"]
            dev_message, prod_message, is_realizable, max_delay, top_delay = self.format_grouped_message(
                symbol, spread_group, is_dev=is_dev_env
            )

            # Topic mapping
            test_topic_map = {
                SignalLevelType.LOW: settings.bot.test_topic_low_id,
                SignalLevelType.MEDIUM: settings.bot.test_topic_medium_id,
                SignalLevelType.HIGH: settings.bot.test_topic_high_id,
                SignalLevelType.VERY_HIGH: settings.bot.test_topic_very_high_id,
            }
            dev_topic_map = {
                SignalLevelType.LOW: settings.bot.dev_topic_low_id,
                SignalLevelType.MEDIUM: settings.bot.dev_topic_medium_id,
                SignalLevelType.HIGH: settings.bot.dev_topic_high_id,
                SignalLevelType.VERY_HIGH: settings.bot.dev_topic_very_high_id,
            }
            prod_topic_map = {
                SignalLevelType.LOW: settings.bot.prod_topic_low_id,
                SignalLevelType.MEDIUM: settings.bot.prod_topic_medium_id,
                SignalLevelType.HIGH: settings.bot.prod_topic_high_id,
                SignalLevelType.VERY_HIGH: settings.bot.prod_topic_very_high_id,
            }
            en_prod_topic_map = {
                SignalLevelType.LOW: settings.bot.en_prod_topic_low_id,
                SignalLevelType.MEDIUM: settings.bot.en_prod_topic_medium_id,
                SignalLevelType.HIGH: settings.bot.en_prod_topic_high_id,
                SignalLevelType.VERY_HIGH: settings.bot.en_prod_topic_very_high_id,
            }
            tech_prod_topic_map = {
                SignalLevelType.LOW: settings.bot.tech_prod_topic_low_id,
                SignalLevelType.MEDIUM: settings.bot.tech_prod_topic_medium_id,
                SignalLevelType.HIGH: settings.bot.tech_prod_topic_high_id,
                SignalLevelType.VERY_HIGH: settings.bot.tech_prod_topic_very_high_id,
            }

            test_topic_id = test_topic_map.get(level)
            dev_topic_id = dev_topic_map.get(level)
            prod_topic_id = prod_topic_map.get(level)
            en_prod_topic_id = en_prod_topic_map.get(level)
            tech_prod_topic_id = tech_prod_topic_map.get(level)

            # Persist broadcast signals to DB (only signals actually sent to TG)
            if self.signal_writer:
                for r in spread_group:
                    # Add telegram_link to result before saving
                    seller_key = r["buy_from"]
                    buyer_key = r["sell_to"]
                    seller_type, seller_source_name = seller_key.split("_", 1)
                    buyer_type, buyer_source_name = buyer_key.split("_", 1)
                    
                    buy_link = self.make_market_link(r, seller_type.lower(), seller_source_name)
                    sell_link = self.make_market_link(r, buyer_type.lower(), buyer_source_name)
                    r["telegram_link"] = f"Buy: {buy_link} | Sell: {sell_link}"
                    
                    self.signal_writer.enqueue(r)

            match settings.environment:
                case "test":
                    tasks.append(asyncio.create_task(
                        send_message(dev_bot, dev_message, settings.bot.test_channel_id, test_topic_id)
                    ))
                case "dev":
                    if max_delay >= 5:
                        tasks.append(asyncio.create_task(
                            send_message(dev_bot, dev_message, settings.bot.dev_channel_id, settings.bot.dev_topic_delay_id)
                        ))
                    else:
                        tasks.append(asyncio.create_task(
                            send_message(dev_bot, dev_message, settings.bot.dev_channel_id, dev_topic_id)
                        ))
                case "prod":
                    tasks.append(asyncio.create_task(
                        send_message(tech_prod_bot, dev_message, settings.bot.tech_prod_channel_id, tech_prod_topic_id)
                    ))
                    # Gate prod on top-pair delay only; stale siblings must not suppress the main signal
                    if top_delay < 15:
                        tasks.append(asyncio.create_task(
                            send_message(prod_bot, prod_message, settings.bot.prod_channel_id, prod_topic_id)
                        ))
                        tasks.append(asyncio.create_task(
                            send_message(en_prod_bot, prod_message, settings.bot.en_prod_channel_id, en_prod_topic_id)
                        ))
                case _:
                    raise Exception(f"Unknown environment: {settings.environment}")

        format_time = time.perf_counter() - format_start

        # Send messages
        send_start = time.perf_counter()
        await asyncio.gather(*tasks, return_exceptions=True)
        send_time = time.perf_counter() - send_start

        total_time = group_time + format_time + send_time
        if total_time > 0.1:
            logger.debug(
                f"Grouped broadcast timing: group={group_time*1000:.1f}ms, "
                f"format={format_time*1000:.1f}ms, send={send_time*1000:.1f}ms, "
                f"total={total_time*1000:.1f}ms for {len(grouped)} tokens"
            )

    async def _send_single_results(self, results: list[dict]):
        """Send individual result messages (original behavior)."""
        from settings import logger

        format_start = time.perf_counter()
        tasks = []

        for result in results:
            symbol = result.get("symbol", "")
            seller_key = result["buy_from"]
            buyer_key = result["sell_to"]
            applied_fee = result.get("applied_fee", 0)

            seller_type, seller_source_name = seller_key.split("_", 1)
            buyer_type, buyer_source_name = buyer_key.split("_", 1)

            tickers_data = result.get("tickers_data", {})

            buy_side_link = self.make_market_link(result, seller_type.lower(), seller_source_name)
            sell_side_link = self.make_market_link(result, buyer_type.lower(), buyer_source_name)

            seller_info = tickers_data.get(seller_type.lower(), {}).get(seller_source_name, {})
            buyer_info = tickers_data.get(buyer_type.lower(), {}).get(buyer_source_name, {})

            seller_get_time = datetime.fromtimestamp(int(seller_info.get('get_at')) / 1000)
            buyer_get_time = datetime.fromtimestamp(int(buyer_info.get('get_at')) / 1000)
            seller_ws_time = f"⏳ Get ws time: {seller_get_time.strftime('%H:%M:%S')}"
            buyer_ws_time = f"⏳ Get ws time: {buyer_get_time.strftime('%H:%M:%S')}"
            is_realizable, realizable_chains = self.get_realizable_chains(seller_info, buyer_info)

            seller_names = {
                self.chain_normalizer.normalize_to_signal(c.get("chain_name")).upper()
                for c in (seller_info.get("chains") or [])
                if c.get("chain_name")
            }
            buyer_names = {
                self.chain_normalizer.normalize_to_signal(c.get("chain_name")).upper()
                for c in (buyer_info.get("chains") or [])
                if c.get("chain_name")
            }

            intersecting_chains = seller_names & buyer_names
            result["spread_realizable"] = is_realizable

            buy_volume_usdt = int(result['buy_price'] * result['volume'])
            prod_seller_message = (
                f"{hlink(f"{seller_source_name.upper()}_{seller_type.upper()}", buy_side_link)}: "
                f"${format_significant(result['buy_price'], 3)} | Buy: {buy_volume_usdt}$"
            )
            dev_seller_message = prod_seller_message + seller_ws_time
            if seller_info:
                prod_seller_message += "\n" + self.format_status(seller_info, realizable_chains, intersecting_chains)
                dev_seller_message += "\n" + self.format_status(seller_info, realizable_chains, intersecting_chains)

            sell_volume_usdt = int(result['sell_price'] * result['volume'])
            prod_buyer_message = (
                f"{hlink(f"{buyer_source_name.upper()}_{buyer_type.upper()}", sell_side_link)}: "
                f"${format_significant(result['sell_price'], 3)} | Sell: {sell_volume_usdt}$"
            )
            dev_buyer_message = prod_buyer_message + buyer_ws_time
            if buyer_info:
                prod_buyer_message += "\n" + self.format_status(buyer_info, realizable_chains, intersecting_chains)
                dev_buyer_message += "\n" + self.format_status(buyer_info, realizable_chains, intersecting_chains)

            chains = self.format_chains(result)
            profit_str = f" ({int(result['profit'])}$)"
            now = datetime.now(UTC).replace(tzinfo=None)

            # Calculate individual delays
            seller_delay = now - seller_get_time
            buyer_delay = now - buyer_get_time

            # Get delay thresholds based on source
            seller_threshold = (self.config.max_lbank_delay
                              if seller_source_name.lower() == "lbank"
                              else self.config.max_other_delay)
            buyer_threshold = (self.config.max_lbank_delay
                             if buyer_source_name.lower() == "lbank"
                             else self.config.max_other_delay)

            # Check if any source exceeds its threshold
            seller_exceeds = seller_delay.total_seconds() > seller_threshold
            buyer_exceeds = buyer_delay.total_seconds() > buyer_threshold

            # Use max delay if any source exceeds threshold, otherwise no delay
            delay = max(seller_delay, buyer_delay) if (seller_exceeds or buyer_exceeds) else timedelta(seconds=0)

            prod_message = (
                f"{hcode(symbol)} {int(result['spread_percent'])}%{profit_str}\n"
                f"{prod_seller_message}\n\n"
                f"{prod_buyer_message}\n\n"
                f"{chains}"
            )

            # Add analyzed_at timestamp for dev
            analyzed_time = ""
            if result.get("analyzed_at"):
                analyzed_dt = datetime.fromtimestamp(int(result["analyzed_at"]) / 1000)
                analyzed_time = f"\n⚙️ Analyzed at: {analyzed_dt.strftime('%H:%M:%S')}"

            dev_message = (
                f"{hcode(symbol)} {int(result['spread_percent'])}%{profit_str}\n"
                f"UTC time: {now.strftime('%H:%M:%S')}{analyzed_time}\n\n"
                f"{dev_seller_message}\n\n"
                f"{dev_buyer_message}\n\n"
                f"{chains or ""}\n\n"
                f"Учтена комиссия в анализаторе: {applied_fee:.2f}$"
            )

            if delay.total_seconds() >= 5:
                dev_message += f"\n\n#delay: {delay.total_seconds():.2f}s"

            level = result["level"]

            test_topic_map = {
                SignalLevelType.LOW: settings.bot.test_topic_low_id,
                SignalLevelType.MEDIUM: settings.bot.test_topic_medium_id,
                SignalLevelType.HIGH: settings.bot.test_topic_high_id,
                SignalLevelType.VERY_HIGH: settings.bot.test_topic_very_high_id,
            }
            dev_topic_map = {
                SignalLevelType.LOW: settings.bot.dev_topic_low_id,
                SignalLevelType.MEDIUM: settings.bot.dev_topic_medium_id,
                SignalLevelType.HIGH: settings.bot.dev_topic_high_id,
                SignalLevelType.VERY_HIGH: settings.bot.dev_topic_very_high_id,
            }
            prod_topic_map = {
                SignalLevelType.LOW: settings.bot.prod_topic_low_id,
                SignalLevelType.MEDIUM: settings.bot.prod_topic_medium_id,
                SignalLevelType.HIGH: settings.bot.prod_topic_high_id,
                SignalLevelType.VERY_HIGH: settings.bot.prod_topic_very_high_id,
            }
            en_prod_topic_map = {
                SignalLevelType.LOW: settings.bot.en_prod_topic_low_id,
                SignalLevelType.MEDIUM: settings.bot.en_prod_topic_medium_id,
                SignalLevelType.HIGH: settings.bot.en_prod_topic_high_id,
                SignalLevelType.VERY_HIGH: settings.bot.en_prod_topic_very_high_id,
            }
            tech_prod_topic_map = {
                SignalLevelType.LOW: settings.bot.tech_prod_topic_low_id,
                SignalLevelType.MEDIUM: settings.bot.tech_prod_topic_medium_id,
                SignalLevelType.HIGH: settings.bot.tech_prod_topic_high_id,
                SignalLevelType.VERY_HIGH: settings.bot.tech_prod_topic_very_high_id,
            }

            test_topic_id = test_topic_map.get(level)
            dev_topic_id = dev_topic_map.get(level)
            prod_topic_id = prod_topic_map.get(level)
            en_prod_topic_id = en_prod_topic_map.get(level)
            tech_prod_topic_id = tech_prod_topic_map.get(level)

            # Persist broadcast signal to DB
            if self.signal_writer:
                # Add telegram_link to result before saving
                result["telegram_link"] = f"Buy: {buy_side_link} | Sell: {sell_side_link}"
                self.signal_writer.enqueue(result)

            match settings.environment:
                case "test":
                    tasks.append(asyncio.create_task(
                        send_message(dev_bot, dev_message, settings.bot.test_channel_id, test_topic_id)
                    ))
                case "dev":
                    if delay.total_seconds() >= 5:
                        tasks.append(asyncio.create_task(
                            send_message(dev_bot, dev_message, settings.bot.dev_channel_id, settings.bot.dev_topic_delay_id)
                        ))
                    else:
                        tasks.append(asyncio.create_task(
                            send_message(dev_bot, dev_message, settings.bot.dev_channel_id, dev_topic_id)
                        ))
                case "prod":
                    tasks.append(asyncio.create_task(
                        send_message(tech_prod_bot, dev_message, settings.bot.tech_prod_channel_id, tech_prod_topic_id)
                    ))
                    if delay.total_seconds() < 15:
                        tasks.append(asyncio.create_task(
                            send_message(prod_bot, prod_message, settings.bot.prod_channel_id, prod_topic_id)
                        ))
                        tasks.append(asyncio.create_task(
                            send_message(en_prod_bot, prod_message, settings.bot.en_prod_channel_id, en_prod_topic_id)
                        ))
                case _:
                    raise Exception(f"Unknown environment: {settings.environment}")

        format_time = time.perf_counter() - format_start

        # Send messages
        send_start = time.perf_counter()
        await asyncio.gather(*tasks, return_exceptions=True)
        send_time = time.perf_counter() - send_start

        total_time = format_time + send_time
        if total_time > 0.1:
            logger.debug(
                f"Single broadcast timing: format={format_time*1000:.1f}ms, "
                f"send={send_time*1000:.1f}ms, total={total_time*1000:.1f}ms "
                f"for {len(results)} results"
            )

import asyncio
from datetime import datetime, UTC, timedelta
from uuid import UUID

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.utils.markdown import hcode, hbold

from bot.keyboards.inline import admin_keyboard, mute_token_kb, exchange_mute_kb
from bot.services.broadcaster import broadcast
from bot.utils.edit_message import edit_message
from bot.utils.states import TokenBlacklistState, ExchangeBlacklistState, PairBlacklistState
from infrastructure.database.models.base import MuteType, SourceType, MarketType
from infrastructure.database.models.tokens import TokenBlacklist
from infrastructure.database.models.sources import ExchangeBlacklist
from infrastructure.services.collectors.utils.cleanup import cleanup_orderbooks
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.normalizer import normalize_signal_to_db
from infrastructure.services.common.cache import RedisCache

router = Router()


@router.message(Command("admin"))
async def admin_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Админ функционал:",
        reply_markup=admin_keyboard
    )


@router.message(Command("find_token"))
async def find_token_cmd(message: Message, repo: RequestsRepo):
    """
    Find global_token_id by symbol and optionally source.
    
    Usage:
        /find_token KEKE
        /find_token KEKE XT
    """
    args = message.text.split(maxsplit=2)
    
    if len(args) < 2:
        await message.answer(
            f"Использование:\n"
            f"{hcode('/find_token SYMBOL')} - найти все GT по символу\n"
            f"{hcode('/find_token SYMBOL EXCHANGE')} - найти GT по символу и бирже\n\n"
            f"Примеры:\n"
            f"{hcode('/find_token KEKE')}\n"
            f"{hcode('/find_token KEKE XT')}"
        )
        return
    
    symbol = args[1].strip().upper()
    source_filter = args[2].strip().upper() if len(args) > 2 else None
    
    # Get all global tokens with this symbol
    all_tokens = await repo.global_tokens.get_by_symbols_with_meta([symbol])
    
    if not all_tokens:
        await message.answer(f"Токен {symbol} не найден в базе")
        return
    
    # Filter out empty/useless GT (no sources, no chains, no data)
    # These are artifacts from token_collector runs
    global_tokens = [
        gt for gt in all_tokens
        if gt.sources and len(gt.sources) > 0  # Has at least one exchange
    ]
    
    if not global_tokens:
        await message.answer(
            f"Токен {symbol} найден ({len(all_tokens)} записей), но все без данных (пустые GT)\n"
            f"Это артефакты сборщика - можно очистить."
        )
        return
    
    # Filter by source if specified
    if source_filter:
        filtered = []
        for gt in global_tokens:
            sources = [s.name.upper() for s in gt.sources] if gt.sources else []
            if source_filter in sources:
                filtered.append(gt)
        global_tokens = filtered
        
        if not global_tokens:
            await message.answer(f"Токен {symbol} на бирже {source_filter} не найден")
            return
    
    # Sort by number of sources (most complete first)
    global_tokens.sort(key=lambda gt: len(gt.sources) if gt.sources else 0, reverse=True)
    
    # Limit to top 20 to avoid spam
    total_count = len(global_tokens)
    if total_count > 20:
        global_tokens = global_tokens[:20]
    
    # Build response
    header = f"🔍 Найдено токенов {symbol}: {len(global_tokens)}"
    if total_count > 20:
        header += f" (показано топ-20 из {total_count})"
    lines = [header + "\n"]
    
    for gt in global_tokens:
        # Get sources
        sources = [s.name for s in gt.sources] if gt.sources else []
        
        # Get market types
        market_types = set()
        for tc in gt.token_on_chains or []:
            if tc.token and tc.token.market_type:
                market_types.add(tc.token.market_type.value)
        
        # Get contracts (first 2)
        contracts = []
        for tc in (gt.token_on_chains or [])[:2]:
            if tc.chain and tc.contract_address:
                contracts.append(f"{tc.chain.name}: {tc.contract_address[:10]}...")
        
        lines.append(f"━━━━━━━━━━━━━━━━━━")
        lines.append(f"{hbold(gt.symbol)}")
        lines.append(f"ID: {hcode(str(gt.id))}")
        lines.append(f"Биржи: {', '.join(sources)}")
        lines.append(f"Рынки: {', '.join(market_types)}")
        if contracts:
            lines.append(f"Контракты: {', '.join(contracts)}")
        lines.append("")
    
    text = "\n".join(lines)
    
    # Split into chunks if too long
    if len(text) > 4000:
        await message.answer(text[:4000] + "\n\n...(обрезано)")
    else:
        await message.answer(text)


@router.callback_query(F.data == "show_token_blacklist")
async def show_blacklist_cb(query: CallbackQuery, repo: RequestsRepo, cache: RedisCache):
    token_blacklist: list[TokenBlacklist] = await repo.token_blacklist.get_all()

    if not token_blacklist:
        await query.answer("Blacklist пуст", show_alert=True)
        return

    now = datetime.now(UTC).replace(tzinfo=None)

    # Group by global_token_id
    grouped = {}
    ungrouped = []

    for t in token_blacklist:
        # Skip expired
        if t.expires_at and t.expires_at <= now:
            continue

        expires = f" (до {t.expires_at.strftime('%Y-%m-%d %H:%M')})" if t.expires_at else ""

        if t.global_token_id and t.global_token:
            # Grouped by token symbol
            symbol = t.global_token.symbol
            if symbol not in grouped:
                grouped[symbol] = []

            # If no chain_name - it's a global mute for all networks of this token
            if not t.chain_name:
                grouped[symbol].append({
                    "chain": None,
                    "address": None,
                    "expires": expires,
                    "is_global": True
                })
            else:
                # Specific network + address mute
                grouped[symbol].append({
                    "chain": t.chain_name,
                    "address": t.contract_address,
                    "expires": expires,
                    "is_global": False
                })
        elif t.chain_name:
            # Ungrouped (legacy format - records without global_token_id)
            ungrouped.append(f"  {hcode(t.chain_name)} : {hcode(t.contract_address or '—')}{expires}")

    lines = []

    # Display grouped tokens
    for symbol in sorted(grouped.keys()):
        lines.append(f"\n{hbold(symbol)}:")
        for item in grouped[symbol]:
            if item["is_global"]:
                lines.append(f"  [Глобальный мут всех сетей]{item['expires']}")
            else:
                lines.append(f"  {hcode(item['chain'])} : {hcode(item['address'] or '—')}{item['expires']}")

    # Display ungrouped entries
    if ungrouped:
        if lines:
            lines.append("\n" + hbold("Без токена:"))
        lines.extend(ungrouped)

    await edit_message(query.message, "\n".join(lines) if lines else "Blacklist пуст", reply_markup=admin_keyboard)
    await query.answer()


@router.callback_query(F.data == "add_token_to_blacklist")
async def add_token_cb(query: CallbackQuery):
    await edit_message(query.message, "Выберите тип мута:", reply_markup=mute_token_kb)


@router.callback_query(F.data.startswith("add_bl:"))
async def choose_mute_type(query: CallbackQuery, state: FSMContext):
    mute_type = query.data.split(":")[1]
    await state.update_data(mute_type=mute_type)

    await edit_message(
        query.message,
        f"Введите токены для добавления, каждый токен на новой строке в формате:\n"
        f"{hcode('chain address')} - мут по сети+адресу\n"
        f"{hcode('symbol TOKEN')} - глобальный мут токена во всех сетях\n"
        f"{hcode('symbol_chain TOKEN CHAIN')} - мут токена в конкретной сети"
    )
    await state.set_state(TokenBlacklistState.get_new_token)


@router.message(TokenBlacklistState.get_new_token)
async def process_add(message: Message, repo: RequestsRepo, cache: RedisCache, state: FSMContext):
    data = await state.get_data()
    mute_type = MuteType(data.get("mute_type", MuteType.PERMANENT.value))

    lines = message.text.strip().splitlines()
    if not lines:
        await message.answer(
            f"Неверный формат. Используйте:\n"
            f"{hcode('BSC 0xaddress')} - мут по сети+адресу\n"
            f"{hcode('symbol AIBOT')} - глобальный мут по символу\n"
            f"{hcode('symbol_chain AIBOT BSC')} - мут токена в конкретной сети"
        )
        return

    added = []
    cached = await cache.get("token_blacklist") or []

    for line in lines:
        parts = line.strip().split(maxsplit=1)
        if not parts:
            continue

        first_part = parts[0].rstrip(":").strip()
        second_part = parts[1].strip() if len(parts) > 1 else None

        expires_at = None
        if mute_type == MuteType.TEMPORARY:
            expires_at = datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=24)

        # Check if this is a symbol-based mute (format: "symbol AIBOT")
        if first_part.lower() == "symbol" and second_part:
            # Global mute by symbol
            symbol = second_part.upper()
            global_token = await repo.global_tokens.get_by_symbol(symbol)

            if not global_token:
                await message.answer(f"Токен {symbol} не найден в базе")
                continue

            await repo.token_blacklist.get_or_create(
                global_token_id=global_token.id,
                mute_type=mute_type,
                expires_at=expires_at
            )

            cached.append({
                "global_token_id": str(global_token.id),
                "symbol": symbol,
                "expires_at": expires_at.isoformat() if expires_at else None
            })
            added.append(f"symbol: {symbol}")

            if message.from_user.id != 0:
                await broadcast(
                    message.bot,
                    [0],
                    f"Админ: {message.from_user.username} | {message.from_user.id} добавил в {mute_type.value} мут:\nsymbol: {symbol}"
                )
        elif first_part.lower() == "symbol_chain" and second_part:
            # Mute specific token in specific chain (format: "symbol_chain AIBOT BSC")
            parts_extra = line.strip().split(maxsplit=2)
            if len(parts_extra) < 3:
                await message.answer(
                    f"Неверный формат для symbol_chain. Используйте:\\n"
                    f"{hcode('symbol_chain TOKEN CHAIN')} (например: symbol_chain AIBOT BSC)"
                )
                continue

            symbol = parts_extra[1].upper()
            chain_input = parts_extra[2].strip()

            global_token = await repo.global_tokens.get_by_symbol(symbol)
            if not global_token:
                await message.answer(f"Токен {symbol} не найден в базе")
                continue

            # Normalize chain name
            chain = normalize_signal_to_db(chain_input)

            await repo.token_blacklist.get_or_create(
                global_token_id=global_token.id,
                chain_name=chain,
                mute_type=mute_type,
                expires_at=expires_at
            )

            cached.append({
                "global_token_id": str(global_token.id),
                "symbol": symbol,
                "chain": chain,
                "expires_at": expires_at.isoformat() if expires_at else None
            })
            added.append(f"symbol_chain: {symbol} в {chain}")

            if message.from_user.id != 0:
                await broadcast(
                    message.bot,
                    [0],
                    f"Админ: {message.from_user.username} | {message.from_user.id} добавил в {mute_type.value} мут:\nsymbol_chain: {symbol} в {chain}"
                )
        else:
            # Chain+address mute
            # Validate that address is provided
            if not second_part:
                await message.answer(
                    f"Ошибка: не указан адрес контракта для сети {first_part}.\n"
                    f"Используйте:\n"
                    f"{hcode('CHAIN ADDRESS')} - для мута конкретного адреса\n"
                    f"{hcode('symbol TOKEN')} - для глобального мута\n"
                    f"{hcode('symbol_chain TOKEN CHAIN')} - для мута токена в конкретной сети"
                )
                continue

            # Auto-lowercase and apply signal-to-db mapping
            chain = normalize_signal_to_db(first_part)
            address = second_part.lower()

            # Try to resolve global_token by chain+address
            global_token = await repo.global_tokens.get_by_chain_address(chain, address)

            await repo.token_blacklist.get_or_create(
                chain_name=chain,
                contract_address=address,
                global_token_id=global_token.id if global_token else None,
                mute_type=mute_type,
                expires_at=expires_at
            )

            symbol_hint = f" ({global_token.symbol})" if global_token else ""
            cached.append({
                "chain": chain,
                "address": address,
                "global_token_id": str(global_token.id) if global_token else None,
                "symbol": global_token.symbol if global_token else None,
                "expires_at": expires_at.isoformat() if expires_at else None
            })
            added.append(f"{chain} : {address}{symbol_hint}")

            if message.from_user.id != 0:
                await broadcast(
                    message.bot,
                    [0],
                    f"Админ: {message.from_user.username} | {message.from_user.id} добавил в {mute_type.value} мут:\n{chain} : {address}"
                )

    await cache.set("token_blacklist", cached)

    await message.answer("Добавлено:\n" + "\n\n".join(added), reply_markup=admin_keyboard)
    await state.clear()


@router.callback_query(F.data == "remove_token_from_blacklist")
async def remove_token_cb(query: CallbackQuery, repo: RequestsRepo):
    token_blacklist: list[TokenBlacklist] = await repo.token_blacklist.get_all()
    if not token_blacklist:
        await query.answer("Blacklist пуст", show_alert=True)
        return

    buttons = [
        [InlineKeyboardButton(
            text=f"{t.chain_name} : {t.contract_address or '—'}",
            callback_data=f"del:{t.id}"
        )]
        for t in token_blacklist
    ]

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await edit_message(query.message, "Выберите токен для удаления:", reply_markup=kb)


@router.callback_query(F.data.startswith("del:"))
async def delete_token_cb(query: CallbackQuery, repo: RequestsRepo, cache: RedisCache):
    token_id = query.data[4:]

    token: TokenBlacklist = await repo.token_blacklist.get(token_id)
    if not token:
        await query.answer("Токен уже удалён", show_alert=True)
        return

    await repo.token_blacklist.delete(token_id)

    cached = await cache.get("token_blacklist") or []
    cached = [
        t for t in cached
        if not (t["chain"] == token.chain_name and t["address"] == token.contract_address)
    ]
    await cache.set("token_blacklist", cached)

    await query.message.answer(
        f"Удалено:\n{token.chain_name} : {token.contract_address or '—'}",
        reply_markup=admin_keyboard
    )
    await query.answer()


@router.callback_query(F.data == "show_exchange_blacklist")
async def show_exchange_blacklist_cb(query: CallbackQuery, repo: RequestsRepo, cache: RedisCache):
    exchange_blacklist: list[ExchangeBlacklist] = await repo.exchange_blacklist.get_all()

    if not exchange_blacklist:
        await query.answer("Exchange blacklist пуст", show_alert=True)
        return

    now = datetime.now(UTC).replace(tzinfo=None)

    # Group by source name
    grouped = {}

    for entry in exchange_blacklist:
        # Skip expired
        if entry.expires_at and entry.expires_at <= now:
            continue

        expires = f" (до {entry.expires_at.strftime('%Y-%m-%d %H:%M')})" if entry.expires_at else " (навсегда)"

        source_name = entry.source.name

        if source_name not in grouped:
            grouped[source_name] = []

        # If market_type is None - it's a global mute for all markets
        if entry.market_type is None:
            grouped[source_name].append({
                "market": None,
                "expires": expires,
                "is_global": True
            })
        else:
            # Specific market mute
            grouped[source_name].append({
                "market": entry.market_type.value,
                "expires": expires,
                "is_global": False
            })

    lines = []

    # Display grouped exchanges
    for source_name in sorted(grouped.keys()):
        lines.append(f"\n{hbold(source_name)}:")
        for item in grouped[source_name]:
            if item["is_global"]:
                lines.append(f"  [Все рынки]{item['expires']}")
            else:
                lines.append(f"  {hcode(item['market'])}{item['expires']}")

    await edit_message(query.message, "\n".join(lines) if lines else "Exchange blacklist пуст", reply_markup=admin_keyboard)
    await query.answer()


@router.callback_query(F.data == "add_exchange_to_blacklist")
async def add_exchange_cb(query: CallbackQuery):
    await edit_message(query.message, "Выберите тип мута:", reply_markup=exchange_mute_kb)


@router.callback_query(F.data.startswith("add_ex_bl:"))
async def choose_exchange_mute_type(query: CallbackQuery, state: FSMContext, repo: RequestsRepo):
    mute_type = query.data.split(":")[1]
    await state.update_data(mute_type=mute_type)

    # Get list of available exchanges
    all_cex = await repo.sources.get_all_cex()
    cex_names = ", ".join(sorted(s.name for s in all_cex))

    await edit_message(
        query.message,
        f"Введите биржи для добавления, каждая биржа на новой строке в формате:\n"
        f"{hcode('MEXC')} - глобальный мут всех рынков\n"
        f"{hcode('MEXC spot')} - мут только спот рынка\n"
        f"{hcode('MEXC futures')} - мут только фьючерсов\n\n"
        f"Доступные биржи: {cex_names}"
    )
    await state.set_state(ExchangeBlacklistState.get_new_exchange)


@router.message(ExchangeBlacklistState.get_new_exchange)
async def process_add_exchange(message: Message, repo: RequestsRepo, cache: RedisCache, state: FSMContext):
    data = await state.get_data()
    mute_type = MuteType(data.get("mute_type", MuteType.PERMANENT.value))

    lines = message.text.strip().splitlines()
    if not lines:
        await message.answer(
            f"Неверный формат. Используйте:\n"
            f"{hcode('MEXC')} - глобальный мут всех рынков\n"
            f"{hcode('MEXC spot')} - мут только спот рынка\n"
            f"{hcode('MEXC futures')} - мут только фьючерсов"
        )
        return

    # Show processing indicator
    processing_msg = await message.answer("⏳ Обработка...")

    added = []
    cached = await cache.get("exchange_blacklist") or []

    for line in lines:
        parts = line.strip().split(maxsplit=1)
        if not parts:
            continue

        source_name = parts[0].strip()
        market_str = parts[1].strip().lower() if len(parts) > 1 else None

        # Parse market_type
        market_type = None
        if market_str == "spot":
            market_type = MarketType.SPOT
        elif market_str == "futures":
            market_type = MarketType.FUTURES
        elif market_str is not None:
            await message.answer(
                f"Неверный тип рынка: {market_str}. Используйте 'spot', 'futures' или оставьте пустым для глобального мута."
            )
            continue

        # Get source from DB (case-insensitive)
        source = await repo.sources.get_by_name_and_type(source_name, SourceType.CEX)
        if not source:
            all_cex = await repo.sources.get_all_cex()
            cex_names = ", ".join(sorted(s.name for s in all_cex))
            await message.answer(f"Биржа {source_name} не найдена.\nДоступные биржи: {cex_names}")
            continue

        expires_at = None
        if mute_type == MuteType.TEMPORARY:
            expires_at = datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=24)

        # Create exchange blacklist entry
        await repo.exchange_blacklist.get_or_create(
            source_id=source.id,
            market_type=market_type,
            mute_type=mute_type,
            expires_at=expires_at
        )

        # Cleanup orderbooks in background (don't block response)
        asyncio.create_task(cleanup_orderbooks(cache, repo, source.id, market_type))

        # Update cache
        cached.append({
            "source_id": str(source.id),
            "source_name": source.name,
            "market_type": market_type.value if market_type else None,
            "expires_at": expires_at.isoformat() if expires_at else None
        })

        market_display = f" ({market_type.value})" if market_type else " (все рынки)"
        added.append(f"{source.name}{market_display}")

        # Broadcast notification
        if message.from_user.id != 0:
            await broadcast(
                message.bot,
                [0],
                f"Админ: {message.from_user.username} | {message.from_user.id} добавил в {mute_type.value} мут биржу:\n{source.name}{market_display}"
            )

    await cache.set("exchange_blacklist", cached)

    # Delete processing message
    await processing_msg.delete()

    await message.answer("Добавлено:\n" + "\n\n".join(added), reply_markup=admin_keyboard)
    await state.clear()


@router.callback_query(F.data == "remove_exchange_from_blacklist")
async def remove_exchange_cb(query: CallbackQuery, repo: RequestsRepo):
    exchange_blacklist: list[ExchangeBlacklist] = await repo.exchange_blacklist.get_all()
    if not exchange_blacklist:
        await query.answer("Exchange blacklist пуст", show_alert=True)
        return

    buttons = []
    for entry in exchange_blacklist:
        source_name = entry.source.name
        if entry.market_type:
            display_text = f"{source_name} - {entry.market_type.value}"
        else:
            display_text = f"{source_name} - все рынки"

        buttons.append([InlineKeyboardButton(
            text=display_text,
            callback_data=f"del_ex:{entry.id}"
        )])

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await edit_message(query.message, "Выберите биржу для удаления:", reply_markup=kb)


@router.callback_query(F.data.startswith("del_ex:"))
async def delete_exchange_cb(query: CallbackQuery, repo: RequestsRepo, cache: RedisCache):
    entry_id = query.data[7:]

    entry: ExchangeBlacklist = await repo.exchange_blacklist.get(entry_id)
    if not entry:
        await query.answer("Биржа уже удалена", show_alert=True)
        return

    source_name = entry.source.name
    market_type = entry.market_type

    await repo.exchange_blacklist.delete(entry_id)

    # Update cache
    cached = await cache.get("exchange_blacklist") or []
    cached = [
        e for e in cached
        if not (e.get("source_id") == str(entry.source_id) and
                (e.get("market_type") == (market_type.value if market_type else None)))
    ]
    await cache.set("exchange_blacklist", cached)

    market_display = f" - {market_type.value}" if market_type else " - все рынки"
    await query.message.answer(
        f"Удалено:\n{source_name}{market_display}",
        reply_markup=admin_keyboard
    )
    await query.answer()


@router.callback_query(F.data.startswith(("global_approve:", "global_decline:")))
async def global_candidate_action(query: CallbackQuery, repo: RequestsRepo, cache: RedisCache):
    action, candidate_hash = query.data.split(":", 1)
    combined = await cache.get(f"global_candidate_hash:{candidate_hash}")
    if not combined:
        await query.answer("Кандидат устарел или недействителен", show_alert=True)
        return

    global_token_a_id, global_token_b_id = combined.split(":")

    if action == "global_approve":
        await repo.global_merge_candidates.approve_candidate(UUID(global_token_a_id), UUID(global_token_b_id))
        await edit_message(query.message, "Одобрено")

    elif action == "global_decline":
        await repo.global_merge_candidates.decline_candidate(UUID(global_token_a_id), UUID(global_token_b_id))
        await query.message.delete()


# ============================================================================
# PAIR BLACKLIST (Global Token Pair Blacklist)
# ============================================================================

@router.callback_query(F.data == "noop")
async def noop_cb(query: CallbackQuery):
    """No-op callback for divider buttons."""
    await query.answer()


@router.callback_query(F.data == "show_pair_blacklist")
async def show_pair_blacklist_cb(query: CallbackQuery, repo: RequestsRepo):
    """Show all blacklisted global_token pairs."""
    pairs = await repo.global_token_pair_blacklist.get_all_blacklisted_pairs()

    if not pairs:
        await query.answer("Блоклист пар пуст", show_alert=True)
        return

    lines = ["🚫 Запрещённые пары для объединения:\n"]

    for pair in pairs:
        gt_a = pair.global_token_a
        gt_b = pair.global_token_b

        # Get sources for both global_tokens
        sources_a = [s.name for s in gt_a.sources] if gt_a.sources else []
        sources_b = [s.name for s in gt_b.sources] if gt_b.sources else []

        # Get market types
        market_types_a = set()
        market_types_b = set()

        for tc in gt_a.token_on_chains or []:
            if tc.token and tc.token.market_type:
                market_types_a.add(tc.token.market_type.value)

        for tc in gt_b.token_on_chains or []:
            if tc.token and tc.token.market_type:
                market_types_b.add(tc.token.market_type.value)

        lines.append(f"━━━━━━━━━━━━━━━━━━")
        lines.append(f"{hbold(gt_a.symbol)} ({', '.join(sources_a)}, {', '.join(market_types_a)})")
        lines.append(f"  ⚡ {hcode(str(pair.global_token_a_id)[:8])}...")
        lines.append(f"  ❌")
        lines.append(f"{hbold(gt_b.symbol)} ({', '.join(sources_b)}, {', '.join(market_types_b)})")
        lines.append(f"  ⚡ {hcode(str(pair.global_token_b_id)[:8])}...")

        if pair.reason:
            lines.append(f"📝 {pair.reason}")

        lines.append(f"🕒 {pair.created_at.strftime('%Y-%m-%d %H:%M')}")
        lines.append("")

    text = "\n".join(lines)

    # Split into chunks if too long (Telegram message limit ~4096 chars)
    if len(text) > 4000:
        await edit_message(query.message, text[:4000] + "\n\n...(обрезано)", reply_markup=admin_keyboard)
    else:
        await edit_message(query.message, text, reply_markup=admin_keyboard)

    await query.answer()


@router.callback_query(F.data == "add_pair_to_blacklist")
async def add_pair_cb(query: CallbackQuery, state: FSMContext):
    """Start adding pair to blacklist."""
    await edit_message(
        query.message,
        f"Введите пары для блокировки объединения.\n\n"
        f"Формат (каждая пара на новой строке):\n"
        f"{hcode('gt_a_id gt_b_id причина')}\n\n"
        f"Пример:\n"
        f"{hcode('123e4567-e89b-12d3-a456-426614174000 987fcdeb-51a2-43c1-b987-654321abcdef XT KEKE: FUTURES vs SPOT')}\n\n"
        f"ID можно получить через SQL или команду /find_token"
    )
    await state.set_state(PairBlacklistState.get_pair)


@router.message(PairBlacklistState.get_pair)
async def process_add_pair(message: Message, repo: RequestsRepo, state: FSMContext):
    """Process adding pairs to blacklist."""
    lines = message.text.strip().splitlines()
    if not lines:
        await message.answer(
            f"Неверный формат. Используйте:\n"
            f"{hcode('gt_a_id gt_b_id причина')}\n\n"
            f"Пример:\n"
            f"{hcode('123e4567-... 987fcdeb-... XT KEKE: FUTURES vs SPOT')}"
        )
        return

    processing_msg = await message.answer("⏳ Обработка...")

    added = []
    errors = []

    for line in lines:
        parts = line.strip().split(maxsplit=2)
        if len(parts) < 2:
            errors.append(f"❌ Неверный формат: {line[:50]}")
            continue

        gt_a_str = parts[0].strip()
        gt_b_str = parts[1].strip()
        reason = parts[2].strip() if len(parts) > 2 else None

        # Validate UUIDs
        try:
            gt_a_id = UUID(gt_a_str)
            gt_b_id = UUID(gt_b_str)
        except ValueError:
            errors.append(f"❌ Неверный UUID: {gt_a_str[:20]} или {gt_b_str[:20]}")
            continue

        # Get global tokens
        gt_a = await repo.global_tokens.get_by_id_with_meta(gt_a_id)
        gt_b = await repo.global_tokens.get_by_id_with_meta(gt_b_id)

        if not gt_a:
            errors.append(f"❌ Global token не найден: {gt_a_str[:20]}")
            continue

        if not gt_b:
            errors.append(f"❌ Global token не найден: {gt_b_str[:20]}")
            continue

        # Get sources
        sources_a = [s.name for s in gt_a.sources] if gt_a.sources else []
        sources_b = [s.name for s in gt_b.sources] if gt_b.sources else []

        # Get market types
        market_types_a = set()
        market_types_b = set()

        for tc in gt_a.token_on_chains or []:
            if tc.token and tc.token.market_type:
                market_types_a.add(tc.token.market_type.value)

        for tc in gt_b.token_on_chains or []:
            if tc.token and tc.token.market_type:
                market_types_b.add(tc.token.market_type.value)

        # Add to blacklist
        result = await repo.global_token_pair_blacklist.add_pair(
            gt_a_id,
            gt_b_id,
            reason=reason,
            created_by=f"tg_admin:{message.from_user.id}"
        )

        if result:
            added.append(
                f"✅ {gt_a.symbol} ({', '.join(sources_a)}, {', '.join(market_types_a)})\n"
                f"   ❌\n"
                f"   {gt_b.symbol} ({', '.join(sources_b)}, {', '.join(market_types_b)})\n"
                f"   📝 {reason or 'Без причины'}"
            )

            # Broadcast notification
            if message.from_user.id != 0:
                await broadcast(
                    message.bot,
                    [0],
                    f"Админ: {message.from_user.username} | {message.from_user.id} добавил пару в блоклист:\n"
                    f"{gt_a.symbol} ❌ {gt_b.symbol}\n"
                    f"Причина: {reason or '—'}"
                )
        else:
            added.append(f"⚠️ Пара уже в блоклисте: {gt_a.symbol} ❌ {gt_b.symbol}")

    # Delete processing message
    await processing_msg.delete()

    # Send results
    result_lines = []

    if added:
        result_lines.append("Добавлено:\n")
        result_lines.extend(added)

    if errors:
        result_lines.append("\n\nОшибки:")
        result_lines.extend(errors)

    if result_lines:
        text = "\n".join(result_lines)
        if len(text) > 4000:
            await message.answer(text[:4000] + "\n\n...(обрезано)", reply_markup=admin_keyboard)
        else:
            await message.answer(text, reply_markup=admin_keyboard)
    else:
        await message.answer("Ничего не добавлено", reply_markup=admin_keyboard)

    await state.clear()


@router.callback_query(F.data == "remove_pair_from_blacklist")
async def remove_pair_cb(query: CallbackQuery, repo: RequestsRepo):
    """Show list of pairs to remove."""
    pairs = await repo.global_token_pair_blacklist.get_all_blacklisted_pairs()

    if not pairs:
        await query.answer("Блоклист пар пуст", show_alert=True)
        return

    buttons = []
    for pair in pairs:
        gt_a = pair.global_token_a
        gt_b = pair.global_token_b

        # Get sources
        sources_a = [s.name for s in gt_a.sources] if gt_a.sources else []
        sources_b = [s.name for s in gt_b.sources] if gt_b.sources else []

        display_text = f"{gt_a.symbol} ({', '.join(sources_a[:2])}) ❌ {gt_b.symbol} ({', '.join(sources_b[:2])})"

        buttons.append([InlineKeyboardButton(
            text=display_text[:60],  # Telegram button text limit
            callback_data=f"del_pair:{pair.id}"
        )])

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await edit_message(query.message, "Выберите пару для удаления:", reply_markup=kb)


@router.callback_query(F.data.startswith("del_pair:"))
async def delete_pair_cb(query: CallbackQuery, repo: RequestsRepo):
    """Delete pair from blacklist."""
    pair_id = query.data[9:]

    pair = await repo.global_token_pair_blacklist.get(pair_id)
    if not pair:
        await query.answer("Пара уже удалена", show_alert=True)
        return

    gt_a = pair.global_token_a
    gt_b = pair.global_token_b

    # Remove pair
    removed = await repo.global_token_pair_blacklist.remove_pair(
        pair.global_token_a_id,
        pair.global_token_b_id
    )

    if removed:
        await query.message.answer(
            f"✅ Удалено из блоклиста:\n"
            f"{gt_a.symbol} ❌ {gt_b.symbol}",
            reply_markup=admin_keyboard
        )
        await query.answer("Удалено")
    else:
        await query.answer("Ошибка при удалении", show_alert=True)

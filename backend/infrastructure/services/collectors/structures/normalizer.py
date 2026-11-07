import re
from datetime import datetime
from itertools import islice
from typing import Any
from uuid import UUID

import orjson

from infrastructure.services.collectors.structures.maps import source_chain_map, dexscreener_chain_map, db_chain_map, \
    reverse_db_chain_map


def normalize_contract_address(address: str | None) -> str | None:
    """
    Clean contract address preserving original case.

    We preserve original case for ALL chains because:
    - Solana uses Base58 where case matters for correctness
    - EVM checksummed addresses use case for validation
    - Other chains may have their own case requirements

    Args:
        address: The contract address

    Returns:
        Cleaned address (preserving case) or None
    """
    if not address:
        return None

    addr = address.strip()
    # Filter garbage values
    if addr.lower() in {"", "null", "none", "0x0", "—"}:
        return None

    return addr


def addresses_match(addr1: str | None, addr2: str | None) -> bool:
    """
    Compare two contract addresses using case-insensitive comparison.

    While we preserve original case for display/links, comparison
    should be case-insensitive to match addresses across exchanges
    that may use different casing conventions.

    Args:
        addr1: First address
        addr2: Second address

    Returns:
        True if addresses match (case-insensitive)
    """
    if addr1 is None or addr2 is None:
        return addr1 == addr2

    return addr1.strip().lower() == addr2.strip().lower()


class ChainNormalizer:
    def __init__(
            self,
            source_map: dict[str, str],
            dex_map: dict[str, str],
            db_map: dict[str, str],
            reverse_db_map: dict[str, str]
    ):
        self.source_map = {k.lower(): v.lower() for k, v in source_map.items()}
        self.dex_map = {k.lower(): v.lower() for k, v in dex_map.items()}
        self.db_map = {k.lower(): v.lower() for k, v in db_map.items()}
        self.reverse_db_map = {k.lower(): v.lower() for k, v in reverse_db_map.items()}

    def normalize_from_source(self, chain: str) -> str | None:
        """Всегда возвращает название сети в нижнем регистре"""
        chain = chain.strip().lower()
        return self.source_map.get(chain, chain)

    def normalize_from_dex(self, chain: str) -> str | None:
        """Всегда возвращает название сети в нижнем регистре"""
        chain = chain.strip().lower()
        return self.dex_map.get(chain, None)

    def normalize_from_cex(self, chain: str) -> str | None:
        """Всегда возвращает название сети в нижнем регистре"""
        chain = chain.strip().lower()
        return self.dex_map.get(chain, chain)

    def normalize_to_db(self, standard_chain: str) -> str | None:
        """Всегда возвращает название сети в нижнем регистре"""
        standard_chain = standard_chain.strip().lower()
        return self.db_map.get(standard_chain, standard_chain)

    def normalize_to_signal(self, db_chain: str) -> str | None:
        """Всегда возвращает название сети в нижнем регистре"""
        db_chain = db_chain.strip().lower()
        return self.reverse_db_map.get(db_chain, db_chain)


def get_chain_normalizer():
    return ChainNormalizer(
        source_map=source_chain_map,
        dex_map=dexscreener_chain_map,
        db_map=db_chain_map,
        reverse_db_map=reverse_db_chain_map
    )


def normalize_signal_to_db(signal_chain: str) -> str:
    """
    Converts signal chain name (BSC, ETH, SOL) to database chain name (binance-smart-chain, ethereum, solana).
    Input is case-insensitive, output is always lowercase.

    Examples:
        BSC -> binance-smart-chain
        bsc -> binance-smart-chain
        ETH -> ethereum
        binance-smart-chain -> binance-smart-chain (passthrough if already in DB format)
    """
    signal_chain = signal_chain.strip().lower()
    # Create lowercase mapping from db_chain_map
    mapping = {k.lower(): v.lower() for k, v in db_chain_map.items()}
    # Return mapped value or original (in case it's already in db format)
    return mapping.get(signal_chain, signal_chain)


def safe_float(value: str | float | None, default: float = 0.0) -> float | None:
    try:
        if value in (None, "None", ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(val) -> int | None:
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def from_ms_timestamp(ms: int | None) -> datetime | None:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=None)
    except (ValueError, TypeError):
        return None


def default_serializer(obj):
    """Serializer for orjson (raises TypeError for unknown types)."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    if obj.__class__.__name__ == "UUID":
        return str(obj)
    raise TypeError


def msgpack_default(obj):
    """Serializer for msgpack (returns converted value or raises TypeError)."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    # Handle asyncpg UUID objects
    if obj.__class__.__name__ == "UUID":
        return str(obj)
    # Let msgpack raise its own error for unknown types
    return obj


def batched(iterable, n):
    """Разбивает список на батчи фиксированного размера."""
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch


def check_dex_price(pair: dict) -> bool:
    """Проверка на аномальную цену из DexScreener."""
    liquidity = safe_float(pair.get("liquidity", {}).get("usd"))
    price_change = pair.get("priceChange", {})
    volume = pair.get("volume", {})

    for interval in ["m5", "h1", "h6"]:
        percent = safe_float(price_change.get(interval))
        vol = safe_float(volume.get(interval))
        if percent is not None and vol is not None:
            break
    else:
        return False

    if liquidity is None:
        return False

    _test = 0.3 * abs(percent) * liquidity / 2 / 100
    return vol > _test


def normalize_dex_pair_item(item: dict) -> dict[str | Any, float | None | int | Any]:
    return {
        "price_usd": safe_float(item.get("priceUsd")),
        "txns_m5": safe_int(
            item.get("txns", {}).get("m5", {}).get("buys")) + safe_int(
            item.get("txns", {}).get("m5", {}).get("sells")),
        "txns_h1": safe_int(
            item.get("txns", {}).get("h1", {}).get("buys")) + safe_int(
            item.get("txns", {}).get("h1", {}).get("sells")),
        "txns_h6": safe_int(
            item.get("txns", {}).get("h6", {}).get("buys")) + safe_int(
            item.get("txns", {}).get("h6", {}).get("sells")),
        "txns_h24": safe_int(
            item.get("txns", {}).get("h24", {}).get("buys")) + safe_int(
            item.get("txns", {}).get("h24", {}).get("sells")),
        "volume_m5": safe_float(item.get("volume", {}).get("m5")),
        "volume_h1": safe_float(item.get("volume", {}).get("h1")),
        "volume_h6": safe_float(item.get("volume", {}).get("h6")),
        "volume_h24": safe_float(item.get("volume", {}).get("h24")),
        "price_change_m5": safe_float(item.get("priceChange", {}).get("m5")),
        "price_change_h1": safe_float(item.get("priceChange", {}).get("h1")),
        "price_change_h6": safe_float(item.get("priceChange", {}).get("h6")),
        "price_change_h24": safe_float(item.get("priceChange", {}).get("h24")),
        "liquidity_usd": safe_float(item.get("liquidity", {}).get("usd")),
        "liquidity_base": safe_float(item.get("liquidity", {}).get("base")),
        "liquidity_quote": safe_float(item.get("liquidity", {}).get("quote")),
        "fdv": safe_float(item.get("fdv")),
        "market_cap": safe_float(item.get("marketCap")),
        "pair_created_at": from_ms_timestamp(item.get("pairCreatedAt")),
        "chain_name": item.get("chainId"),
        "base_address": item.get("baseToken", {}).get("address"),
        "quote_address": item.get("quoteToken", {}).get("address"),
        "pair_address": item.get("pairAddress"),
        "url": item.get("url"),
    }


def parse_multiplier(ticker: str) -> tuple[int, str]:
    """
    Парсит мультипликатор из тикера.
    Мультипликаторы начинаются с '1' и состоят из цифр, например:
    - 1000BONK -> (1000, BONK)
    - 10PEPE -> (10, PEPE)
    - 4EVER -> (1, 4EVER) # часть имени
    - 200M -> (1, 200M) # не мультипликатор, т.к. не начинается с '1'
    """
    # защита от 3L / 3S токенов
    if re.match(r".*\d+[LS]$", ticker):
        return 1, ticker

    # ищем мультипликатор, начинающийся с '1'
    m = re.match(r"^(1\d+)([A-Z][A-Z0-9]*)$", ticker)
    if m:
        num, rest = m.groups()
        return int(num), rest

    return 1, ticker


def normalize_cex_price(price: float | None, ticker: str) -> float:
    """нормализует цену по тикеру"""
    if price is None:
        return 0
    multiplier, clear_ticker = parse_multiplier(ticker)
    return price / multiplier


def parse_tax_from_tip(tip: str | None) -> float | None:
    """парсит комиссию (%) из текста подсказки"""
    if not tip:
        return None

    patterns = [
        r"Please note that .* (\d+)% fee for.*",
        r"Please note that (?:\S+ will charge a|there will be an additional) (\d+)%.*"
    ]

    for pattern in patterns:
        match = re.search(pattern, tip)

        if match:
            result = float(match.group(1))
            return result

    return None


def normalize_pair(chain_name: str | None, contract_address: str | None) -> tuple[str, str | None]:
    """
    Приводит пару (chain_name, contract_address) к единому детерминированному виду для сравнения:
      - chain_name → str.lower().strip()
      - contract_address → str.lower().strip() (для корректного set-lookup)
        - None, если пустая строка / 'null' / None
    """
    # нормализация сети
    chain = (chain_name or "").strip().lower()

    # нормализация адреса - приводим к нижнему регистру для сравнений
    addr_raw = normalize_contract_address(contract_address)
    addr = addr_raw.lower() if addr_raw else None

    return chain, addr


def normalize_orderbook(symbol: str, data: dict) -> dict:
    multiplier, _ = parse_multiplier(symbol)
    ob = data["orderbook"]
    if isinstance(ob, str):
        ob = orjson.loads(ob)

    def normalize_levels(levels):
        result = []
        for lvl in levels:
            if len(lvl) == 2:
                price = float(lvl[0]) / multiplier
                volume = float(lvl[1])
            else:
                price = float(lvl[0]) / multiplier
                volume = float(lvl[1]) * float(lvl[2])
            result.append([price, volume])
        return result

    data["orderbook"] = {
        "asks": normalize_levels(ob.get("asks", ())),
        "bids": normalize_levels(ob.get("bids", ()))
    }
    return data


def normalize_chains(chains, bid_price, bid_volume):
    for c in chains:
        c["withdrawal_fee"] = safe_float(c.get("withdrawal_fee"))
        c["withdraw_tax"] = safe_float(c.get("withdraw_tax"))
        c["deposit_tax"] = safe_float(c.get("deposit_tax"))
        c["withdrawal_fee_in_usdt"] = c["withdrawal_fee"] * bid_price
        c["cex_withdrawal_fee"] = bid_volume * c["withdraw_tax"] + c["withdrawal_fee_in_usdt"]
        c["cex_deposit_fee"] = (bid_volume - c["cex_withdrawal_fee"]) * c["deposit_tax"]
        c["total_tax_fee_in_token"] = c["cex_withdrawal_fee"] + c["cex_deposit_fee"]
    return chains

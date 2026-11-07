# Методичка: Добавление новой DEX биржи

## Обзор

Этот документ описывает **полный пошаговый процесс** интеграции новой DEX биржи в систему сбора данных. Написан на основе реального опыта интеграции GMGN (REST + WebSocket) с учётом всех проблем, решений и критических моментов.

**⚠️ КРИТИЧНО:** DEX интеграция **ПРИНЦИПИАЛЬНО ОТЛИЧАЕТСЯ** от CEX! Не пытайтесь использовать CEX методичку для DEX.

**📅 Обновлено:** 2026-02-03 на основе успешной WebSocket интеграции GMGN

---

## Содержание

1. [Ключевые отличия DEX от CEX](#1-ключевые-отличия-dex-от-cex)
2. [Предварительный анализ API](#2-предварительный-анализ-api)
3. [Выбор подхода: WebSocket vs REST](#3-выбор-подхода-websocket-vs-rest)
4. [Создание базового DEX парсера](#4-создание-базового-dex-парсера)
5. [WebSocket имплементация (рекомендуемая)](#5-websocket-имплементация-рекомендуемая)
6. [REST имплементация (fallback)](#6-rest-имплементация-fallback)
7. [Обход защиты Cloudflare](#7-обход-защиты-cloudflare)
8. [Интеграция в Spread Analyzer](#8-интеграция-в-spread-analyzer)
9. [Регистрация в менеджерах](#9-регистрация-в-менеджерах)
10. [Настройка Docker](#10-настройка-docker)
11. [Маппинг сетей (chains)](#11-маппинг-сетей-chains)
12. [Тестирование и деплой](#12-тестирование-и-деплой)
13. [Типичные проблемы и решения](#13-типичные-проблемы-и-решения)
14. [Чеклист интеграции](#14-чеклист-интеграции)
15. [Обеспечение работы сигналов (КРИТИЧНО!)](#15-обеспечение-работы-сигналов-критично)
16. [Итоговый поток данных DEX (визуализация)](#16-итоговый-поток-данных-dex-визуализация)

---

## 1. Ключевые отличия DEX от CEX

### 1.1 Архитектурные различия

| Аспект | CEX | DEX |
|--------|-----|-----|
| **Идентификация токена** | `symbol` (BTCUSDT) | `chain` + `contract_address` |
| **MarketType** | `SPOT` / `FUTURES` | Не используется (только цепи) |
| **Источник данных** | Centralized orderbook | On-chain / aggregators |
| **Формат цены** | Orderbook (asks/bids) | **Одна цена** (конвертируется в orderbook) |
| **Базовый класс** | `BaseParser` | `BaseDexPriceParser` |
| **Redis stream** | `prices:{source}:{market_type}` | `dex_prices:{source}` |
| **Distributor** | Нужен (загружает токены) | **НЕ НУЖЕН** |
| **Real-time updates** | WebSocket (опционально) | **WebSocket (предпочтительно)** |

### 1.2 Поток данных

**CEX поток:**
```
Price Collector → Redis (prices:mexc:spot)
                       ↓
                 Distributor (загружает токены из БД)
                       ↓
                 Redis (available_tokens:mexc:spot)
                       ↓
                 Spread Loader (читает orderbooks)
                       ↓
                 Redis (orderbooks:updates stream)
                       ↓
                 Spread Collector (анализ арбитража)
```

**DEX поток (НОВЫЙ!):**
```
Price Collector → Redis (dex_prices:gmgn)
                       ↓
                 Spread Loader (читает DEX streams + сопоставляет по chain+address)
                       ↓
                 Redis (orderbooks:updates stream)
                       ↓
                 Spread Collector (анализ арбитража)
```

**⚠️ ВАЖНО:** DEX данные НЕ идут через Distributor! Loader читает напрямую из `dex_prices:*` streams.

---

## 2. Предварительный анализ API

### 2.1 Что нужно найти

| Информация | Где искать | Пример (GMGN) |
|------------|-----------|---------------|
| **Список поддерживаемых цепей** | Документация / сайт | Solana, Ethereum, BSC, Base, Arbitrum, Polygon |
| **REST API для цен** | Developer docs | `GET /defi/quotation/v1/tokens/{chain}?address={token_address}` |
| **WebSocket API** | Developer docs | `wss://gmgn.ai/ws` |
| **Python библиотека** | PyPI / GitHub | `gmgnapi` (если существует) |
| **WebSocket каналы** | Документация | `new_pair_update`, `new_pool_info` |
| **Rate limits** | Заголовки ответов | Обычно нет лимитов или очень высокие |
| **Cloudflare защита** | Попробовать curl | 403 Forbidden = есть защита |
| **Формат contract address** | Примеры | Lowercase, с/без 0x префикса |

### 2.2 Проверка API вручную

```bash
# 1. Проверка REST доступности (может вернуть 403 из-за Cloudflare)
curl -s "https://gmgn.ai/defi/quotation/v1/tokens/sol?address=..." | jq '.'

# 2. Если 403 - нужен curl_cffi (см. раздел 7)

# 3. Проверка WebSocket доступности
wscat -c "wss://gmgn.ai/ws"

# 4. Поиск Python библиотеки
pip search gmgn
# Или поиск на GitHub: https://github.com/search?q=gmgn+api

# 5. Проверка структуры ответа
curl -H "User-Agent: Mozilla/5.0..." \
     "https://gmgn.ai/defi/quotation/v1/tokens/eth?address=0x..." | jq '.data.tokens[0]'
```

### 2.3 Определение формата данных

**Типичный REST ответ:**
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "tokens": [{
      "chain": "sol",
      "address": "g1vh...",
      "symbol": "MEGA",
      "price": 0.00035317,
      "volume_24h": 58107237.62,
      "liquidity": 105100.30,
      "market_cap": 1234567.89
    }]
  }
}
```

**Типичный WebSocket ответ (new_pair_update):**
```json
[
  {
    "_v_ch": "stat",  // КРИТИЧНО: только "stat" содержат цены!
    "a": "token_address",
    "n": "sol",
    "p": 0.000300475733663272,  // PRICE!
    "mc": "300475.73",
    "lq": 107499.04,
    "v24h": "39744534.65",
    "s": "TOKEN"
  }
]
```

**⚠️ КРИТИЧНО:** У DEX НЕТ orderbook в традиционном смысле! Есть только **одна цена**. Нужно конвертировать её в orderbook формат.

---

## 3. Выбор подхода: WebSocket vs REST

### 3.1 Матрица решений (ОБНОВЛЕНО!)

| Критерий | WebSocket | REST (Polling) |
|----------|-----------|----------------|
| **Когда использовать** | **ВСЕГДА, если доступен** | Только если WebSocket не работает |
| **Преимущества** | Низкая задержка, реал-тайм, меньше нагрузки | Гарантированные данные, проще отладка |
| **Недостатки** | Сложнее имплементация, нужна библиотека | Высокая нагрузка, нужны прокси, медленнее |
| **Производительность** | **~37 updates/sec** (GMGN реальные данные) | ~3-5 updates/sec |
| **Сложность** | Средняя (с библиотекой) - Высокая (без) | Низкая-Средняя |

### 3.2 Наш опыт с GMGN (КРИТИЧНО!)

**⚠️ ВАЖНО:** Изначальная оценка WebSocket была **НЕВЕРНОЙ**! WebSocket работает отлично для GMGN.

**Попытка 1: REST API polling**
```python
# ✅ СРАБОТАЛО, но МЕДЛЕННО
# Endpoint: GET /defi/quotation/v1/tokens/{chain}?address={token_address}
# Интервал: 10 секунд
# Батчи: По 100 токенов
# Результат: ~3-5 updates/sec, стабильно, но медленно
# Проблема: Высокая нагрузка, нужны прокси, 8462 токена = долго
```

**Попытка 2: WebSocket (ФИНАЛЬНОЕ РЕШЕНИЕ)**
```python
# ✅✅✅ СРАБОТАЛО ОТЛИЧНО!
# Библиотека: gmgnapi (https://github.com/ChipaDevTeam/GmGnAPI)
# Каналы: new_pair_update (stat updates), new_pool_info
# Результат: ~37 updates/sec, 1000 updates за 27 секунд!
# Токены: 8,462 → индекс из 3,359 уникальных (chain, address) пар
# Статистика: 1000 updates, 57 saved, 0 errors
```

**🔥 КРИТИЧЕСКИЕ ОТКРЫТИЯ:**

1. **Документация библиотеки НЕВЕРНА!** В README указаны методы, которых нет:
   - ❌ `subscribe_token_updates()` - НЕ СУЩЕСТВУЕТ
   - ❌ `@client.on_token_update` - НЕ СУЩЕСТВУЕТ
   - ✅ `subscribe_pair_updates()` - ПРАВИЛЬНЫЙ
   - ✅ `client.on("new_pair_update", handler)` - ПРАВИЛЬНЫЙ

2. **Async generator pattern:** `client.listen()` возвращает async generator, нужен `async for`

3. **Data structure:** WebSocket возвращает не dict напрямую, а Message object с атрибутами `.channel` и `.data`

4. **Channel filtering:** Только updates с `"_v_ch": "stat"` содержат актуальные цены!

5. **Token indexing:** Критично создать индекс `(chain, address) -> (symbol, token_id)` для быстрого поиска

### 3.3 Рекомендация (ОБНОВЛЕНО!)

**Используйте WebSocket (ПРЕДПОЧТИТЕЛЬНО!)**, если:
- ✅ У биржи есть WebSocket API
- ✅ Существует Python библиотека (проверить GitHub/PyPI)
- ✅ Документация упоминает real-time каналы
- ✅ Производительность критична (8000+ токенов)

**Используйте REST polling (FALLBACK)**, если:
- ✅ WebSocket API отсутствует
- ✅ Нет готовой библиотеки (писать с нуля сложно)
- ✅ Небольшое количество токенов (<1000)
- ✅ Нужна простая отладка

---

## 4. Создание базового DEX парсера

### 4.1 Структура файлов

```
backend/infrastructure/services/collectors/
├── price_collector/
│   ├── base_dex.py                    # Базовый класс для DEX
│   ├── sources/
│   │   ├── gmgn_rest.py              # REST имплементация (fallback)
│   │   ├── gmgn_ws_working.py        # WebSocket имплементация (ГЛАВНАЯ)
│   │   └── [новая_биржа]_ws.py       # Ваша биржа (WebSocket)
│   │   └── [новая_биржа]_rest.py     # Ваша биржа (REST fallback)
│   └── manager.py                     # Регистрация источников
├── structures/
│   └── maps.py                        # CHAIN_MAP для маппинга
└── common/
    └── gmgn_client.py                 # HTTP клиент (если нужен Cloudflare bypass)
```

### 4.2 Базовый класс: BaseDexPriceParser

**Файл:** `backend/infrastructure/services/collectors/price_collector/base_dex.py`

**Ключевые методы:**

```python
class BaseDexPriceParser:
    def __init__(self, cache, repo, source_name, supported_chains):
        """
        Args:
            source_name: Имя источника (gmgn, dexscreener)
            supported_chains: Список поддерживаемых цепей
        """
        self.source_name = source_name
        self.supported_chains = supported_chains  # ["ethereum", "solana", ...]
        self.source = None  # Будет загружен из БД

    async def setup(self):
        """Загрузить source из БД"""
        self.source = await self.repo.sources.get_by_name(self.source_name)

    async def load_tokens_by_chain(self) -> dict[str, list[tuple]]:
        """
        Загрузить токены из БД, сгруппированные по цепям.

        Returns:
            {
                "ethereum": [
                    (contract_address, symbol, token_id),
                    ...
                ],
                "solana": [...]
            }
        """
        # Читает из БД токены с token_chains для этого источника

    async def save_price_update(self, symbol, price_data, timestamp):
        """
        Сохранить обновление цены в Redis stream.

        КРИТИЧНО: Конвертирует одну цену в orderbook формат!
        """
        price = price_data.get("price", 0)
        volume = price_data.get("volume_24h", 0) / 100 / 2 / price if price > 0 else 0

        # Формат как у dexscreener
        orderbook = {
            "asks": [[price, volume]],
            "bids": [[price, volume]]
        }

        # Сохранить в stream dex_prices:{source_name}
        stream_key = f"dex_prices:{self.source_name}"
        data = {
            "symbol": symbol,
            "price": str(price),
            "orderbook": str(orderbook),  # Строка! Loader парсит через ast.literal_eval
            "chain": price_data.get("chain"),
            "contract_address": price_data.get("contract_address"),
            "token_id": str(price_data.get("token_id")),
            "timestamp": str(timestamp),
        }
        await self.cache.client.xadd(stream_key, data)
```

**⚠️ КРИТИЧНЫЕ МОМЕНТЫ:**

1. **Orderbook формат**: DEX возвращает одну цену, но система ожидает orderbook. Используйте формулу:
   ```python
   volume = liquidity_usd / 100 / 2 / price if price else 0
   orderbook = {"asks": [[price, volume]], "bids": [[price, volume]]}
   ```

2. **Stream key**: Всегда `dex_prices:{source_name}`, БЕЗ market_type!

3. **Данные как строки**: Всё в Redis сохраняется как строки, включая orderbook (используется `str()`)

---

## 5. WebSocket имплементация (рекомендуемая)

### 5.1 Поиск Python библиотеки

**ШАГ 1:** Проверить существование библиотеки

```bash
# PyPI
pip search <биржа>

# GitHub
# Поиск: "<биржа> api python"
# Пример: "gmgn api python" → https://github.com/ChipaDevTeam/GmGnAPI
```

**ШАГ 2:** Установить и протестировать

```bash
# Если на GitHub
poetry add git+https://github.com/user/repo.git

# Если на PyPI
poetry add <package-name>

# Тест подключения
python
>>> from <package> import Client
>>> client = Client()
>>> # Попробовать методы из README
```

**⚠️ КРИТИЧНО:** Документация библиотек часто **УСТАРЕВШАЯ** или **НЕВЕРНАЯ**! Всегда:
1. Читайте исходный код библиотеки (файлы `client.py`, `__init__.py`)
2. Ищите реальные методы через `dir(client)`
3. Проверяйте signature методов через `inspect.signature()`
4. Создавайте тестовые скрипты для проверки работы

### 5.2 Изучение структуры данных

**Создать тестовый скрипт:**

```python
"""test_<биржа>_structure.py - Исследование WebSocket API"""

import asyncio
import json
from <package> import Client

async def inspect_websocket():
    async with Client() as client:
        # Подписаться на канал
        await client.subscribe_<channel>(chain="sol")

        count = 0
        async for message in client.listen():
            count += 1

            # Тип объекта
            print(f"Type: {type(message)}")
            print(f"Module: {type(message).__module__}")

            # Атрибуты
            attrs = [a for a in dir(message) if not a.startswith('_')]
            print(f"Attributes: {attrs}")

            # Попытка доступа к данным
            for attr in ['channel', 'data', 'payload', 'message']:
                if hasattr(message, attr):
                    value = getattr(message, attr)
                    print(f"{attr}: {type(value)}")

                    if isinstance(value, (dict, list)):
                        print(json.dumps(value, indent=2, default=str)[:500])

            if count >= 5:
                break

asyncio.run(inspect_websocket())
```

**🔥 ПРИМЕР: GMGN WebSocket**

```python
# Реальная структура данных GMGN:

# Message object
type(message) = <class 'gmgnapi.models.Message'>
message.channel = "new_pair_update"  # Строка
message.data = [...]  # Список словарей

# Data structure для "new_pair_update"
[
  {
    "_v_ch": "stat",  # ФИЛЬТР: только "stat" имеют цены!
    "a": "address",
    "n": "sol",
    "p": 0.000300475733663272,  # Цена
    "mc": "300475.73",
    "lq": 107499.04,
    "v24h": "39744534.65",
    "s": "TOKEN"
  }
]

# Data structure для "new_pool_info"
[
  {
    "c": "sol",
    "p": [
      {
        "bti": {
          "p": 0.000123,  # Цена
          "mc": 12345,
          "v24h": 123,
          "s": "TOKEN"
        },
        "ba": "token_address"
      }
    ]
  }
]
```

### 5.3 WebSocket имплементация

**Файл:** `backend/infrastructure/services/collectors/price_collector/sources/gmgn_ws_working.py`

**ПОЛНЫЙ РАБОЧИЙ КОД:**

```python
"""GMGN WebSocket price collector - WORKING VERSION with real prices!"""

import asyncio
import time
from typing import Any

from gmgnapi import GmGnClient

from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base_dex import BaseDexPriceParser
from infrastructure.services.collectors.structures.maps import GMGN_CHAIN_MAP
from infrastructure.services.common.cache import RedisCache
from settings import logger


class GmgnWSPriceParser(BaseDexPriceParser):
    """
    GMGN WebSocket price collector.

    Uses gmgnapi library to subscribe to real-time price updates.
    Channels:
    - new_pair_update: Real-time token updates with prices
    - new_pool_info: New pool launches with initial prices
    """

    def __init__(self, cache: RedisCache, repo: RequestsRepo):
        # GMGN supports same chains as defined in GMGN_CHAIN_MAP
        supported_chains = list(GMGN_CHAIN_MAP.keys())

        super().__init__(
            cache=cache,
            repo=repo,
            source_name="gmgn",
            supported_chains=supported_chains,
        )
        self._running = False
        self._token_map = {}  # {(chain, address): (symbol, token_id)}
        self._stats = {"updates": 0, "saved": 0, "errors": 0}

    async def start(self) -> None:
        """Start WebSocket price collection for all supported chains."""
        await self.setup()

        if not self.source:
            raise ValueError(f"Source {self.source_name} not found in database")

        # Load tokens from DB
        tokens_by_chain = await self.load_tokens_by_chain()

        if not tokens_by_chain:
            logger.warning(
                f"No tokens found for {self.source_name} in supported chains: {self.supported_chains}"
            )
            return

        # Build token index for fast lookup: (chain, address) -> (symbol, token_id)
        for chain_name, tokens in tokens_by_chain.items():
            gmgn_chain = GMGN_CHAIN_MAP.get(chain_name)
            if not gmgn_chain:
                continue

            for addr, symbol, token_id in tokens:
                key = (gmgn_chain, addr.lower())
                self._token_map[key] = (symbol, token_id)

        logger.info(
            f"GMGN WS: Loaded {len(self._token_map)} tokens from {len(tokens_by_chain)} chains"
        )

        # Start WebSocket client
        self._running = True

        try:
            async with GmGnClient() as client:
                logger.info("GMGN WS: Client connected")

                # Subscribe to all chains
                for chain_name in tokens_by_chain.keys():
                    gmgn_chain = GMGN_CHAIN_MAP.get(chain_name)
                    if gmgn_chain:
                        # Subscribe to pair updates (main channel with prices)
                        await client.subscribe_pair_updates(chain=gmgn_chain)
                        logger.info(f"GMGN WS: Subscribed to pair_updates for {gmgn_chain}")

                        # Subscribe to new pools (for new token launches)
                        await client.subscribe_new_pools(chain=gmgn_chain)
                        logger.info(f"GMGN WS: Subscribed to new_pools for {gmgn_chain}")

                # Register handlers
                client.on("new_pair_update", self._handle_pair_update)
                client.on("new_pool_info", self._handle_new_pool)

                logger.info("GMGN WS: Event handlers registered")

                # Start listening
                logger.info("GMGN WS: Starting message loop")

                async for message in client.listen():
                    if not self._running:
                        break

                    # Log stats every 1000 updates
                    if self._stats["updates"] % 1000 == 0 and self._stats["updates"] > 0:
                        logger.info(
                            f"GMGN WS: {self._stats['updates']} updates, "
                            f"{self._stats['saved']} saved, "
                            f"{self._stats['errors']} errors"
                        )

        except Exception as e:
            logger.error(f"GMGN WS: Fatal error: {e}", exc_info=True)
            self._running = False

    async def _handle_pair_update(self, data: list[dict]) -> None:
        """
        Handle pair update events.

        Data format:
        [
            {
                "_v_ch": "stat",  // stat updates have prices
                "a": "address",
                "n": "sol",
                "p": 0.000123,  // price in USD
                "mc": "123456",  // market cap
                "lq": 12345,  // liquidity
                "v24h": "123456",  // volume 24h
                "s": "TOKEN",  // symbol
                ...
            }
        ]
        """
        try:
            self._stats["updates"] += 1

            if not isinstance(data, list):
                return

            for item in data:
                if not isinstance(item, dict):
                    continue

                # Only process stat updates (they have prices)
                if item.get("_v_ch") != "stat":
                    continue

                await self._process_price_update(item)

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"GMGN WS: Error in pair_update handler: {e}", exc_info=True)

    async def _handle_new_pool(self, data: list[dict]) -> None:
        """
        Handle new pool events.

        Data format:
        [
            {
                "c": "sol",
                "p": [
                    {
                        "bti": {
                            "p": 0.000123,  // price
                            "mc": 12345,
                            "v24h": 123,
                            "s": "TOKEN",
                            ...
                        },
                        "ba": "token_address",
                        ...
                    }
                ]
            }
        ]
        """
        try:
            self._stats["updates"] += 1

            if not isinstance(data, list):
                return

            for item in data:
                if not isinstance(item, dict):
                    continue

                chain = item.get("c", "")
                pools = item.get("p", [])

                if not isinstance(pools, list):
                    continue

                for pool in pools:
                    if not isinstance(pool, dict):
                        continue

                    bti = pool.get("bti", {})
                    if not isinstance(bti, dict):
                        continue

                    # Extract price from bti
                    price = self._safe_float(bti.get("p"))
                    if price <= 0:
                        continue

                    # Get token address
                    address = pool.get("ba", "").lower()
                    if not address:
                        continue

                    # Look up in our token map
                    key = (chain, address)
                    if key not in self._token_map:
                        continue

                    symbol, token_id = self._token_map[key]

                    # Build price update
                    price_update = {
                        "price": price,
                        "volume_24h": self._safe_float(bti.get("v24h", 0)),
                        "market_cap": self._safe_float(bti.get("mc", 0)),
                        "liquidity": 0,
                        "chain": chain,
                        "contract_address": address,
                        "token_id": token_id,
                    }

                    # Save to Redis
                    timestamp = int(time.time() * 1000)
                    await self.save_price_update(symbol, price_update, timestamp)
                    self._stats["saved"] += 1

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"GMGN WS: Error in new_pool handler: {e}", exc_info=True)

    async def _process_price_update(self, item: dict) -> None:
        """Process a single price update from pair_update channel."""
        try:
            # Extract data
            address = item.get("a", "").lower()
            chain = item.get("n", "")
            price = self._safe_float(item.get("p"))

            if not address or not chain or price <= 0:
                return

            # Look up in our token map
            key = (chain, address)
            if key not in self._token_map:
                return

            symbol, token_id = self._token_map[key]

            # Build price update
            price_update = {
                "price": price,
                "volume_24h": self._safe_float(item.get("v24h", 0)),
                "market_cap": self._safe_float(item.get("mc", 0)),
                "liquidity": self._safe_float(item.get("lq", 0)),
                "chain": chain,
                "contract_address": address,
                "token_id": token_id,
            }

            # Save to Redis stream
            timestamp = int(time.time() * 1000)
            await self.save_price_update(symbol, price_update, timestamp)
            self._stats["saved"] += 1

        except Exception as e:
            self._stats["errors"] += 1
            logger.debug(f"GMGN WS: Error processing price update: {e}")

    async def stop(self) -> None:
        """Stop WebSocket client."""
        self._running = False

        logger.info(
            f"GMGN WS: Stopping. Final stats: "
            f"{self._stats['updates']} updates, "
            f"{self._stats['saved']} saved, "
            f"{self._stats['errors']} errors"
        )

    @staticmethod
    def _safe_float(value: Any) -> float:
        """Convert to float safely."""
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
```

**🔥 КРИТИЧНЫЕ МОМЕНТЫ WebSocket:**

1. **Token indexing:** Создайте `{(chain, address): (symbol, token_id)}` индекс ПЕРЕД подпиской
2. **Context manager:** Используйте `async with Client()` для автоматического cleanup
3. **Async generator:** `async for message in client.listen()` - НЕ `await client.listen()`
4. **Event handlers:** `client.on("channel_name", handler)` - НЕ декораторы!
5. **Channel filtering:** Проверяйте `_v_ch == "stat"` для price updates
6. **Statistics:** Логируйте каждые 1000 updates для мониторинга
7. **Error handling:** Try-except в каждом handler, иначе весь цикл упадёт

### 5.4 Тестирование WebSocket

```bash
# 1. Создать тестовый скрипт
cat > test_<биржа>_ws.py << 'EOF'
import asyncio
from infrastructure.services.collectors.price_collector.sources.<биржа>_ws import <Биржа>WSPriceParser
from infrastructure.services.common.cache import RedisCache
from infrastructure.database.requests import RequestsRepo

async def test():
    cache = RedisCache(...)
    repo = RequestsRepo(...)

    parser = <Биржа>WSPriceParser(cache, repo)

    try:
        await parser.start()
    except KeyboardInterrupt:
        await parser.stop()

asyncio.run(test())
EOF

# 2. Запустить на 60 секунд
timeout 60 python test_<биржа>_ws.py

# 3. Ожидаемые логи:
# ✅ Loaded X tokens from Y chains
# ✅ Client connected
# ✅ Subscribed to channels
# ✅ Starting message loop
# ✅ 1000 updates, 57 saved, 0 errors

# 4. Проверить Redis
docker exec redis redis-cli XLEN dex_prices:<биржа>
# Должно быть > 0
```

---

## 6. REST имплементация (fallback)

**ИСПОЛЬЗУЙТЕ ТОЛЬКО ЕСЛИ:**
- ❌ WebSocket API недоступен
- ❌ Python библиотека отсутствует
- ❌ WebSocket требует сложную аутентификацию

**Полный код REST имплементации см. в оригинальной версии методички (раздел 4.3)**

**Производительность REST:**
- ~3-5 updates/sec (vs 37 updates/sec у WebSocket)
- Требует прокси для обхода Cloudflare
- Высокая нагрузка на API

---

## 7. Обход защиты Cloudflare

### 7.1 Проблема

Многие DEX агрегаторы (GMGN, DexScreener) используют Cloudflare anti-bot защиту:

```bash
curl "https://gmgn.ai/defi/quotation/v1/tokens/sol?address=..."
# Результат: 403 Forbidden
```

### 7.2 Решение: curl_cffi с TLS fingerprinting

**⚠️ КРИТИЧНО:** Обычные библиотеки (httpx, aiohttp) НЕ РАБОТАЮТ! Нужен curl_cffi.

**Установка:**
```bash
poetry add curl-cffi
```

**Файл:** `backend/infrastructure/services/common/gmgn_client.py`

```python
from curl_cffi import requests

class GmgnClient:
    BASE_URL = "https://gmgn.ai"
    IMPERSONATE = "chrome124"  # TLS fingerprint Chrome 124

    def __init__(self, proxies: list[str] | None = None):
        self.proxies = proxies or []
        self._proxy_index = 0

        # Browser-like headers (КРИТИЧНО!)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Origin": "https://gmgn.ai",
            "Referer": "https://gmgn.ai/",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

    def _get_next_proxy(self):
        """Round-robin прокси"""
        if not self.proxies:
            return None
        proxy_url = self.proxies[self._proxy_index]
        self._proxy_index = (self._proxy_index + 1) % len(self.proxies)
        return {"https": proxy_url, "http": proxy_url}

    def get_json(self, endpoint: str, params: dict | None = None, max_retries: int = 3):
        """GET запрос с curl_cffi"""
        url = f"{self.BASE_URL}{endpoint}"

        for attempt in range(1, max_retries + 1):
            try:
                proxies = self._get_next_proxy()

                # КРИТИЧНО: impersonate + verify=False
                response = requests.get(
                    url,
                    params=params,
                    headers=self.headers,
                    proxies=proxies,
                    impersonate=self.IMPERSONATE,  # Chrome TLS fingerprint
                    timeout=30,
                    verify=False  # Отключить SSL верификацию
                )

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 403:
                    logger.warning(f"403 Forbidden on attempt {attempt}/{max_retries}")
                    continue
                elif response.status_code == 429:
                    logger.warning(f"Rate limited on attempt {attempt}/{max_retries}")
                    continue
                else:
                    logger.error(f"HTTP {response.status_code}")
                    return None

            except Exception as e:
                logger.error(f"Request failed: {e}")
                if attempt == max_retries:
                    return None

        return None
```

**⚠️ КРИТИЧНЫЕ ПАРАМЕТРЫ:**

1. **`impersonate="chrome124"`** - Имитация TLS fingerprint Chrome 124
2. **`verify=False`** - Отключение SSL верификации
3. **Browser headers** - Полный набор заголовков как у реального браузера
4. **Proxies** - Round-robin ротация прокси при 403/429

**📝 ПРИМЕЧАНИЕ:** WebSocket обычно НЕ требует обхода Cloudflare! Защита применяется только к REST API.

---

## 8. Интеграция в Spread Analyzer

### 8.1 Почему это критично

**Проблема:** Просто сохранение в `dex_prices:gmgn` НЕ ДОСТАТОЧНО! Данные должны попасть в spread analyzer.

**Решение:** Модифицировать `loader_optimized.py`, чтобы он читал DEX streams.

### 8.2 Модификация Loader

**Файл:** `backend/infrastructure/services/collectors/spread_collector/loader_optimized.py`

**Шаг 1: Добавить import**
```python
import ast  # Для парсинга orderbook строк
```

**Шаг 2: Добавить вызов в load_tokens_with_orderbooks**
```python
async def load_tokens_with_orderbooks(self) -> int:
    # ... существующий код ...

    # Step 7: Fetch orderbooks from Redis
    await self._fill_orderbooks(keys, key_to_price)
    timer.checkpoint("fill_orderbooks")

    # Step 8: Add DEX orderbooks from streams (НОВОЕ!)
    await self._fill_dex_orderbooks(filtered_tokens)
    timer.checkpoint("fill_dex_orderbooks")

    # ... остальной код ...
```

**Шаг 3: Реализовать _fill_dex_orderbooks**
```python
async def _fill_dex_orderbooks(self, tokens: list[dict]):
    """
    Прочитать DEX цены из streams и добавить orderbook к токенам.

    Сопоставление: chain + contract_address
    """
    dex_sources = ["gmgn"]  # Добавить новые DEX здесь

    logger.debug(f"DEX: Starting DEX orderbook fill for {len(tokens)} tokens")

    # Построить индекс: (chain, address) -> [price1, price2, ...]
    token_index = {}
    for token in tokens:
        for price in token.get("prices", []):
            for chain_info in price.get("chains") or []:
                chain = chain_info.get("chain_name", "").lower()
                address = chain_info.get("contract_address", "").lower()
                if chain and address:
                    key = (chain, address)
                    if key not in token_index:
                        token_index[key] = []
                    token_index[key].append(price)

    if not token_index:
        logger.debug("DEX: No DEX tokens found")
        return

    logger.info(f"DEX: Found {len(token_index)} unique DEX token addresses")

    # Читать из каждого DEX источника
    for source in dex_sources:
        stream_key = f"dex_prices:{source}"

        try:
            # Читать последние 1000 записей (покрывает ~10 мин при 10s интервале)
            entries = await self._retry_on_redis_loading(
                f"read_{source}_stream",
                lambda: self.cache.client.xrevrange(stream_key, count=1000)
            )

            if not entries:
                logger.debug(f"DEX: No entries in {stream_key}")
                continue

            logger.info(f"DEX: Read {len(entries)} entries from {stream_key}")

            # Обработать записи
            processed = set()
            matches_found = 0

            for entry_id, data in entries:
                # Извлечь chain и address
                chain = data.get(b"chain", b"").decode("utf-8").lower()
                address = data.get(b"contract_address", b"").decode("utf-8").lower()

                if not chain or not address:
                    continue

                key = (chain, address)

                # Пропустить дубликаты (используем только последнюю запись)
                if key in processed:
                    continue

                # Найти совпадения
                matching_prices = token_index.get(key)
                if not matching_prices:
                    continue

                # Парсить orderbook
                try:
                    orderbook_str = data.get(b"orderbook", b"").decode("utf-8")
                    # Формат: "{'asks': [[price, volume]], 'bids': [[price, volume]]}"
                    orderbook = ast.literal_eval(orderbook_str)

                    timestamp = data.get(b"timestamp", b"").decode("utf-8")

                    # Добавить orderbook ко всем совпадениям
                    for price in matching_prices:
                        price["orderbook"] = orderbook
                        price["get_at"] = int(timestamp) if timestamp else None
                        matches_found += 1

                    processed.add(key)

                except Exception as e:
                    logger.debug(f"Failed to parse DEX orderbook for {chain}/{address}: {e}")
                    continue

            if processed:
                logger.info(f"DEX: Added {len(processed)} orderbooks from {source} ({matches_found} price objects updated)")
            else:
                logger.debug(f"DEX: No matches for {source}")

        except Exception as e:
            logger.error(f"Error reading DEX stream {stream_key}: {e}")
            continue
```

**⚠️ КРИТИЧНЫЕ МОМЕНТЫ:**

1. **Сопоставление по (chain, address)**: DEX токены идентифицируются НЕ по symbol!
2. **ast.literal_eval**: Orderbook сохранён как строка, нужно парсить
3. **XREVRANGE count=1000**: Берём последние записи (новые первыми)
4. **Дубликаты**: Используем `processed` set, чтобы взять только последнюю цену
5. **Lowercase нормализация**: chain и address приводим к lowercase

### 8.3 Проверка интеграции

```bash
# 1. Проверить, что Loader читает DEX streams
docker logs spread_loader | grep "DEX:"

# Ожидаемый вывод:
# DEX: Found 3806 unique DEX token addresses to check
# DEX: Read 100 entries from dex_prices:gmgn
# DEX: Added 50 DEX orderbooks from gmgn (120 price objects updated)

# 2. Проверить, что данные публикуются в orderbooks:updates
docker logs spread_loader | grep "Published"

# 3. Проверить, что Spread Collector анализирует данные
docker logs docker-spread-collector-1 | grep "messages="
```

---

## 9. Регистрация в менеджерах

### 9.1 Price Collector Manager

**Файл:** `backend/infrastructure/services/collectors/price_collector/manager.py`

```python
def get_price_collector_manager(
    repo: RequestsRepo,
    cache: RedisCache,
    market_type: MarketType | None = None,  # Не используется для DEX!
    scale_count: int | None = None,
    source_name: str | None = None,
) -> PriceCollectorManager:
    available_sources: dict[str, type[SupportsPriceParsing]] = {
        "mexc": MexcPriceParser,
        "gate": GatePriceParser,
        # ... другие CEX ...

        # DEX источники (добавить здесь)
        "gmgn": GmgnWSPriceParser,  # WebSocket (основная)
        # "gmgn_rest": GmgnRestPriceParser,  # REST (fallback)
        "[новая_биржа]": [НоваяБиржа]WSPriceParser,  # WebSocket предпочтительно
    }

    # ... validation ...

    source_class = available_sources[source_name]

    # DEX vs CEX инициализация
    try:
        import inspect
        sig = inspect.signature(source_class.__init__)

        if 'market_type' in sig.parameters:
            # CEX - нужен market_type
            source_instance = source_class(cache=cache, repo=repo, market_type=market_type)
        else:
            # DEX - market_type НЕ нужен
            source_instance = source_class(cache=cache, repo=repo)
    except Exception as e:
        raise ValueError(f"Failed to instantiate {source_name}: {e}")

    return PriceCollectorManager(...)
```

**⚠️ КРИТИЧНО:** Проверка `'market_type' in sig.parameters` автоматически определяет DEX vs CEX!

---

## 10. Настройка Docker

### 10.1 Docker Compose конфигурация

**Файл:** `docker/dev.yml`

```yaml
services:
  # CEX collector (для сравнения)
  price-collector-spot-mexc:
    build:
      context: ..
      dockerfile: backend/api/Dockerfile
    command: python infrastructure/services/collectors/price_collector/main.py
    volumes:
      - ./data/proxies.txt:/app/data/proxies.txt
    environment:
      PYTHONPATH: /app
      MARKET_TYPE: spot        # CEX нужен market_type
      SOURCE_NAME: mexc
    depends_on:
      - redis
      - postgres
      - bot
      - price_distributor      # CEX нужен distributor
    restart: unless-stopped

  # DEX WebSocket collector (РЕКОМЕНДУЕМЫЙ!)
  price-collector-gmgn:
    build:
      context: ..
      dockerfile: backend/api/Dockerfile
    command: python infrastructure/services/collectors/price_collector/main.py
    # БЕЗ volumes для прокси - WebSocket не требует!
    environment:
      PYTHONPATH: /app
      SOURCE_NAME: gmgn        # БЕЗ MARKET_TYPE!
      LOGGING__LOGGING_LEVEL: DEBUG  # Для отладки
    depends_on:
      - redis
      - postgres
      - bot
      # БЕЗ price_distributor!  DEX не использует distributor
    restart: unless-stopped
```

**⚠️ КРИТИЧНЫЕ ОТЛИЧИЯ от CEX:**

1. **НЕТ `MARKET_TYPE`** - DEX не использует spot/futures
2. **НЕТ `depends_on: price_distributor`** - DEX идёт напрямую в loader
3. **НЕТ прокси для WebSocket** - защита Cloudflare обычно только на REST API

**📝 ПРИМЕЧАНИЕ:** Если используете REST fallback, добавьте volume для прокси:
```yaml
volumes:
  - ./data/proxies.txt:/app/data/proxies.txt  # Только для REST!
```

### 10.2 Файл с прокси (только для REST)

**Файл:** `docker/data/proxies.txt`

```text
# Формат: user:pass@host:port
user1:pass1@proxy1.example.com:8080
user2:pass2@proxy2.example.com:8080

# Или без авторизации:
proxy3.example.com:3128
```

---

## 11. Маппинг сетей (chains)

### 11.1 Создание CHAIN_MAP

**Файл:** `backend/infrastructure/services/collectors/structures/maps.py`

```python
# GMGN chain mapping (добавить для новой биржи)
GMGN_CHAIN_MAP = {
    # Наше название -> GMGN название
    "ethereum": "eth",
    "binance-smart-chain": "bsc",
    "solana": "sol",
    "base": "base",
    "arbitrum-one": "arb",  # КРИТИЧНО: не "arbitrum"!
    "polygon-pos": "polygon",
}

# Новая биржа (пример)
DEXSCREENER_CHAIN_MAP = {
    "ethereum": "ethereum",  # Могут совпадать
    "binance-smart-chain": "bsc",
    "solana": "solana",
    "base": "base",
    # ...
}
```

### 11.2 Определение маппинга

**Как узнать названия цепей биржи:**

```bash
# 1. Проверить документацию
# 2. Сделать тестовый запрос
curl "https://api.биржа.com/chains" | jq '.[] | .name'

# 3. Проверить примеры в ответах API
curl "https://api.биржа.com/tokens?chain=..." | jq '.chain'

# 4. Для WebSocket - посмотреть в subscribe методах:
python
>>> from <package> import Client
>>> help(Client.subscribe_pair_updates)
# Параметр chain: "sol", "eth", "bsc", ...
```

**⚠️ КРИТИЧНО:** Маппинг ОБЯЗАТЕЛЕН! Используется в двух местах:
1. Price Collector: наше название → API название
2. Loader: API название → наше название (для сопоставления)

---

## 12. Тестирование и деплой

### 12.1 Локальное тестирование

```bash
# 1. Собрать контейнер
docker compose -f docker/dev.yml build price-collector-[биржа]

# 2. Запустить
docker compose -f docker/dev.yml up -d price-collector-[биржа]

# 3. Проверить логи (первые 2 минуты)
docker logs price-collector-[биржа] -f

# Ожидаемые логи для WebSocket:
# ✅ Loaded 8462 tokens for [биржа]/ethereum, solana, ...
# ✅ [БИРЖА] WS: Loaded 3359 tokens from 6 chains
# ✅ Client connected
# ✅ Subscribed to pair_updates for eth, sol, ...
# ✅ Event handlers registered
# ✅ Starting message loop
# ✅ [БИРЖА] WS: 1000 updates, 57 saved, 0 errors

# 4. Проверить Redis stream
docker exec redis redis-cli XLEN dex_prices:[биржа]
# Ожидается: > 0

# 5. Проверить содержимое stream
docker exec redis redis-cli XREVRANGE dex_prices:[биржа] + - COUNT 1
# Проверить наличие полей: symbol, price, orderbook, chain, contract_address
```

### 12.2 Проверка Loader интеграции

```bash
# 1. Перезапустить loader (чтобы подхватить изменения)
docker compose -f docker/dev.yml restart spread-loader

# 2. Подождать 30 секунд, затем проверить логи
docker logs spread_loader | grep "DEX:"

# Ожидаемые логи:
# ✅ DEX: Found 3000+ unique DEX token addresses to check
# ✅ DEX: Read 100 entries from dex_prices:[биржа]
# ✅ DEX: Added 50 DEX orderbooks from [биржа] (120 price objects updated)

# 3. Если НЕТ совпадений - проверить маппинг цепей!
# Проблема: chain names в БД != chain names в API
```

### 12.3 Проверка Spread Collector

```bash
# Проверить, что данные доходят до анализатора
docker logs docker-spread-collector-1 | grep "messages="

# Ожидаемый вывод:
# ✅ messages=1 | tokens=1 | ... | results=0

# results=0 - это нормально (арбитраж редкий)
# Главное - messages > 0
```

### 12.4 Деплой на сервер

```bash
# 1. Закоммитить изменения
git add .
git commit -m "feat: add [биржа] DEX integration (WebSocket)"
git push origin develop

# 2. На сервере
ssh root@server
cd /home/finder-bot
git pull origin develop

# 3. КРИТИЧНО: Пересобрать с --no-cache
docker compose -f docker/dev.yml build --no-cache price-collector-[биржа]
docker compose -f docker/dev.yml build --no-cache spread-loader

# 4. Остановить и удалить старый контейнер
docker compose -f docker/dev.yml stop price-collector-[биржа]
docker compose -f docker/dev.yml rm -f price-collector-[биржа]

# 5. Запустить заново
docker compose -f docker/dev.yml up -d price-collector-[биржа]
docker compose -f docker/dev.yml restart spread-loader

# 6. Проверить логи (см. 12.1 и 12.2)
docker logs price-collector-[биржа] --tail 100

# 7. Убедиться, что используется ПРАВИЛЬНЫЙ файл
docker logs price-collector-[биржа] | grep "\.py:"
# Должно быть: "[биржа]_ws_working.py" или "[биржа]_ws.py"
# НЕ ДОЛЖНО быть: "[биржа]_rest.py" (если хотели WebSocket!)
```

**🔥 КРИТИЧНО ДЛЯ ДЕПЛОЯ:**

После изменения кода коллектора, Docker может использовать **старый закешированный код**!

**Правильная последовательность:**
1. `git pull` - обновить код на сервере
2. `docker compose stop + rm` - удалить контейнер
3. `docker compose build --no-cache` - пересобрать БЕЗ кеша
4. `docker compose up -d` - запустить заново
5. **Проверить логи на имя файла:** должно быть `_ws.py`, а не `_rest.py`

---

## 13. Типичные проблемы и решения

### Проблема 1: 403 Forbidden от API

**Симптомы:**
```
GMGN: HTTP 403 - https://gmgn.ai/defi/quotation/v1/tokens/...
```

**Причина:** Cloudflare anti-bot защита (только для REST API!)

**Решение:**
1. ✅ Переключиться на WebSocket (обычно не защищён)
2. ✅ Использовать `curl_cffi` с `impersonate="chrome124"` (для REST)
3. ✅ Добавить browser headers
4. ✅ Использовать прокси

---

### Проблема 2: Документация библиотеки неверна

**Симптомы:**
```python
AttributeError: 'Client' object has no attribute 'subscribe_token_updates'
# Или
AttributeError: 'Client' object has no attribute 'on_token_update'
```

**Причина:** Документация библиотек часто устаревшая или неполная

**Решение:**

```python
# 1. Читайте ИСХОДНЫЙ КОД библиотеки
import inspect
from <package> import Client

# 2. Смотрите все методы
client = Client()
methods = [m for m in dir(client) if not m.startswith('_')]
print(methods)

# 3. Проверяйте signature методов
for method_name in ['subscribe', 'on', 'listen']:
    if hasattr(client, method_name):
        method = getattr(client, method_name)
        print(f"{method_name}: {inspect.signature(method)}")

# 4. Создайте тестовый скрипт для проверки работы
```

**🔥 ПРИМЕР: gmgnapi**

README говорит:
```python
# ❌ НЕПРАВИЛЬНО (из документации)
await client.subscribe_token_updates(chain="sol")
@client.on_token_update
async def handler(data):
    pass
```

Реальность:
```python
# ✅ ПРАВИЛЬНО (из исходного кода)
await client.subscribe_pair_updates(chain="sol")
client.on("new_pair_update", handler)
```

---

### Проблема 3: WebSocket получает ack, но нет данных

**Симптомы:**
```
WebSocket connected successfully
Subscribed to pair_updates
Event handlers registered
Starting message loop
# ... 15 минут тишины ...
```

**Причина:** Обработчик не вызывается или данные фильтруются

**Отладка:**

```python
async def debug_handler(data):
    print(f"RAW DATA: {type(data)}")
    print(f"DATA: {data}")

    # Если Message object
    if hasattr(data, '__dict__'):
        print(f"ATTRIBUTES: {vars(data)}")

client.on("new_pair_update", debug_handler)

# Запустить и смотреть ВСЕ сообщения
```

**Решение:**

1. ✅ Проверьте фильтрацию данных:
   ```python
   # Неправильно
   if item.get("type") == "price":  # Может не существовать
       ...

   # Правильно
   if isinstance(item, dict):  # Сначала проверка типа
       if item.get("_v_ch") == "stat":  # Потом фильтр
           ...
   ```

2. ✅ Логируйте КАЖДЫЙ handler call:
   ```python
   async def handler(data):
       logger.info(f"HANDLER CALLED: {type(data)}")
       # ... остальной код ...
   ```

---

### Проблема 4: Loader не находит DEX токены

**Симптомы:**
```
DEX: Found 0 unique DEX token addresses to check
```

**Причина:** Токены в БД не имеют `token_chains` записей

**Решение:**

1. Проверить наличие токенов в БД:
   ```sql
   SELECT COUNT(*) FROM token_chains tc
   JOIN chains c ON tc.chain_id = c.id
   JOIN sources s ON tc.source_id = s.id
   WHERE c.name IN ('ethereum', 'solana', ...)
     AND s.name = 'gmgn';
   ```

2. Если токенов нет - запустить Token Collector для этой биржи (если есть)

3. Или добавить токены вручную через миграцию

---

### Проблема 5: DEX данные не сопоставляются

**Симптомы:**
```
DEX: Read 100 entries from dex_prices:gmgn
DEX: No matching tokens found for gmgn stream entries
```

**Причина:** Несовпадение chain names в API и БД

**Отладка:**

```python
# 1. Проверить chain name в Redis stream
docker exec redis redis-cli XREVRANGE dex_prices:gmgn + - COUNT 1
# Посмотреть поле "chain"

# 2. Проверить chain names в БД
SELECT DISTINCT c.name FROM chains c
JOIN token_chains tc ON tc.chain_id = c.id;

# 3. Проверить маппинг
# Файл: structures/maps.py
# CHAIN_MAP должен содержать: db_name -> api_name
```

**Решение:**

1. Обновить `CHAIN_MAP` в `structures/maps.py`
2. Убедиться, что маппинг используется в обе стороны:
   - Collector: db name → API name (при запросе)
   - Loader: API name должен совпадать с сохранённым в Redis

---

### Проблема 6: Контейнер использует старый код после деплоя

**Симптомы:**
```bash
# После git pull и docker compose restart
docker logs price-collector-gmgn | grep "\.py:"
# Показывает: gmgn_rest.py

# Но должен показывать: gmgn_ws_working.py
```

**Причина:** Docker использует закешированные слои образа

**Решение (КРИТИЧНО!):**

```bash
# 1. Полностью остановить и удалить контейнер
docker compose -f docker/dev.yml stop price-collector-gmgn
docker compose -f docker/dev.yml rm -f price-collector-gmgn

# 2. Пересобрать БЕЗ кеша
docker compose -f docker/dev.yml build --no-cache price-collector-gmgn

# 3. Запустить заново
docker compose -f docker/dev.yml up -d price-collector-gmgn

# 4. КРИТИЧНО: Проверить имя файла в логах
docker logs price-collector-gmgn --tail 50 | grep "\.py:"
# Должно быть: "(gmgn_ws_working.py:start:66)"
```

**Проверка ПЕРЕД деплоем:**

```bash
# На сервере проверить, что файл обновлён
ssh root@server
cd /home/finder-bot
git log --oneline -n 5  # Последние коммиты
ls -la backend/infrastructure/services/collectors/price_collector/sources/gmgn*
cat backend/infrastructure/services/collectors/price_collector/manager.py | grep "gmgn"
```

---

### Проблема 7: RedisCache' object has no attribute 'redis'

**Симптомы:**
```
AttributeError: 'RedisCache' object has no attribute 'redis'
```

**Причина:** Неправильное обращение к Redis клиенту

**Решение:**

```python
# ❌ НЕПРАВИЛЬНО
await self.cache.redis.xadd(stream_key, data)

# ✅ ПРАВИЛЬНО
await self.cache.client.xadd(stream_key, data)
```

---

### Проблема 8: Orderbook не парсится в Loader

**Симптомы:**
```
Failed to parse DEX orderbook for eth/0x123: malformed node or string
```

**Причина:** Orderbook сохранён неправильно или парсится неправильно

**Проверка формата:**

```bash
# Проверить формат в Redis
docker exec redis redis-cli XREVRANGE dex_prices:gmgn + - COUNT 1

# Поле orderbook должно быть строкой:
# orderbook: "{'asks': [[0.123, 100]], 'bids': [[0.123, 100]]}"
```

**Решение:**

1. При сохранении:
   ```python
   "orderbook": str(orderbook)  # Конвертировать в строку
   ```

2. При чтении:
   ```python
   import ast
   orderbook = ast.literal_eval(orderbook_str)  # Парсить обратно
   ```

---

## 14. Чеклист интеграции

### Фаза 1: Предварительный анализ

- [ ] Найдена документация API
- [ ] Определён список поддерживаемых цепей
- [ ] Найден REST endpoint для цен по адресу (опционально)
- [ ] Проверен WebSocket API (ПРИОРИТЕТ!)
- [ ] Найдена Python библиотека (PyPI / GitHub)
- [ ] Протестирована библиотека (создан test script)
- [ ] Изучена структура данных WebSocket (Message object, channels)
- [ ] Проверена защита Cloudflare (403?)
- [ ] Определён формат contract address (lowercase, с/без 0x)
- [ ] Найдены примеры response для каждой цепи

### Фаза 2: Реализация Collector (WebSocket)

- [ ] Создан `{биржа}_ws.py` на основе `gmgn_ws_working.py`
- [ ] Наследуется от `BaseDexPriceParser`
- [ ] Реализован `__init__` с `supported_chains`
- [ ] Реализован `start()` с WebSocket подпиской
- [ ] Реализован token indexing: `{(chain, address): (symbol, token_id)}`
- [ ] Реализованы event handlers (минимум 1 канал)
- [ ] Используется `save_price_update()` из базового класса
- [ ] Добавлена статистика (updates, saved, errors)
- [ ] Логируется каждые 1000 updates
- [ ] Создан `{БИРЖА}_CHAIN_MAP` в `structures/maps.py`
- [ ] Создан тестовый скрипт для локального запуска

### Фаза 3: Интеграция в Loader

- [ ] Добавлен `import ast` в `loader_optimized.py`
- [ ] Добавлен вызов `_fill_dex_orderbooks()` в `load_tokens_with_orderbooks()`
- [ ] Добавлена биржа в `dex_sources = ["gmgn", "новая_биржа"]`
- [ ] Проверена нормализация chain names (lowercase)
- [ ] Проверена нормализация addresses (lowercase, без 0x если нужно)

### Фаза 4: Регистрация

- [ ] Добавлена биржа в `available_sources` в `manager.py`
- [ ] Добавлен сервис в `docker/dev.yml`
- [ ] БЕЗ `MARKET_TYPE` environment variable
- [ ] БЕЗ `depends_on: price_distributor`
- [ ] БЕЗ volume mount для прокси (если WebSocket)

### Фаза 5: Тестирование

- [ ] Локально: `docker compose up -d price-collector-[биржа]`
- [ ] Проверены логи: "Client connected", "Subscribed", "1000 updates"
- [ ] Проверен Redis: `XLEN dex_prices:[биржа]` > 0
- [ ] Проверен формат: `XREVRANGE` содержит все поля
- [ ] Перезапущен loader: `docker compose restart spread-loader`
- [ ] Проверены логи loader: "DEX: Found X addresses", "Added Y orderbooks"
- [ ] Проверены логи collector: "messages=N" > 0

### Фаза 6: Деплой

- [ ] Закоммичены все изменения
- [ ] Push в develop
- [ ] Pull на сервере
- [ ] **КРИТИЧНО:** Stop + rm контейнера
- [ ] Build с `--no-cache`
- [ ] Запуск сервиса
- [ ] **КРИТИЧНО:** Проверка имени файла в логах (должен быть `_ws.py`!)
- [ ] Проверка логов на сервере (как в Фазе 5)
- [ ] Мониторинг 24 часа на ошибки
- [ ] Мониторинг производительности (updates/sec)

---

## Итоговая структура файлов

```
backend/infrastructure/services/
├── collectors/
│   ├── price_collector/
│   │   ├── base_dex.py                      # Базовый класс (не трогать)
│   │   ├── sources/
│   │   │   ├── gmgn_ws_working.py           # ✅ WebSocket (reference)
│   │   │   ├── gmgn_rest.py                 # Fallback REST (reference)
│   │   │   └── [новая_биржа]_ws.py          # ← СОЗДАТЬ (WebSocket)
│   │   │   └── [новая_биржа]_rest.py        # ← ОПЦИОНАЛЬНО (REST fallback)
│   │   └── manager.py                        # ← ДОБАВИТЬ биржу
│   ├── spread_collector/
│   │   └── loader_optimized.py              # ← ДОБАВИТЬ в dex_sources
│   ├── structures/
│   │   └── maps.py                          # ← СОЗДАТЬ CHAIN_MAP
│   └── common/
│       └── [биржа]_client.py                # ← ОПЦИОНАЛЬНО (только для REST+Cloudflare)
```

---

## Полезные команды

```bash
# Проверка Redis streams
docker exec redis redis-cli XLEN dex_prices:[биржа]
docker exec redis redis-cli XREVRANGE dex_prices:[биржа] + - COUNT 10

# Проверка БД
docker exec database psql -U dex-dev -d dex-dev -c "
SELECT c.name, COUNT(*)
FROM token_chains tc
JOIN chains c ON tc.chain_id = c.id
WHERE c.name IN ('ethereum', 'solana', 'base')
GROUP BY c.name;
"

# Логи в реальном времени
docker logs price-collector-[биржа] -f
docker logs spread_loader -f | grep "DEX:"
docker logs docker-spread-collector-1 -f

# Проверка используемого файла (КРИТИЧНО!)
docker logs price-collector-[биржа] | grep "\.py:" | head -10
# Должно показать: "([биржа]_ws.py:start:66)" или подобное

# Перезапуск после изменений
docker compose -f docker/dev.yml restart price-collector-[биржа]
docker compose -f docker/dev.yml restart spread-loader

# Полная пересборка (КРИТИЧНО для деплоя!)
docker compose -f docker/dev.yml stop price-collector-[биржа]
docker compose -f docker/dev.yml rm -f price-collector-[биржа]
docker compose -f docker/dev.yml build --no-cache price-collector-[биржа]
docker compose -f docker/dev.yml build --no-cache spread-loader
docker compose -f docker/dev.yml up -d
```

---

## Заключение

Интеграция DEX биржи — это **многошаговый процесс**, требующий:

1. ✅✅✅ **Приоритет WebSocket** - производительность в 10x выше REST (~37 vs ~3-5 updates/sec)
2. ✅ Тщательной проверки документации библиотеки (часто устаревшая!)
3. ✅ Исследования структуры данных через тестовые скрипты
4. ✅ Правильного маппинга цепей (bi-directional)
5. ✅ Интеграции в Spread Loader
6. ✅ Тщательного деплоя с проверкой используемого кода

**🔥 КЛЮЧЕВЫЕ УРОКИ из GMGN интеграции:**

1. **WebSocket работает отлично**, даже если изначально казалось иначе
2. **Документация библиотек может быть НЕВЕРНОЙ** - всегда проверяйте исходный код
3. **Token indexing критичен** - создайте `{(chain, address): metadata}` индекс
4. **Channel filtering критичен** - фильтруйте по `_v_ch == "stat"` или аналогам
5. **Docker кеш опасен** - всегда используйте `--no-cache` + stop/rm при деплое
6. **Проверяйте имя файла в логах** - убедитесь, что используется правильная версия

**Следуйте этой методичке пошагово**, начиная с WebSocket подхода, и интеграция пройдёт эффективно!

---

---

## 15. Обеспечение работы сигналов (КРИТИЧНО!)

После того как Price Collector работает и данные попадают в Redis, нужно убедиться что **сигналы реально генерируются**. Этот раздел описывает типичные проблемы на последнем этапе.

### 15.1 Пороги арбитража

**Минимальные пороги для сигнала (по умолчанию):**

| Уровень | Спред (%) | Профит ($) |
|---------|-----------|------------|
| `low` | ≥1.0% | ≥$2 |
| `medium` | ≥3.0% | ≥$35 |
| `high` | ≥10.0% | ≥$100 |
| `very_high` | ≥20.0% | ≥$200 |

**Файл конфигурации:** `backend/infrastructure/services/collectors/spread_collector/config.py`

**⚠️ ВАЖНО:** Если спред между DEX и CEX меньше 1%, сигнал НЕ будет отправлен!

### 15.2 Диагностика отсутствия сигналов

**Симптомы:** Price Collector работает, Loader читает DEX данные, но `results=0` в логах.

**Пошаговая диагностика:**

```bash
# 1. Проверить что DEX цены есть в Redis
docker exec redis redis-cli XLEN dex_prices:gmgn
# Ожидается: > 0

# 2. Проверить что Loader находит DEX токены
docker logs spread_loader | grep "DEX:"
# Ожидается: "DEX: Added N DEX orderbooks from gmgn"

# 3. Добавить debug логирование в manager_optimized.py (временно)
# См. секцию 15.3
```

### 15.3 Debug логирование для анализа

**Добавить в `manager_optimized.py` в метод `_analyze_token_group`:**

```python
# После парсинга orderbooks, перед analyze:
if dex_sources and spot_sources:
    for spot_name, spot_data in spot_sources.items():
        for dex_name, dex_data in dex_sources.items():
            spot_ob = spot_data.get("orderbook", {})
            dex_ob = dex_data.get("orderbook", {})
            spot_bid = spot_ob.get("bids", [[0]])[0][0] if spot_ob.get("bids") else 0
            spot_ask = spot_ob.get("asks", [[0]])[0][0] if spot_ob.get("asks") else 0
            dex_price = dex_ob.get("bids", [[0]])[0][0] if dex_ob.get("bids") else 0

            # Расчёт потенциального спреда
            if dex_price > 0 and spot_bid > 0:
                spread = ((spot_bid - dex_price) / dex_price) * 100
                logger.info(
                    f"DEX-CEX ANALYSIS: {token.get('symbol')} | "
                    f"CEX({spot_name}): bid={spot_bid}, ask={spot_ask} | "
                    f"DEX({dex_name}): price={dex_price} | "
                    f"potential_spread={spread:.2f}%"
                )
```

**⚠️ ВАЖНО:** Удалите debug логирование после диагностики!

### 15.4 Проблема: CEX source_type не распознаётся

**Симптомы:**
```
SOURCES DEBUG: ELONCOIN | spot=[] | futures=[] | dex=['gmgn'] | prices_count=2
```

CEX source есть (prices_count=2), но `spot=[]`.

**Причина:** Поле `source["type"]` пустое или не равно "cex".

**Диагностика:**

```python
# Добавить в цикл парсинга prices:
logger.info(
    f"PRICE PARSE: {token.get('symbol')} | "
    f"source_name={source_name} | source_type={source_type} | "
    f"raw_type={source.get('type')}"
)
```

**Решение:** Проверить что в `loader_optimized.py` правильно заполняется поле `source["type"]`.

### 15.5 Проблема: token_source_name отсутствует в chains

**Симптомы:** Ссылка на DEX биржу не генерируется (пустая или неправильная).

```
GMGN_DEX: $0.00134 | Sell: 56$
┞ SOL │ D:🟢 W:🟢
# НЕТ ССЫЛКИ на gmgn.ai!
```

**Причина:** Поле `token_source_name` отсутствует в chains для DEX.

**Где добавить:**

**1. В `loader_optimized.py` при создании DEX price:**

```python
# В методе _fill_dex_orderbooks, при создании dex_chains:
dex_chains = []
for c in sample_price.get("chains", []):
    if c.get("chain_name", "").lower() == chain.lower():
        dex_chains.append({
            "chain_name": c.get("chain_name"),
            "contract_address": c.get("contract_address") or address,
            "withdraw_enabled": True,
            "deposit_enabled": True,
            "withdrawal_fee_in_usdt": 0,
            "token_source_name": source,  # ← КРИТИЧНО! DEX source name
        })
```

**2. В `loader_optimized.py` при стриппинге CEX prices:**

```python
"chains": [
    {
        "chain_name": c.get("chain_name"),
        "contract_address": c.get("contract_address"),
        "withdraw_enabled": c.get("withdraw_enabled"),
        "deposit_enabled": c.get("deposit_enabled"),
        "withdrawal_fee_in_usdt": c.get("withdrawal_fee_in_usdt"),
        "token_source_name": price.get("source", {}).get("name"),  # ← ДОБАВИТЬ
    }
    for c in price.get("chains", [])
],
```

**3. В `manager_optimized.py` в `prepare_result_chains`:**

```python
all_chains.append({
    "chain_name": c.get("chain_name"),
    "contract_address": c.get("contract_address"),
    # ... другие поля ...
    "token_source_name": c.get("token_source_name"),  # ← ДОБАВИТЬ
})
```

### 15.6 Проблема: Неправильная ссылка на DEX

**Симптомы:** Ссылка ведёт на `https://gmgn.ai/?chain=sol` вместо страницы токена.

**Причина:** В `utils.py` генерируется неполная ссылка.

**Решение в `structures/utils.py`:**

```python
case "dex":
    if source == "gmgn":
        # Полный URL с contract_address
        if contract_address:
            return f"https://gmgn.ai/{chain}/token/{contract_address}"
        return f"https://gmgn.ai/{chain}"
    # Generic dex (dexscreener)
    return f"https://dexscreener.com/{chain}/{contract_address}"
```

### 15.7 Чеклист для обеспечения сигналов

После базовой интеграции проверьте:

- [ ] **source["type"]** заполнен корректно ("cex" или "dex")
- [ ] **token_source_name** присутствует в chains для DEX prices
- [ ] **contract_address** присутствует в chains для DEX prices
- [ ] **Ссылка на DEX** генерируется с contract_address
- [ ] **Пороги арбитража** соответствуют реальным спредам (может потребоваться снижение для тестирования)
- [ ] **Debug логирование** показывает потенциальные спреды между DEX и CEX

### 15.8 Тестирование с пониженными порогами

Для тестирования можно временно снизить пороги:

```python
# В config.py, временно для тестирования:
spread_thresholds: SpreadThresholds = field(default_factory=lambda: SpreadThresholds(
    very_high=10.0,
    high=5.0,
    medium=1.0,
    low=0.3,  # ← Снизить до 0.3% для тестирования
))

profit_thresholds: ProfitThresholds = field(default_factory=lambda: ProfitThresholds(
    very_high=100.0,
    high=50.0,
    medium=10.0,
    low=0.1,  # ← Снизить до $0.1 для тестирования
))
```

**⚠️ ВАЖНО:** Верните оригинальные значения после тестирования!

---

## 16. Итоговый поток данных DEX (визуализация)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        GMGN WebSocket API                               │
│                    wss://gmgn.ai/ws                                     │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ new_pair_update, new_pool_info
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     Price Collector (gmgn_ws_working.py)                │
│  • Подписка на все chains (sol, eth, bsc, base, arb, polygon)          │
│  • Фильтрация: только _v_ch == "stat"                                   │
│  • Индекс: {(chain, address): (symbol, token_id)}                       │
│  • Конвертация: single price → orderbook format                        │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ xadd
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Redis Stream: dex_prices:gmgn                        │
│  • symbol, price, orderbook, chain, contract_address, token_id          │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ xrevrange
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  Spread Loader (loader_optimized.py)                    │
│  • Читает CEX prices из Redis (prices:xt:spot, prices:mexc:spot...)     │
│  • Читает DEX prices из dex_prices:gmgn                                 │
│  • Сопоставляет по (chain, contract_address)                            │
│  • Создаёт DEX price objects с token_source_name и contract_address     │
│  • Публикует в orderbooks:updates stream                                │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ xadd (msgpack)
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                Redis Stream: orderbooks:updates                          │
│  • Токены с CEX + DEX prices                                            │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ xread
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│               Spread Collector (manager_optimized.py)                    │
│  • Парсит source["type"]: "cex" → spot_sources, "dex" → dex_sources     │
│  • analyze_cross_type("spot", spot_sources, "dex", dex_sources)         │
│  • Фильтрация по spread_thresholds и profit_thresholds                  │
│  • Генерация ссылок через generate_market_link()                        │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ if spread >= 1% AND profit >= $2
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Broadcaster (broadcaster.py)                      │
│  • Форматирование сообщения с ссылками                                  │
│  • make_market_link() → generate_market_link() с token_source_name      │
│  • Отправка в Telegram                                                  │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          Telegram Bot                                    │
│  ELONCOIN 11% (5$)                                                       │
│  XT_SPOT (https://xt.com/...): $0.00120 | Buy: 50$                       │
│  GMGN_DEX (https://gmgn.ai/sol/token/...): $0.00134 | Sell: 56$         │
│  SOL: 2akxpuyfxavn5yofzpzmfbp2vxognpmv9noobmuhpump                       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

**Версия документа:** 3.0
**Дата:** 2026-02-05
**Автор:** На основе опыта REST + WebSocket интеграции GMGN
**Статус:** Production Ready

**Основные изменения в v3.0:**
- ✅ Добавлен раздел 15: Обеспечение работы сигналов
- ✅ Добавлена диагностика проблем с source_type и token_source_name
- ✅ Добавлено исправление генерации ссылок на DEX
- ✅ Добавлен чеклист для проверки сигналов
- ✅ Добавлена визуализация полного потока данных (раздел 16)
- ✅ Добавлены примеры debug логирования для диагностики

**Основные изменения в v2.0:**
- ✅ Добавлен приоритет WebSocket над REST (раздел 3)
- ✅ Добавлен раздел 5: WebSocket имплементация с полным кодом
- ✅ Обновлена матрица решений с реальной производительностью
- ✅ Добавлены критические открытия о gmgnapi библиотеке
- ✅ Добавлен раздел "Проблема 2": неверная документация библиотек
- ✅ Добавлен раздел "Проблема 6": Docker кеш при деплое
- ✅ Обновлён чеклист с акцентом на WebSocket
- ✅ Добавлены команды проверки используемого файла

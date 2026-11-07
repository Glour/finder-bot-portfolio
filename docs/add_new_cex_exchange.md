# Методичка: Добавление новой CEX биржи

## Обзор

Этот документ описывает пошаговый процесс интеграции новой CEX биржи в систему сбора данных. На примере Asterdex.

---

## Содержание

1. [Предварительный анализ API](#1-предварительный-анализ-api)
2. [Создание Token Collector](#2-создание-token-collector)
3. [Создание WebSocket клиента](#3-создание-websocket-клиента)
4. [Создание Price Parser](#4-создание-price-parser)
5. [Регистрация в менеджерах](#5-регистрация-в-менеджерах)
6. [Настройка Docker](#6-настройка-docker)
7. [Маппинг сетей](#7-маппинг-сетей)
8. [Тестирование и деплой](#8-тестирование-и-деплой)
9. [Типичные проблемы и решения](#9-типичные-проблемы-и-решения)
10. [Верификация интеграции](#10-верификация-интеграции)
11. [Обновление веб-интерфейса (Prices Tab)](#11-обновление-веб-интерфейса-prices-tab)

---

## 1. Предварительный анализ API

### 1.1 Что нужно найти в документации

| Информация | Где искать | Пример (Asterdex) |
|------------|-----------|-------------------|
| REST API для списка токенов | `/exchangeInfo`, `/symbols` | `sapi.asterdex.com/api/v1/exchangeInfo` |
| REST API для фьючерсов | Отдельный домен или путь | `fapi.asterdex.com/fapi/v3/exchangeInfo` |
| Информация о сетях/выводе | `/deposit/assets`, `/currencies` | `/bapi/futures/v1/public/future/aster/deposit/assets` |
| WebSocket URL | Обычно `wss://stream...` | `wss://sstream.asterdex.com` (spot), `wss://fstream.asterdex.com` (futures) |
| Формат подписки WS | `SUBSCRIBE`, `sub`, etc. | `{"method": "SUBSCRIBE", "params": ["btcusdt@depth"], "id": 1}` |
| Rate limits | Заголовки или документация | 6000 req/min (spot), 2400 req/min (futures) |

### 1.2 Проверка API вручную

```bash
# Проверка доступности REST API
curl -s "https://sapi.asterdex.com/api/v1/exchangeInfo" | jq '.symbols | length'
curl -s "https://fapi.asterdex.com/fapi/v3/exchangeInfo" | jq '.symbols | length'

# Проверка структуры данных токенов
curl -s "https://sapi.asterdex.com/api/v1/exchangeInfo" | jq '.symbols[0]'

# Проверка информации о сетях (если есть)
curl -s "https://www.asterdex.com/bapi/futures/v1/public/future/aster/deposit/assets" | jq '.[0]'
```

### 1.3 Определение формата данных

**Spot токен (типичная структура):**
```json
{
  "symbol": "BTCUSDT",
  "baseAsset": "BTC",
  "quoteAsset": "USDT",
  "status": "TRADING",
  "quotePrecision": 8
}
```

**Futures контракт:**
```json
{
  "symbol": "BTCUSDT",
  "pair": "BTCUSDT",
  "contractType": "PERPETUAL",
  "status": "TRADING",
  "contractSize": 1
}
```

**Информация о сетях:**
```json
{
  "name": "BTC",
  "network": "BTC",
  "chainId": 0,
  "contractAddress": null,
  "depositEnabled": true,
  "withdrawEnabled": true,
  "withdrawFee": "0.0005"
}
```

### 1.4 Типы API (КРИТИЧНО для WebSocket реализации)

Биржи используют **разные стили API**, что влияет на реализацию:

#### Тип 1: Binance-compatible (Binance-style)
**Примеры:** Asterdex, MEXC, XT, Gate.io, LBank, Bitmart

**Характеристики:**
- WebSocket: Единый URL для spot и futures
- Формат подписки: `{"method": "SUBSCRIBE", "params": ["btcusdt@depth"], "id": 1}`
- Формат сообщений: `{"stream": "btcusdt@depth", "data": {"b": [...], "a": [...]}}`
- Символы: `BTCUSDT` (без разделителя)
- Структура API: `/api/v1/exchangeInfo` (spot), `/fapi/v1/exchangeInfo` (futures)

**WebSocket пример:**
```python
class BinanceStyleWSClient:
    WS_URL = "wss://stream.exchange.com/stream"

    async def _subscribe(self, ws):
        streams = [f"{sym.lower()}@depth@100ms" for sym in self.symbols]
        payload = {"method": "SUBSCRIBE", "params": streams, "id": 1}
        await ws.send_str(orjson.dumps(payload).decode())
```

#### Тип 2: Custom protocol (Non-Binance)
**Примеры:** AscendEX, Hyperliquid

**AscendEX характеристики:**
- WebSocket: Единый URL `wss://ascendex.com:443/api/pro/v2/stream`
- Формат подписки: `{"op": "sub", "ch": "depth:BTC/USDT"}`
- Ping/Pong: `{"op": "pong"}` (не автоматический!)
- Символы: `BTC/USDT` (с разделителем `/`)
- Структура данных: `{"m": "depth", "symbol": "BTC/USDT", "data": {"bids": [...], "asks": [...]}}`

**WebSocket пример:**
```python
class AscendexWSClient:
    WS_URL = "wss://ascendex.com:443/api/pro/v2/stream"

    async def _subscribe_all(self, ws):
        for sym in self.symbols:
            payload = {"op": "sub", "ch": f"depth:{sym}"}
            await ws.send_str(orjson.dumps(payload).decode())

    async def _handle_message(self, ws, raw: str):
        msg = orjson.loads(raw)
        if msg.get("m") == "ping":
            await ws.send_str(orjson.dumps({"op": "pong"}).decode())
```

**Hyperliquid характеристики:**
- WebSocket: `wss://api.hyperliquid.xyz/ws`
- Формат подписки: `{"method": "subscribe", "subscription": {"type": "l2Book", "coin": "BTC"}}`
- Символы: Название монеты без пары (только `BTC`, не `BTCUSDT`)
- REST API: POST запросы вместо GET

#### Тип 3: Разделенные домены (Split domains)
**Примеры:** Asterdex (spot: `sstream.asterdex.com`, futures: `fstream.asterdex.com`)

**Характеристики:**
- **Отдельные WebSocket URL** для spot и futures
- Разные домены API (sapi для spot, fapi для futures)
- Binance-compatible протокол

**Реализация:**
```python
class SplitDomainWSClient:
    SPOT_WS_URL = "wss://sstream.exchange.com/stream"
    FUTURES_WS_URL = "wss://fstream.exchange.com/stream"

    def __init__(self, ..., market_type: MarketType):
        self.ws_url = (
            self.SPOT_WS_URL if market_type == MarketType.SPOT
            else self.FUTURES_WS_URL
        )
```

### 1.5 Выбор подхода при интеграции

**1. Проверить тип API:**
```bash
# Открыть документацию WebSocket
# Найти пример подписки на orderbook

# Binance-style:
{"method": "SUBSCRIBE", "params": [...]}

# Custom:
{"op": "sub", ...} или {"method": "subscribe", "subscription": {...}}
```

**2. Найти пример символа:**
- Binance-style: `BTCUSDT`, `btcusdt@depth`
- AscendEX: `BTC/USDT`, `depth:BTC/USDT`
- Hyperliquid: `BTC` (без пары)

**3. Проверить Ping/Pong:**
- Binance-style: автоматический heartbeat в aiohttp
- AscendEX: нужен явный `{"op": "pong"}` ответ
- Hyperliquid: не требуется

**4. Использовать референсный код:**
- Binance-style → копировать структуру из `asterdex.py` или `mexc.py`
- Custom → создавать с нуля, изучая документацию

---

## 2. Создание Token Collector

### 2.1 Структура файла

Путь: `backend/infrastructure/services/collectors/token_collector/sources/{exchange_name}.py`

```python
import asyncio
from typing import Any

from infrastructure.database.models.base import SourceType, MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.normalizer import ChainNormalizer
from infrastructure.services.collectors.token_collector.base import BaseTokenParser
from infrastructure.services.collectors.token_collector.supports.protocol import SupportsTokenParsing
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.common.http_client import HTTPClientPool
from settings import logger


class AsterdexTokenParser(BaseTokenParser, SupportsTokenParsing):
    """Token parser for Asterdex exchange."""

    # API endpoints
    SPOT_HOST = "sapi.asterdex.com"
    FUTURES_HOST = "fapi.asterdex.com"
    ASSETS_HOST = "www.asterdex.com"

    # Chain ID mapping (если биржа использует числовые ID)
    CHAIN_ID_MAP = {
        1: "ETH",
        56: "BSC",
        42161: "ARB",
        101: "SOL",
    }

    def __init__(
            self,
            cache: RedisCache,
            chain_normalizer: ChainNormalizer,
            repo: RequestsRepo,
            http_client: HTTPClientPool,
    ):
        super().__init__(
            cache,
            chain_normalizer,
            repo,
            source_name="asterdex",  # Имя источника (lowercase)
            source_type=SourceType.CEX,
        )
        self.http_client = http_client

    async def fetch_tokens(self) -> list[dict]:
        """Main method - fetch all tokens from exchange."""
        spot_data, futures_data, assets_data = await asyncio.gather(
            self._fetch_spot_symbols(),
            self._fetch_futures_symbols(),
            self._fetch_deposit_assets(),
            return_exceptions=True,
        )

        # Handle exceptions
        if isinstance(spot_data, Exception):
            logger.error(f"Failed to fetch spot data: {spot_data}")
            spot_data = []
        if isinstance(futures_data, Exception):
            logger.error(f"Failed to fetch futures data: {futures_data}")
            futures_data = []
        if isinstance(assets_data, Exception):
            logger.error(f"Failed to fetch assets data: {assets_data}")
            assets_data = []

        return self._build_token_map(spot_data, futures_data, assets_data)

    async def _fetch_spot_symbols(self) -> list[dict]:
        """Fetch spot trading pairs."""
        url = f"https://{self.SPOT_HOST}/api/v1/exchangeInfo"
        response = await self.http_client.get(url)

        if not response or "symbols" not in response:
            return []

        return [
            s for s in response["symbols"]
            if s.get("status") == "TRADING"
            and s.get("quoteAsset") == "USDT"  # ВАЖНО: только USDT!
        ]

    async def _fetch_futures_symbols(self) -> list[dict]:
        """Fetch futures/perpetual contracts."""
        url = f"https://{self.FUTURES_HOST}/fapi/v3/exchangeInfo"
        response = await self.http_client.get(url)

        if not response or "symbols" not in response:
            return []

        return [
            s for s in response["symbols"]
            if s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"  # ВАЖНО: только USDT!
        ]

    async def _fetch_deposit_assets(self) -> list[dict]:
        """Fetch deposit/withdrawal network info."""
        url = f"https://{self.ASSETS_HOST}/bapi/futures/v1/public/future/aster/deposit/assets"
        response = await self.http_client.get(url)
        return response if isinstance(response, list) else []

    def _build_token_map(
            self,
            spot_symbols: list[dict],
            futures_symbols: list[dict],
            assets: list[dict],
    ) -> list[dict]:
        """Build unified token map from all data sources."""

        # Build asset info lookup: symbol -> chains
        asset_chains = {}
        for asset in assets:
            symbol = asset.get("name", "").upper()
            if not symbol:
                continue

            chain_id = asset.get("chainId")
            chain_name = self.CHAIN_ID_MAP.get(chain_id)
            if not chain_name:
                continue

            if symbol not in asset_chains:
                asset_chains[symbol] = []

            asset_chains[symbol].append({
                "chain_name": chain_name,
                "contract_address": asset.get("contractAddress") or "",
                "deposit_enabled": asset.get("depositEnabled", True),
                "withdraw_enabled": asset.get("withdrawEnabled", True),
                "withdrawal_fee": asset.get("withdrawFee"),
            })

        token_map = []

        # Process spot symbols
        for sym in spot_symbols:
            base = sym.get("baseAsset", "").upper()
            quote = sym.get("quoteAsset", "").upper()
            symbol = sym.get("symbol", "")

            # ВАЖНО: Пропускаем плечевые токены (создают фантомные спреды)
            if base.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            token_map.append({
                "symbol": symbol,
                "base_asset": base,
                "base_coin_name": base,
                "quote_asset": quote,
                "market_type": MarketType.SPOT,
                "source_id": self.source.id,
                "chains": asset_chains.get(base, []),
            })

        # Process futures symbols
        for sym in futures_symbols:
            base = sym.get("baseAsset", "").upper()
            quote = sym.get("quoteAsset", "").upper()
            symbol = sym.get("symbol", "")
            contract_size = sym.get("contractSize", 1)

            # ВАЖНО: Пропускаем плечевые токены (создают фантомные спреды)
            if base.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
                continue

            token_map.append({
                "symbol": symbol,
                "base_asset": base,
                "base_coin_name": base,
                "quote_asset": quote,
                "market_type": MarketType.FUTURES,
                "source_id": self.source.id,
                "contract_size": contract_size,
                "chains": asset_chains.get(base, []),
            })

        return token_map

    async def save_to_db(self, token_map: list[dict]) -> None:
        """Save tokens to database."""
        await self.save_token_data_cex(token_map)
```

### 2.2 Ключевые моменты

1. **Наследование:** `BaseTokenParser` + `SupportsTokenParsing`
2. **source_name:** lowercase, без спецсимволов
3. **chains:** список сетей для каждого токена (для CEX-CEX арбитража)
4. **base_coin_name:** ключ для группировки токенов в global_token
5. **contract_size:** важно для фьючерсов (1000PEPE = 1000 PEPE)

### 2.3 КРИТИЧНО: Фильтрация токенов

**Обязательные фильтры для ВСЕХ бирж:**

#### 1. Только USDT пары
```python
# В _fetch_spot_symbols и _fetch_futures_symbols
if s.get("quoteAsset") == "USDT"  # НЕ USDC! Только USDT!
```

**Причина:** Система работает только с USDT парами для унификации и ликвидности.

**Пример на AscendEX:**
- API возвращает: 1133 USDT/USDC пары
- Загружено: 747 USDT пар (после фильтрации leveraged)

#### 2. Исключить плечевые (leveraged) токены
```python
# В _build_token_map для spot и futures
if base.endswith(("3S", "3L", "5S", "5L", "2S", "2L")):
    continue  # Пропускаем плечевые токены
```

**Причина:** Плечевые токены создают **фантомные спреды** из-за встроенного плеча.

**Примеры плечевых токенов:**
- `BTC3L` = 3x Long Bitcoin (цена в 3 раза выше обычного BTC)
- `ETH3S` = 3x Short Ethereum (обратная корреляция с ETH)
- `DOGE5L` = 5x Long Dogecoin
- `SOL2S` = 2x Short Solana

**Пример фантомного спреда:**
```
BTC на Binance: $50,000
BTC3L на Gate: $150,000 (3x плечо)
→ Фантомный спред: 200% (НЕ арбитраж!)
```

**Статистика по AscendEX:**
- Всего USDT/USDC пар: 1133
- Плечевые токены: 386 (34%!)
- После фильтрации: 747 валидных токенов ✅

#### 3. Только активные токены
```python
if s.get("status") == "TRADING"  # или "Normal", в зависимости от API
```

**Причина:** Неактивные токены нельзя торговать.

#### 4. Только perpetual контракты (для futures)
```python
if s.get("contractType") == "PERPETUAL"  # или "-PERP" в символе
```

**Причина:** Срочные контракты имеют дату экспирации, что усложняет арбитраж.

### 2.4 Проверка корректности фильтрации

После реализации проверьте:

```bash
# 1. API vs Загружено
curl -s "https://api.exchange.com/symbols" | jq '[.data[] | select(.quoteAsset == "USDT")] | length'
# Сравнить с количеством в БД

# 2. Плечевые токены должны быть исключены
docker exec database psql -U dex-dev -d dex-dev -c "
SELECT symbol FROM tokens WHERE base_asset ~ '(3S|3L|5S|5L|2S|2L)$' LIMIT 5;
"
# Должно быть 0 строк!

# 3. Только USDT
docker exec database psql -U dex-dev -d dex-dev -c "
SELECT DISTINCT quote_asset FROM tokens WHERE source_id = (SELECT id FROM sources WHERE name='exchange');
"
# Должно быть только 'USDT'
```

---

## 3. Создание WebSocket клиента

### 3.1 Структура файла

Путь: `backend/infrastructure/services/collectors/price_collector/ws/{exchange_name}.py`

```python
import asyncio
from typing import Callable, Any

import aiohttp
import orjson

from infrastructure.database.models.base import MarketType
from settings import logger


class AsterdexWSClient:
    """WebSocket client for Asterdex orderbook data."""

    SPOT_WS_URL = "wss://sstream.asterdex.com/stream"
    FUTURES_WS_URL = "wss://fstream.asterdex.com/stream"

    PING_INTERVAL = 30  # seconds
    RECONNECT_DELAY = 5  # seconds
    MAX_STREAMS_PER_CONNECTION = 200  # лимит биржи

    def __init__(
            self,
            symbols: list[str],
            update_callback: Callable[[str, dict], Any],
            market_type: MarketType,
            proxy_url: str | None = None,
    ):
        self.symbols = symbols
        self.update_callback = update_callback
        self.market_type = market_type
        self.proxy_url = proxy_url

        self.ws_url = self.SPOT_WS_URL if market_type == MarketType.SPOT else self.FUTURES_WS_URL
        self._running = False
        self._ws = None

    async def connect(self):
        """Main connection loop with auto-reconnect."""
        self._running = True

        while self._running:
            try:
                await self._run_connection()
            except Exception as e:
                logger.error(f"Asterdex WS error: {e}")

            if self._running:
                await asyncio.sleep(self.RECONNECT_DELAY)

    async def _run_connection(self):
        """Single connection lifecycle."""
        connector = aiohttp.TCPConnector(ssl=False)

        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.ws_connect(
                self.ws_url,
                proxy=self.proxy_url,
                heartbeat=self.PING_INTERVAL,
            ) as ws:
                self._ws = ws

                # Subscribe to orderbook streams
                await self._subscribe(ws)

                # Start ping task
                ping_task = asyncio.create_task(self._ping_loop(ws))

                try:
                    await self._listen(ws)
                finally:
                    ping_task.cancel()

    async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse):
        """Subscribe to orderbook depth streams."""
        # Binance-compatible format
        streams = [f"{sym.lower()}@depth@100ms" for sym in self.symbols]

        payload = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1,
        }

        await ws.send_str(orjson.dumps(payload).decode())
        logger.info(f"Asterdex: subscribed to {len(streams)} streams")

    async def _listen(self, ws: aiohttp.ClientWebSocketResponse):
        """Listen for incoming messages."""
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self._handle_message(msg.data)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.error(f"Asterdex WS error: {ws.exception()}")
                break

    async def _handle_message(self, data: str):
        """Parse and process incoming message."""
        try:
            parsed = orjson.loads(data)

            # Skip subscription confirmations
            if "result" in parsed or "id" in parsed:
                return

            # Extract stream name and data
            stream = parsed.get("stream", "")
            payload = parsed.get("data", parsed)

            # Parse symbol from stream name (btcusdt@depth -> BTCUSDT)
            if "@" in stream:
                symbol = stream.split("@")[0].upper()
            else:
                symbol = payload.get("s", "").upper()

            if not symbol:
                return

            # Build orderbook update
            orderbook = {
                "bids": [[float(p), float(q)] for p, q in payload.get("b", [])[:20]],
                "asks": [[float(p), float(q)] for p, q in payload.get("a", [])[:20]],
            }

            await self.update_callback(symbol, orderbook)

        except Exception as e:
            logger.debug(f"Failed to parse Asterdex message: {e}")

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        """Send periodic pings to keep connection alive."""
        while True:
            await asyncio.sleep(self.PING_INTERVAL)
            try:
                await ws.pong()
            except Exception:
                break

    async def close(self):
        """Close the connection."""
        self._running = False
        if self._ws:
            await self._ws.close()
```

### 3.2 Ключевые моменты

1. **proxy_url:** обязательный параметр для совместимости с другими клиентами
2. **Формат подписки:** зависит от API биржи (Binance-style, Huobi-style, etc.)
3. **Парсинг orderbook:** bids/asks в формате `[[price, quantity], ...]`
4. **Auto-reconnect:** обязателен для production
5. **market_type:** WebSocket клиент должен поддерживать оба типа рынка

### 3.3 Поддержка spot и futures в одном клиенте

**Правильный подход:**

```python
class ExchangeWSClient:
    SPOT_WS_URL = "wss://sstream.exchange.com/stream"
    FUTURES_WS_URL = "wss://fstream.exchange.com/stream"  # Если разные URL

    def __init__(
            self,
            symbols: list[str],
            update_callback: Callable,
            market_type: MarketType,  # КРИТИЧНО: принимать market_type
            proxy_url: str | None = None,
    ):
        self.market_type = market_type
        # Выбор URL в зависимости от market_type
        self.ws_url = (
            self.SPOT_WS_URL if market_type == MarketType.SPOT
            else self.FUTURES_WS_URL
        )
```

**Если биржа использует единый WebSocket URL:**

```python
class UnifiedWSClient:
    WS_URL = "wss://stream.exchange.com/ws"  # Единый для spot и futures

    def __init__(self, ..., market_type: MarketType):
        self.market_type = market_type
        self.ws_url = self.WS_URL  # Не меняется

    async def _subscribe(self, ws):
        # Формат подписки может различаться
        if self.market_type == MarketType.SPOT:
            streams = [f"{sym}@depth" for sym in self.symbols]
        else:  # FUTURES
            streams = [f"{sym}@depth@futures" for sym in self.symbols]
```

**НЕПРАВИЛЬНО (не делать!):**

```python
# Создание разных классов для spot и futures - дублирование кода
class ExchangeSpotWSClient:  # ❌
    pass

class ExchangeFuturesWSClient:  # ❌
    pass

# Вместо этого - один класс с параметром market_type
```

---

## 4. Создание Price Parser

### 4.1 Структура файла

Путь: `backend/infrastructure/services/collectors/price_collector/sources/{exchange_name}.py`

```python
from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base import BaseWSCexPriceParser
from infrastructure.services.collectors.price_collector.ws.asterdex import AsterdexWSClient
from infrastructure.services.common.cache import RedisCache


class AsterdexPriceParser(BaseWSCexPriceParser):
    """Price parser for Asterdex using WebSocket."""

    symbols_per_ws = 30  # Количество символов на одно WS соединение

    def __init__(
            self,
            cache: RedisCache,
            repo: RequestsRepo,
            market_type: MarketType,
    ):
        super().__init__(
            cache=cache,
            repo=repo,
            source_name="asterdex",
            market_type=market_type,
        )

    def _create_ws_client(
            self,
            symbols: list[str],
            callback,
            proxy_url: str | None = None,
    ) -> AsterdexWSClient:
        """Create WebSocket client instance."""
        return AsterdexWSClient(
            symbols=symbols,
            update_callback=callback,
            market_type=self.market_type,
            proxy_url=proxy_url,
        )
```

### 4.2 Ключевые моменты

1. **symbols_per_ws:** оптимальное количество (обычно 20-50)
   - Слишком мало (<10): много соединений, высокая нагрузка на биржу
   - Слишком много (>100): риск потери сообщений, высокая задержка
   - Оптимум: 30-50 для большинства бирж

2. **Наследование:** `BaseWSCexPriceParser` делает всю работу
   - Автоматическое создание нескольких WS клиентов
   - Балансировка символов между клиентами
   - Обработка обновлений и сохранение в Redis
   - Мониторинг устаревших данных (stale detection)

3. **_create_ws_client:** единственный метод для переопределения
   - Должен возвращать экземпляр WebSocket клиента
   - Передать все параметры без изменений

4. **source_name:** должен совпадать с именем в `token_collector` и `manager.py`

5. **stream_key:** ключ Redis для orderbook данных
   - Формат: `prices:{market_type}` (lowercase!)
   - Spot: `prices:spot`
   - Futures: `prices:futures`

### 4.3 Пример полной интеграции (AscendEX)

**Файл:** `backend/infrastructure/services/collectors/price_collector/sources/ascendex.py`

```python
from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base import BaseWSCexPriceParser
from infrastructure.services.collectors.price_collector.ws.ascendex import AscendexWSClient
from infrastructure.services.common.cache import RedisCache


class AscendexPriceParser(BaseWSCexPriceParser):
    """Price parser for AscendEX using WebSocket."""

    def __init__(
            self,
            repo: RequestsRepo,
            cache: RedisCache,
            market_type: MarketType = MarketType.SPOT,
    ):
        super().__init__(
            cache=cache,
            repo=repo,
            source_name="ascendex",  # КРИТИЧНО: точно как в token_collector!
            ws_client_cls=AscendexWSClient,  # Класс WS клиента
            symbols_per_ws=30,  # Оптимальное количество
            market_type=market_type,
            stream_key="prices:spot" if market_type == MarketType.SPOT else "prices:futures",
        )
```

**Почему так просто:**
- `BaseWSCexPriceParser` автоматически:
  1. Загружает символы из Redis (`available_tokens:{source}:{source_id}:{market_type}`)
  2. Разбивает символы на группы по `symbols_per_ws`
  3. Создает по одному WS клиенту на группу (вызывает `_create_ws_client`)
  4. Обрабатывает обновления orderbook
  5. Сохраняет в Redis Streams (`prices:spot` или `prices:futures`)
  6. Запускает мониторинг (WS monitor в логах)

**Если нужна кастомизация:**

```python
class CustomPriceParser(BaseWSCexPriceParser):
    # Переопределить количество символов динамически
    @property
    def symbols_per_ws(self) -> int:
        # Для больших бирж - меньше символов на соединение
        total_symbols = len(self.symbols)
        if total_symbols > 500:
            return 20
        return 30

    # Переопределить обработку сообщений (редко нужно)
    async def _process_update(self, symbol: str, data: dict):
        # Кастомная логика перед сохранением
        if symbol == "BTCUSDT":
            logger.info(f"BTC price: {data['asks'][0][0]}")
        await super()._process_update(symbol, data)
```

---

## 5. Регистрация в менеджерах

### 5.1 Token Collector Manager

Файл: `backend/infrastructure/services/collectors/token_collector/manager.py`

```python
# Добавить импорт
from infrastructure.services.collectors.token_collector.sources.asterdex import AsterdexTokenParser

# В функции get_token_collector_manager() добавить в список sources:
async def get_token_collector_manager(...):
    sources = [
        # ... existing sources ...
        AsterdexTokenParser(cache, chain_normalizer, repo, http_client),
    ]
```

### 5.2 Price Collector Manager

Файл: `backend/infrastructure/services/collectors/price_collector/manager.py`

```python
# Добавить импорт
from infrastructure.services.collectors.price_collector.sources.asterdex import AsterdexPriceParser

# В словарь available_sources:
available_sources = {
    # ... existing sources ...
    "asterdex": AsterdexPriceParser,
}
```

---

## 6. Настройка Docker

### 6.1 Добавление сервисов в docker-compose

Файл: `docker/dev.yml` (или `prod.yml`)

```yaml
services:
  # ... existing services ...

  price-collector-spot-asterdex:
    build:
      context: ../backend
      dockerfile: ../docker/price-collector/Dockerfile
      target: test-build
    container_name: docker-price-collector-spot-asterdex-1
    restart: unless-stopped
    env_file:
      - .env.dev
    environment:
      - PRICE_COLLECTOR_SOURCE=asterdex
      - PRICE_COLLECTOR_MARKET_TYPE=SPOT
      - OPTIMIZE_USE_STREAMS=true  # Важно для производительности!
    volumes:
      - ../backend:/app
    depends_on:
      - redis
      - postgres
    networks:
      - dex-network

  price-collector-futures-asterdex:
    build:
      context: ../backend
      dockerfile: ../docker/price-collector/Dockerfile
      target: test-build
    container_name: docker-price-collector-futures-asterdex-1
    restart: unless-stopped
    env_file:
      - .env.dev
    environment:
      - PRICE_COLLECTOR_SOURCE=asterdex
      - PRICE_COLLECTOR_MARKET_TYPE=FUTURES
      - OPTIMIZE_USE_STREAMS=true
    volumes:
      - ../backend:/app
    depends_on:
      - redis
      - postgres
    networks:
      - dex-network
```

### 6.2 Переменные окружения

| Переменная | Описание | Пример |
|-----------|----------|--------|
| `SOURCE_NAME` | Имя источника (как в manager.py) | `asterdex` |
| `MARKET_TYPE` | Тип рынка | `spot` или `futures` (lowercase!) |
| `OPTIMIZE_USE_STREAMS` | Использовать Redis Streams | `true` (рекомендуется) |

### 6.3 КРИТИЧНО: Один контейнер = одна биржа + один market_type

**Правило:** Для каждой биржи создается **2 отдельных контейнера**:
1. `price-collector-spot-{exchange}`
2. `price-collector-futures-{exchange}`

**НЕПРАВИЛЬНО (не делать!):**
```yaml
# Один контейнер на всю биржу - НЕ РАБОТАЕТ!
price-collector-asterdex:
  environment:
    SOURCE_NAME: asterdex
    # MARKET_TYPE отсутствует - какой рынок собирать?
```

**ПРАВИЛЬНО:**
```yaml
# Spot контейнер
price-collector-spot-asterdex:
  command: python infrastructure/services/collectors/price_collector/main.py
  environment:
    MARKET_TYPE: spot      # Только spot токены
    SOURCE_NAME: asterdex

# Futures контейнер (отдельный!)
price-collector-futures-asterdex:
  command: python infrastructure/services/collectors/price_collector/main.py
  environment:
    MARKET_TYPE: futures   # Только futures токены
    SOURCE_NAME: asterdex
```

**Почему так:**
1. **Разные WebSocket URL** (spot vs futures) у некоторых бирж
2. **Разные наборы токенов** (spot != futures)
3. **Независимая масштабируемость** (можно отключить futures, оставить spot)
4. **Изоляция ошибок** (падение futures не влияет на spot)

### 6.4 Количество контейнеров на биржу

**Минимум:** 2 контейнера (spot + futures)

**Масштабирование (если биржа большая):**
```yaml
# Можно запустить несколько инстансов price-collector
price-collector-spot-binance:
  deploy:
    replicas: 3  # 3 параллельных воркера для spot
```

**Когда масштабировать:**
- Биржа имеет >500 торговых пар
- WS monitor показывает `stale > 100` (много устаревших токенов)
- Высокая нагрузка на CPU/память

**Пример dev.yml (AscendEX):**
```yaml
services:
  price-collector-spot-ascendex:
    build:
      context: ..
      dockerfile: backend/api/Dockerfile
    command: python infrastructure/services/collectors/price_collector/main.py
    env_file: .env.dev
    environment:
      PYTHONPATH: /app
      MARKET_TYPE: spot          # КРИТИЧНО
      SOURCE_NAME: ascendex      # КРИТИЧНО
    depends_on:
      - redis
      - postgres
      - price_distributor        # КРИТИЧНО: distributor должен быть запущен!
    restart: unless-stopped
    networks:
      - shared_network

  price-collector-futures-ascendex:  # Отдельный контейнер!
    build:
      context: ..
      dockerfile: backend/api/Dockerfile
    command: python infrastructure/services/collectors/price_collector/main.py
    env_file: .env.dev
    environment:
      PYTHONPATH: /app
      MARKET_TYPE: futures       # Другой market_type
      SOURCE_NAME: ascendex
    depends_on:
      - redis
      - postgres
      - price_distributor
    restart: unless-stopped
    networks:
      - shared_network
```

**ОШИБКА: 2 контейнера от одной биржи:**
Пользователь спросил: "почему 2 контейнера фьючерсов от аскендекса"

Причина: В `dev.yml` были добавлены:
- `price-collector-futures-ascendex` (правильно)
- `price-collector-futures-asterdex` (ошибочно, старая биржа)

**Решение:** Проверять имена бирж перед добавлением сервисов!

---

## 7. Маппинг сетей

### 7.1 Chain Normalizer

Файл: `backend/infrastructure/services/collectors/structures/normalizer.py`

Если биржа использует нестандартные названия сетей, добавить маппинг:

```python
# В классе ChainNormalizer или в maps.py
ASTERDEX_CHAIN_MAP = {
    "EVM": "ETH",      # Если биржа называет Ethereum как "EVM"
    "BSC": "BSC",
    "ARBITRUM": "ARB",
    "SOLANA": "SOL",
}
```

### 7.2 Block Time

Файл: `backend/infrastructure/services/collectors/structures/maps.py`

```python
chain_block_time = {
    # ... existing chains ...
    "ETH": 12,
    "BSC": 3,
    "ARB": 0.25,
    "SOL": 0.4,
}
```

---

## 8. Тестирование и деплой

### 8.1 Локальное тестирование

```bash
# 1. Проверка импортов
cd backend
python -c "from infrastructure.services.collectors.token_collector.sources.asterdex import AsterdexTokenParser; print('OK')"
python -c "from infrastructure.services.collectors.price_collector.sources.asterdex import AsterdexPriceParser; print('OK')"

# 2. Type check
python -m mypy infrastructure/services/collectors/token_collector/sources/asterdex.py
python -m mypy infrastructure/services/collectors/price_collector/sources/asterdex.py

# 3. Тест API вручную
python -c "
import asyncio
from infrastructure.services.common.http_client import HTTPClientPool

async def test():
    client = HTTPClientPool()
    await client.init()
    data = await client.get('https://sapi.asterdex.com/api/v1/exchangeInfo')
    print(f'Spot symbols: {len(data.get(\"symbols\", []))}')

asyncio.run(test())
"
```

### 8.2 Деплой на сервер

```bash
# 1. Коммит и пуш
git add -A
git commit -m "feat: add Asterdex exchange integration"
git push origin develop

# 2. На сервере
ssh user@server
cd /home/finder-bot
git pull origin develop

# 3. Запуск новых сервисов
cd docker
docker compose -f dev.yml up -d --build \
    token-collector \
    price-collector-spot-asterdex \
    price-collector-futures-asterdex \
    price_distributor

# 4. Проверка логов
docker compose -f dev.yml logs -f token-collector | grep -i asterdex
docker compose -f dev.yml logs -f price-collector-spot-asterdex
docker compose -f dev.yml logs -f price-collector-futures-asterdex
```

### 8.3 Проверка результатов

```bash
# Количество токенов в Redis
docker compose -f dev.yml exec redis redis-cli SCARD 'available_tokens:asterdex:*:SPOT'
docker compose -f dev.yml exec redis redis-cli SCARD 'available_tokens:asterdex:*:FUTURES'

# Проверка в БД
docker compose -f dev.yml exec postgres psql -U dex-dev -d dex-dev -c "
SELECT market_type, COUNT(*)
FROM tokens t
JOIN sources s ON t.source_id = s.id
WHERE s.name = 'asterdex'
GROUP BY market_type;
"

# Проверка price collector (WS monitor)
docker compose -f dev.yml logs price-collector-spot-asterdex --tail 5
# Ожидаемый вывод: WS monitor: total=41, stale=16, threshold=600s
```

### 8.4 Порядок запуска сервисов (КРИТИЧНО!)

**Правильная последовательность запуска:**

```bash
# 1. Базовые сервисы (если еще не запущены)
docker compose -f dev.yml up -d redis postgres

# 2. Token Collector - создает source и токены в БД
docker compose -f dev.yml up -d token-collector

# 3. Ждем создания токенов (10-30 секунд)
docker compose -f dev.yml logs token-collector -f | grep -i ascendex
# Ожидаем: "Saved X spot tokens for ascendex"
# Ctrl+C когда увидели сообщение

# 4. Price Distributor - загружает токены в Redis
docker compose -f dev.yml up -d price_distributor

# 5. Ждем загрузки в Redis (5-10 секунд)
docker compose -f dev.yml logs price_distributor -f | grep -i ascendex
# Ожидаем: "Added X new tokens into Redis for ascendex/spot"
# Ctrl+C когда увидели сообщение

# 6. Price Collectors - подписываются на WebSocket
docker compose -f dev.yml up -d \
    price-collector-spot-ascendex \
    price-collector-futures-ascendex

# 7. Проверка активации (через 30 секунд)
docker compose -f dev.yml logs price-collector-spot-ascendex --tail 3
# Ожидаемый вывод: WS monitor: total=100+, stale=<10
```

**Зависимости в docker-compose.yml:**
```yaml
price-collector-spot-ascendex:
  depends_on:
    - redis              # Нужен для чтения токенов
    - postgres           # Нужен для загрузки metadata
    - bot                # Опционально, для логов в Telegram
    - price_distributor  # КРИТИЧНО! Должен загрузить токены в Redis
```

**Типичная ошибка - запуск без distributor:**
```bash
# НЕПРАВИЛЬНО - price-collector запущен до distributor
docker compose -f dev.yml up -d price-collector-spot-ascendex

# Лог price-collector:
# "No tokens found in Redis for ascendex/spot"
# WS monitor: total=0, stale=0
```

**Решение:**
```bash
# Перезапустить в правильном порядке
docker compose -f dev.yml restart price_distributor
sleep 10
docker compose -f dev.yml restart price-collector-spot-ascendex
```

### 8.5 Диагностика проблем запуска

**Проблема 1: Source not found in database**
```
ERROR: Source ascendex not found in database
```
**Причина:** Token collector еще не создал source (или упал с ошибкой)
**Решение:**
```bash
# Проверить логи token-collector
docker compose -f dev.yml logs token-collector --tail 100 | grep -i error

# Если ошибок нет - перезапустить
docker compose -f dev.yml restart token-collector
```

**Проблема 2: No tokens in Redis**
```
INFO: No tokens found in Redis for ascendex/spot
WS monitor: total=0
```
**Причина:** Distributor не загрузил токены
**Решение:**
```bash
# Проверить есть ли токены в БД
docker exec database psql -U dex-dev -d dex-dev -c "
SELECT COUNT(*) FROM tokens t
JOIN sources s ON t.source_id = s.id
WHERE s.name = 'ascendex';
"
# Если 0 - проблема в token-collector
# Если >0 - проблема в distributor

# Перезапустить distributor
docker compose -f dev.yml restart price_distributor

# Проверить логи
docker compose -f dev.yml logs price_distributor -f | grep ascendex
```

**Проблема 3: Container restart loop**
```bash
docker ps | grep ascendex
# STATUS: Restarting (1) 3 seconds ago
```
**Причина:** Ошибка в коде или неправильные environment variables
**Решение:**
```bash
# Смотреть логи (они могут исчезать из-за restart)
docker compose -f dev.yml logs price-collector-spot-ascendex --tail 50

# Типичные ошибки:
# - ImportError (неправильный импорт в коде)
# - KeyError MARKET_TYPE (не указан в environment)
# - SOURCE_NAME не найден в available_sources (опечатка)
```

---

## 9. Типичные проблемы и решения

### 9.1 Phantom Spreads (разные токены с одинаковым символом)

**Проблема:** Токен Q на Asterdex ($0.02) смержился с Q на XT ($0.0000003), создавая phantom spread 6674019%.

**Причина:** Токены без chain info мержатся по символу в один global_token.

**Решение:** Система автоматически создает source-specific global tokens для токенов без chain info с форматом `{symbol}:{source_name}` (например, `Q:asterdex`).

### 9.2 Низкое количество токенов

**Проблема:** Загружается меньше токенов, чем есть на бирже.

**Возможные причины:**
1. Фильтр по `status == "TRADING"` отсекает неактивные
2. Фильтр по `quoteAsset in ("USDT", "USDC")` отсекает другие пары
3. Токены без chain info раньше не сохранялись (исправлено)

**Решение:** Проверить фильтры в `fetch_*` методах и убедиться что `save_token_data_cex` сохраняет все токены.

### 9.3 Высокий CPU в price-collector

**Проблема:** Контейнер price-collector потребляет 100%+ CPU.

**Причина:** Не включен режим Redis Streams.

**Решение:** Добавить `OPTIMIZE_USE_STREAMS=true` в environment сервиса.

### 9.4 WebSocket не подключается

**Проблема:** WS monitor показывает `total=0`.

**Возможные причины:**
1. Неправильный URL WebSocket
2. Неправильный формат подписки
3. Биржа блокирует IP

**Решение:**
```bash
# Проверить логи на ошибки
docker compose -f dev.yml logs price-collector-spot-asterdex --tail 100 | grep -i error

# Тест WS вручную через wscat
wscat -c "wss://sstream.asterdex.com/stream"
> {"method": "SUBSCRIBE", "params": ["btcusdt@depth"], "id": 1}
```

### 9.5 Токены не появляются в Redis

**Проблема:** Token collector работает, но в Redis нет токенов.

**Причина:** Distributor не загрузил токены (нет global_token_sources).

**Решение:**
```bash
# Перезапустить distributor
docker compose -f dev.yml restart price_distributor

# Проверить логи
docker compose -f dev.yml logs price_distributor --tail 50 | grep -i asterdex
```

### 9.6 Мультипликаторы (1000PEPE, 1000SHIB)

**Проблема:** Фьючерсные контракты типа 1000PEPE представляют 1000 единиц базового актива.

**Решение:** Убедиться что `contract_size` правильно парсится из API и сохраняется в БД.

```python
# В token parser
contract_size = sym.get("contractSize", 1)
# или
contract_size = sym.get("contractMultiplier", 1)
```

### 9.7 Частые ошибки при реализации

#### Ошибка 1: Неправильный формат символа в WebSocket

**Проблема:**
```python
# В token_collector: "BTC/USDT"
# В WS клиенте: подписка на "btcusdt"
# Результат: WS не получает обновления
```

**Решение:** Убедиться что формат символа совпадает с требованиями биржи.

```python
# AscendEX требует "BTC/USDT" (с разделителем)
payload = {"op": "sub", "ch": f"depth:{symbol}"}  # symbol = "BTC/USDT"

# Binance-style требует "btcusdt" (без разделителя, lowercase)
streams = [f"{sym.lower()}@depth" for sym in symbols]  # "BTCUSDT" → "btcusdt"
```

#### Ошибка 2: Забыли добавить ping/pong для custom protocols

**Проблема:**
```python
# Биржа отключает WS через 60 секунд без pong ответа
# WS monitor: total падает до 0 каждую минуту
```

**Решение:** Явная обработка ping/pong для non-Binance бирж.

```python
async def _handle_message(self, ws, raw: str):
    msg = orjson.loads(raw)

    # КРИТИЧНО для AscendEX, Hyperliquid и других
    if msg.get("m") == "ping" or msg.get("op") == "ping":
        await ws.send_str(orjson.dumps({"op": "pong"}).decode())
        return  # Не обрабатывать как orderbook update!
```

#### Ошибка 3: Не передан market_type в WS клиент

**Проблема:**
```python
class BadWSClient:
    def __init__(self, symbols, callback):
        # market_type не принимается
        self.ws_url = "wss://stream.exchange.com"  # Всегда spot URL!
```

**Результат:** Futures контейнер подключается к spot WebSocket.

**Решение:**
```python
class GoodWSClient:
    SPOT_WS_URL = "wss://sstream.exchange.com"
    FUTURES_WS_URL = "wss://fstream.exchange.com"

    def __init__(self, ..., market_type: MarketType):  # КРИТИЧНО!
        self.ws_url = (
            self.SPOT_WS_URL if market_type == MarketType.SPOT
            else self.FUTURES_WS_URL
        )
```

#### Ошибка 4: Парсинг orderbook не обрабатывает разные форматы

**Проблема:**
```python
# Binance: {"b": [["50000", "1.5"]], "a": [...]}
# AscendEX: {"bids": [[50000, 1.5]], "asks": [...]}
# Код парсит только один формат
```

**Решение:** Универсальный парсер или проверка формата.

```python
async def _handle_message(self, data: str):
    parsed = orjson.loads(data)
    payload = parsed.get("data", parsed)

    # Поддержка обоих форматов
    bids_raw = payload.get("b") or payload.get("bids", [])
    asks_raw = payload.get("a") or payload.get("asks", [])

    # Нормализация к float
    orderbook = {
        "bids": [[float(p), float(q)] for p, q in bids_raw[:20]],
        "asks": [[float(p), float(q)] for p, q in asks_raw[:20]],
    }
```

#### Ошибка 5: Не фильтруются confirmation messages

**Проблема:**
```python
# WebSocket отправляет подтверждение подписки
{"result": null, "id": 1}
# Код пытается обработать как orderbook → exception
```

**Решение:**
```python
async def _handle_message(self, data: str):
    parsed = orjson.loads(data)

    # Пропустить subscription confirmations
    if "result" in parsed or "id" in parsed:
        return

    # Пропустить пустые сообщения
    if not parsed.get("data") and not parsed.get("s"):
        return

    # Обработать orderbook...
```

### 9.8 Futures токены не загружаются в Redis (только малая часть активна)

**Проблема:** WebSocket price collector показывает очень мало активных futures токенов (например, 27 вместо 264).

**Симптомы:**
```bash
# В логах price-collector
WS monitor: total=27, stale=0, threshold=600s
# А в БД токенов гораздо больше
```

**Причина:** Distributor (`price_collector/distributor.py`) не может найти `global_token_id` для futures токенов без chain info.

**Детали проблемы:**
- Futures токены часто не имеют `token_on_chains` (нет информации о сетях)
- При сохранении таких токенов система добавляет суффиксы к `base_coin_name`: `_FUTURES`, `_F100`, `_F1000` и т.д.
- Distributor пытается найти `global_token_id` только по `base_asset` (без суффиксов)
- Не находит → пропускает токен → токен не попадает в Redis → не активируется в WebSocket

**Решение:** Добавлен fallback механизм в distributor для поиска с суффиксами.

**Файл:** `backend/infrastructure/services/collectors/price_collector/distributor.py`

```python
async def load_source_tokens_incremental(source, repo: RequestsRepo, redis: RedisCache, tokens_load_limit: int):
    # ... existing code ...

    for tok in tokens:
        # Try to get global_token_id from token_on_chains first
        global_token_id = None
        if tok.token_on_chains:
            global_token_id = tok.token_on_chains[0].global_token_id

        # If no token_on_chains, try to find by base_asset symbol
        if not global_token_id:
            global_token_id = symbol_to_global_id.get(tok.base_asset)

        # КРИТИЧНО: Для FUTURES токенов без chains пробуем суффиксы
        if not global_token_id and tok.market_type == MarketType.FUTURES:
            # Эти суффиксы создаются в save_token_data_cex для futures без chains
            for suffix in ["_FUTURES", "_F100", "_F1000", "_F10", "_F0"]:
                candidate = f"{tok.base_asset}{suffix}"
                global_token_id = symbol_to_global_id.get(candidate)
                if global_token_id:
                    break

        if not global_token_id:
            continue  # Токен не найден, пропускаем

        # ... save to Redis ...
```

**Проверка после исправления:**
```bash
# Перезапустить distributor
docker compose -f dev.yml restart price_distributor

# Проверить логи - должно быть больше токенов
docker compose -f dev.yml logs price_distributor --tail 50 | grep "Added"
# Ожидаемо: "Added 237 new tokens" вместо "Added 0"

# Проверить в price-collector - WS monitor
docker compose -f dev.yml logs price-collector-futures-asterdex --tail 5
# Ожидаемо: total=264 вместо 27
```

**Результат на Asterdex:** 27 → 264 активных futures токенов (98% прирост!)

**Когда применять:** Если после добавления биржи futures токены не активируются в WebSocket, проверьте этот механизм.

---

## 10. Верификация интеграции

### 10.1 Проверка мержа токенов

После деплоя важно убедиться, что токены правильно объединяются с другими биржами.

```bash
# Проверить, с какими биржами смержились токены новой биржи
docker exec database psql -U dex-dev -d dex-dev -c "
SELECT gt.symbol, array_agg(DISTINCT s.name ORDER BY s.name) as sources, COUNT(DISTINCT s.id) as cnt
FROM global_tokens gt
JOIN global_token_sources gts ON gt.id = gts.global_token_id
JOIN sources s ON gts.source_id = s.id
WHERE gt.symbol IN ('BTC', 'ETH', 'SOL', 'DOGE', 'PEPE', 'WIF')
GROUP BY gt.symbol
ORDER BY gt.symbol;
"
# Ожидаемый результат: новая биржа в списке sources для основных токенов
```

```bash
# Проверить токены с суффиксом :source_name (не смержились)
docker exec database psql -U dex-dev -d dex-dev -c "
SELECT gt.symbol, COUNT(*) as count
FROM global_tokens gt
WHERE gt.symbol LIKE '%:asterdex'
GROUP BY gt.symbol
LIMIT 20;
"
# Это source-specific токены, которые не имеют chain info
```

### 10.2 Проверка формирования сигналов

```bash
# Проверить spread-collector на результаты
docker logs docker-spread-collector-1 --tail 50 2>&1 | grep "results="
# Ожидаемый вывод: results=N где N > 0 означает найденные спреды

# Проверить spread_loader для конкретного токена
docker logs spread_loader --tail 100 2>&1 | grep -i asterdex
```

### 10.3 Проверка ошибок мержа

```bash
# Смотреть rejected merge candidates
docker logs docker-token-collector-1 2>&1 | grep -i "Rejected merge candidate" | tail -20
# Типичные причины:
# - "contract conflict" - разные контракты на одной сети (это правильно, не мержить)
# - "price ratio too large" - слишком большая разница цен (разные токены)
# - "multiplied token conflict" - один токен с мультипликатором (1000PEPE vs PEPE)
```

### 10.4 Мерж токенов без chain info (price-based merge)

Биржи без информации о сетях (например, Hyperliquid - DEX на собственном L1) используют **проверку по цене** для определения идентичности токенов.

**Как это работает:**

1. Для **доверенных источников** (`TRUSTED_SOURCES` в `base.py`): hyperliquid, binance, bybit, okx, kucoin, coinbase
2. При создании токена передается поле `price` (цена из API биржи)
3. Система сравнивает цену нового токена с ценой существующего global_token
4. Если разница ≤ 3% (`MAX_PRICE_DIFF_PCT`) - токены мержатся
5. Если разница > 3% - создается source-specific токен (`{symbol}:{source_name}`)

**Примеры из логов:**
```
# Успешный мерж (цены совпадают)
Merged BTC from hyperliquid with existing global_token (price diff: 0.11%)
Merged ETH from hyperliquid with existing global_token (price diff: 0.44%)

# Отклоненный мерж (цены сильно отличаются - разные токены)
Rejected merge for GAS from hyperliquid: price diff 66.70% > 3.0%
Rejected merge for YGG from hyperliquid: price diff 9.78% > 3.0%
```

**Добавление price поля в token parser:**
```python
# В _build_token_map()
token_entry = {
    "symbol": symbol,
    "base_coin_name": base_symbol,
    ...
    "price": float(all_mids.get(name, 0)) / contract_size,  # Цена для валидации мержа
}
```

**Важно:** Поле `price` используется **только для валидации** мержа и должно быть удалено перед сохранением в БД:
```python
# В save_to_db() или save_token_data_cex()
for token in token_map:
    token.pop("price", None)  # Удалить временное поле
```

**Проверка price-based мержа:**
```bash
# Смотреть успешные мержи с проверкой цены
docker logs docker-token-collector-1 2>&1 | grep "Merged.*price diff"

# Смотреть отклоненные мержи из-за разницы цен
docker logs docker-token-collector-1 2>&1 | grep "Rejected merge.*price diff"
```

### 10.5 Мультипликаторы токенов (kPEPE, 1000SHIB) - КРИТИЧНО!

Некоторые биржи торгуют **мультиплицированными токенами** - контрактами, представляющими N единиц базового актива:
- `kPEPE` = 1000 PEPE (Hyperliquid)
- `kBONK` = 1000 BONK
- `kSHIB` = 1000 SHIB
- `1000PEPE` = 1000 PEPE (Binance, Bybit)
- `1000SHIB` = 1000 SHIB

**Проблема phantom spreads:**

Если kPEPE смержится с обычным PEPE, возникает phantom spread ~99900%:
- PEPE на MEXC: $0.00000425
- kPEPE на Hyperliquid: $0.00425 (в 1000 раз больше!)

**Правильная обработка мультипликаторов:**

1. **НЕ разворачивать** мультиплицированные символы в базовые
2. **Сохранять оригинальный символ** как `base_asset` и `base_coin_name`
3. Система price-based merge автоматически отклонит их из-за разницы цен > 3%

```python
# ПРАВИЛЬНО - НЕ разворачивать мультипликаторы
def _get_base_symbol(self, symbol: str) -> tuple[str, float]:
    """Extract base symbol and multiplier."""
    for prefix, multiplier in self.MULTIPLIER_PREFIXES.items():
        if symbol.startswith(prefix) and len(symbol) > len(prefix):
            return symbol[len(prefix):], multiplier  # ("PEPE", 1000)
    return symbol, 1.0

def _build_token_map(self, ...):
    base_symbol, multiplier = self._get_base_symbol(name)
    is_multiplied = multiplier > 1

    # Для мультиплицированных - оставить оригинальный символ
    effective_base = name if is_multiplied else base_symbol

    token_entry = {
        "symbol": name,                    # kPEPE (оригинал)
        "base_asset": effective_base,      # kPEPE (не PEPE!)
        "base_coin_name": effective_base,  # kPEPE
        "contract_size": multiplier,       # 1000
        "price": float(all_mids.get(name, 0)),  # Без деления!
    }
```

**НЕПРАВИЛЬНО (создает phantom spreads):**
```python
# НЕ ДЕЛАТЬ ТАК!
token_entry = {
    "symbol": "kPEPE",
    "base_asset": "PEPE",        # ОШИБКА! Смержится с обычным PEPE
    "base_coin_name": "PEPE",
    "price": price / 1000,       # ОШИБКА! Фиктивная цена
}
```

**Проверка правильности:**
```bash
# Мультиплицированные токены должны быть отдельными global_tokens
docker exec database psql -U dex-dev -d dex-dev -c "
SELECT symbol, COUNT(DISTINCT gts.source_id) as sources
FROM global_tokens gt
LEFT JOIN global_token_sources gts ON gt.id = gts.global_token_id
WHERE symbol IN ('kPEPE', 'kBONK', 'kFLOKI', 'kSHIB', 'kLUNC', 'kNEIRO')
GROUP BY symbol;
"
# Ожидаемо: каждый k-токен только от одного источника (hyperliquid)

# Обычные токены НЕ должны содержать hyperliquid (для k-версий)
docker exec database psql -U dex-dev -d dex-dev -c "
SELECT gt.symbol, array_agg(DISTINCT s.name) as sources
FROM global_tokens gt
JOIN global_token_sources gts ON gt.id = gts.global_token_id
JOIN sources s ON gts.source_id = s.id
WHERE gt.symbol IN ('PEPE', 'BONK', 'FLOKI', 'SHIB', 'LUNC')
GROUP BY gt.symbol;
"
# Ожидаемо: hyperliquid НЕ в списке sources (у них только k-версии)
```

**Если обнаружены неправильные мержи - удалить:**
```sql
-- Удалить некорректные связи
DELETE FROM global_token_sources gts
WHERE gts.source_id = (SELECT id FROM sources WHERE name = 'hyperliquid')
AND gts.global_token_id IN (
    SELECT gt.id FROM global_tokens gt
    WHERE gt.symbol IN ('PEPE', 'BONK', 'FLOKI', 'SHIB', 'LUNC', 'NEIRO')
);
```

### 10.6 Ссылки на торговлю

При добавлении биржи нужно добавить генерацию ссылок на торговлю:

**Файл:** `backend/infrastructure/services/collectors/structures/utils.py`

```python
def generate_market_link(market: str, symbol: str, ..., source: str = "") -> str:
    match market:
        case "spot":
            match source:
                # ... existing ...
                case "asterdex":
                    # Убрать underscore из символа: TAG_USDT -> TAGUSDT
                    return f"https://www.asterdex.com/en/trade/pro/spot/{symbol.replace('_', '')}"
        case "futures":
            match source:
                # ... existing ...
                case "asterdex":
                    return f"https://www.asterdex.com/en/trade/pro/futures/{symbol.replace('_', '')}"
```

**Проверить формат ссылок:**
```bash
# Открыть реальную страницу биржи и сравнить URL
# Правильно: https://www.asterdex.com/en/trade/pro/spot/TAGUSDT
# Неправильно: https://www.asterdex.com/en/trade/pro/spot/TAG_USDT
```

---

### 10.7 Типичные числовые метрики (референсные значения)

При интеграции новой биржи полезно сравнить результаты с существующими биржами:

**Количество токенов (примеры реальных бирж):**

| Биржа | API (всего) | После фильтров | Leveraged | % filtered | Market types |
|-------|-------------|----------------|-----------|------------|-------------|
| AscendEX | 1133 | 747 | 386 | 34% | spot+futures |
| Asterdex | ~300 | 264 | ~36 | 12% | spot+futures |
| MEXC | ~2000 | ~1600 | ~400 | 20% | spot+futures |
| Gate.io | ~3000 | ~2400 | ~600 | 20% | spot+futures |
| Hyperliquid | 141 | 141 | 0 | 0% | только futures |

**WS monitor метрики (здоровые значения):**
```
WS monitor: total=264, stale=3, threshold=600s
```
- `total`: количество активных токенов (должно быть близко к count в БД)
- `stale`: устаревшие >10 минут (должно быть <5% от total)
- `threshold`: порог устаревания в секундах

**Красные флаги:**
- `total < 10%` от count в БД → проблема с distributor или Redis
- `stale > 50%` от total → проблема с WebSocket (дисконнекты, rate limits)
- `total = 0` → distributor не загрузил токены или неправильный market_type

**Время активации (от запуска до первых данных):**
- Token collector: 10-60 секунд (парсинг API + сохранение в БД)
- Distributor: 5-10 секунд (загрузка из БД в Redis)
- Price collector: 10-30 секунд (WebSocket подключение + первые updates)
- **Итого:** ~1-2 минуты до полной активации

**Проверка после 5 минут работы:**
```bash
# Должно быть примерно одинаково
TOKENS_IN_DB=$(docker exec database psql -U dex-dev -d dex-dev -tAc "SELECT COUNT(*) FROM tokens WHERE source_id=(SELECT id FROM sources WHERE name='ascendex') AND market_type='SPOT'")
TOKENS_IN_REDIS=$(docker exec redis redis-cli SCARD 'available_tokens:ascendex:*:SPOT')

echo "DB: $TOKENS_IN_DB, Redis: $TOKENS_IN_REDIS"
# Ожидаемо: разница <5%
```

**Если разница >20%:**
- Проверить логи distributor на ошибки
- Проверить что distributor fallback для futures работает (см. 9.7)
- Проверить что нет конфликтов global_token_id

---

## 11. Обновление веб-интерфейса (Prices Tab)

### 11.1 Обязательно при добавлении любой новой биржи

После добавления price-collector новой биржи **обязательно** обновить веб-дашборд, иначе данные биржи не будут отображаться в Prices Tab.

Файлы для изменения:
- `backend/webapp/lib/utils.ts` — метаданные бирж (название, цвет, URL)
- `backend/webapp/components/PricesTable.tsx` — колонки таблицы

### 11.2 utils.ts — добавить в три места

**1. EXCHANGE_LABELS** (отображаемое название):
```typescript
const EXCHANGE_LABELS: Record<string, string> = {
  // ... existing ...
  bybit:   'Bybit',
  kucoin:  'KuCoin',
  binance: 'Binance',
  bitget:  'Bitget',
  okx:     'OKX',
}
```

**2. EXCHANGE_COLORS** (цветовая схема бейджа):
```typescript
const EXCHANGE_COLORS: Record<string, {...}> = {
  // ... existing ...
  bybit:   { bg: 'bg-yellow-500/15', text: 'text-yellow-400', border: 'border-yellow-500/30' },
  kucoin:  { bg: 'bg-green-500/15',  text: 'text-green-400',  border: 'border-green-500/30' },
  binance: { bg: 'bg-amber-400/15',  text: 'text-amber-300',  border: 'border-amber-400/30' },
  bitget:  { bg: 'bg-cyan-500/15',   text: 'text-cyan-400',   border: 'border-cyan-500/30' },
  okx:     { bg: 'bg-indigo-500/15', text: 'text-indigo-400', border: 'border-indigo-500/30' },
}
```

**3. getExchangeUrl** (ссылки на страницу торговли):
```typescript
export function getExchangeUrl(sourceKey: string, symbol: string): string | null {
  // ...
  switch (exchange) {
    // ... existing ...
    case 'bybit':
      if (market === 'spot')    return `https://www.bybit.com/trade/spot/${sym}/USDT`
      if (market === 'futures') return `https://www.bybit.com/trade/futures/usdt/${sym}USDT`
      break
    case 'kucoin':
      if (market === 'spot')    return `https://www.kucoin.com/trade/${sym}-USDT`
      if (market === 'futures') return `https://www.kucoin.com/futures/${sym}USDTM`
      break
    case 'binance':
      if (market === 'spot')    return `https://www.binance.com/trade/${sym}_USDT`
      if (market === 'futures') return `https://www.binance.com/futures/${sym}USDT`
      break
    case 'bitget':
      if (market === 'spot')    return `https://www.bitget.com/spot/${sym}USDT`
      if (market === 'futures') return `https://www.bitget.com/futures/${sym}USDT`
      break
    case 'okx':
      if (market === 'spot')    return `https://www.okx.com/trade-spot/${symLow}-usdt`
      if (market === 'futures') return `https://www.okx.com/trade-swap/${symLow}-usdt-swap`
      break
  }
}
```

> ⚠️ **Важно:** URL-формат у каждой биржи свой. Проверяйте реальную ссылку на бирже перед добавлением.

### 11.3 PricesTable.tsx — добавить колонки

Добавить в `EXCHANGE_COLUMNS` (spot и futures отдельными строками):
```typescript
const EXCHANGE_COLUMNS: Array<{ key: string; label: string; short: string }> = [
  // ... existing ...
  { key: 'spot_bybit',       label: 'Bybit Spot',   short: 'BB S' },
  { key: 'futures_bybit',    label: 'Bybit Fut',    short: 'BB F' },
  { key: 'spot_kucoin',      label: 'KuCoin Spot',  short: 'KC S' },
  { key: 'futures_kucoin',   label: 'KuCoin Fut',   short: 'KC F' },
  { key: 'spot_binance',     label: 'Binance Spot', short: 'BN S' },
  { key: 'futures_binance',  label: 'Binance Fut',  short: 'BN F' },
  { key: 'spot_bitget',      label: 'Bitget Spot',  short: 'BG S' },
  { key: 'futures_bitget',   label: 'Bitget Fut',   short: 'BG F' },
  { key: 'spot_okx',         label: 'OKX Spot',     short: 'OKX S' },
  { key: 'futures_okx',      label: 'OKX Fut',      short: 'OKX F' },
]
```

> 💡 **activeCols** автоматически скрывает колонки без данных — добавление новых колонок не сломает prod пока коллектор не запущен.

### 11.4 Также обновить: utils.py (сигнал бота)

Ссылки в Telegram-сигналах генерируются в **backend** (не webapp):

Файл: `backend/infrastructure/services/collectors/structures/utils.py`

```python
def generate_market_link(market: str, symbol: str, ..., source: str = "") -> str:
    match market:
        case "spot":
            match source:
                case "bybit":
                    base = symbol.replace("_USDT", "").replace("-USDT", "").replace("USDT", "").strip("_-")
                    return f"https://www.bybit.com/trade/spot/{base}/USDT"
                case "kucoin":
                    return f"https://www.kucoin.com/trade/{symbol}"
                # ... и т.д.
        case "futures":
            match source:
                case "bybit":
                    return f"https://www.bybit.com/trade/futures/usdt/{symbol}"
```

### 11.6 Текущие биржи в webapp (актуально на март 2026)

| Ключ | Биржа | Статус |
|------|-------|--------|
| gate | Gate.io | ✅ prod + dev |
| xt | XT.com | ✅ prod + dev |
| mexc | MEXC | ✅ prod + dev |
| bitmart | BitMart | ✅ prod + dev |
| lbank | LBank | ✅ prod + dev |
| hyperliquid | Hyperliquid | ✅ prod + dev |
| asterdex | AsterDEX | ✅ prod + dev |
| ascendex | AscendEX | ✅ prod + dev |
| bybit | Bybit | ✅ webapp, dev only |
| kucoin | KuCoin | ✅ webapp, dev only |
| binance | Binance | ✅ webapp, не запущен |
| bitget | Bitget | ✅ webapp, не запущен |
| okx | OKX | ✅ webapp, не запущен |
| gmgn | GMGN (DEX) | ✅ prod + dev |

---

## Чеклист добавления биржи

### Подготовка
- [ ] Изучить API документацию
- [ ] Проверить API вручную (curl)
- [ ] **Определить тип API** (Binance-compatible, Custom protocol, Split domains)
- [ ] Определить наличие мультипликаторов (kPEPE, 1000SHIB и т.д.)
- [ ] Определить наличие chain info в API
- [ ] **Определить формат символов** (BTCUSDT vs BTC/USDT vs BTC)

### Разработка
- [ ] Создать `token_collector/sources/{name}.py`
- [ ] **КРИТИЧНО: Добавить фильтрацию плечевых токенов** `if base.endswith(("3S", "3L", "5S", "5L", "2S", "2L")): continue`
- [ ] **КРИТИЧНО: Фильтр только USDT** `if s.get("quoteAsset") == "USDT"` (НЕ USDC!)
- [ ] Создать `price_collector/ws/{name}.py` (с правильным типом API!)
- [ ] Создать `price_collector/sources/{name}.py`
- [ ] Зарегистрировать в `token_collector/manager.py`
- [ ] Зарегистрировать в `price_collector/manager.py`
- [ ] Добавить маппинг сетей (если нужно)
- [ ] Добавить ссылки в `structures/utils.py` (generate_market_link)
- [ ] **Добавить 2 сервиса в docker-compose** (spot + futures отдельно!)
- [ ] **Обновить webapp** (см. раздел 11):
  - [ ] `utils.ts`: добавить в EXCHANGE_LABELS, EXCHANGE_COLORS, getExchangeUrl
  - [ ] `PricesTable.tsx`: добавить 2 строки в EXCHANGE_COLUMNS (spot + futures)
  - [ ] `utils.py` (backend): добавить в generate_market_link (spot + futures)
  - [ ] Задеплоить webapp: `docker compose ... up --build --force-recreate -d webapp`

### Тестирование
- [ ] Локальный тест импортов и type check
- [ ] Деплой на dev сервер **в правильном порядке** (token-collector → distributor → price-collectors)
- [ ] Проверить логи token-collector
- [ ] **КРИТИЧНО: Проверить количество токенов в БД vs API** (с учетом фильтров!)
- [ ] **КРИТИЧНО: Проверить отсутствие плечевых токенов в БД** `SELECT * FROM tokens WHERE base_asset ~ '(3S|3L|5S|5L|2S|2L)$'` → должно быть 0
- [ ] **КРИТИЧНО: Проверить только USDT пары** `SELECT DISTINCT quote_asset FROM tokens WHERE source_id=...` → только 'USDT'
- [ ] Проверить WS monitor в price-collector (**total близко к count в БД, stale <5%**)
- [ ] Проверить токены в Redis (**count близко к count в БД**)
- [ ] **Если futures: проверить fallback механизм** (см. 9.7) - должны загрузиться все futures

### Верификация мержа (КРИТИЧНО!)
- [ ] **Проверить мерж основных токенов (BTC, ETH, SOL)** - должны объединяться с 6-7 биржами
- [ ] **Проверить k-токены (kPEPE, kBONK)** - должны быть ОТДЕЛЬНЫМИ global_tokens
- [ ] **Проверить что PEPE не содержит hyperliquid** (если есть kPEPE)
- [ ] **Проверить price-based merge в логах** (`Merged...price diff`, `Rejected merge`)
- [ ] **Проверить отсутствие phantom spreads** (99000%+ спреды = ошибка!)
- [ ] **Сравнить метрики с референсными** (см. 10.7)

### Финальная проверка
- [ ] Проверить формирование сигналов в spread-collector
- [ ] Проверить ссылки на торговлю в сигналах (открыть 2-3 реальные ссылки)
- [ ] Убедиться что контейнеры не в restart loop (docker ps)
- [ ] **Проверить что в docker-compose нет дубликатов** (одна биржа = 2 контейнера максимум)

---

## 11. Быстрая диагностика (шпаргалка)

### Однострочные проверки

```bash
# === БАЗОВЫЕ ПРОВЕРКИ ===

# Количество токенов в БД по market_type
docker exec database psql -U dex-dev -d dex-dev -tAc "SELECT market_type, COUNT(*) FROM tokens WHERE source_id=(SELECT id FROM sources WHERE name='ascendex') GROUP BY market_type"

# Количество в Redis
docker exec redis redis-cli KEYS 'available_tokens:ascendex:*' | xargs -I {} docker exec redis redis-cli SCARD {}

# WS monitor последняя строка
docker logs price-collector-spot-ascendex 2>&1 | grep "WS monitor" | tail -1

# Проверка плечевых токенов (должно быть 0)
docker exec database psql -U dex-dev -d dex-dev -tAc "SELECT COUNT(*) FROM tokens WHERE source_id=(SELECT id FROM sources WHERE name='ascendex') AND base_asset ~ '(3S|3L|5S|5L|2S|2L)$'"

# Проверка только USDT (должна быть только одна строка: USDT)
docker exec database psql -U dex-dev -d dex-dev -tAc "SELECT DISTINCT quote_asset FROM tokens WHERE source_id=(SELECT id FROM sources WHERE name='ascendex')"

# === ДИАГНОСТИКА ПРОБЛЕМ ===

# Проверка что source создан
docker exec database psql -U dex-dev -d dex-dev -tAc "SELECT id, name, type FROM sources WHERE name='ascendex'"

# Последние ошибки в token-collector
docker logs token-collector 2>&1 | grep -i "error\|exception" | tail -10

# Последние ошибки в price-collector
docker logs price-collector-spot-ascendex 2>&1 | grep -i "error\|exception" | tail -10

# Проверка мержа основных токенов
docker exec database psql -U dex-dev -d dex-dev -tAc "SELECT gt.symbol, array_agg(DISTINCT s.name ORDER BY s.name) FROM global_tokens gt JOIN global_token_sources gts ON gt.id = gts.global_token_id JOIN sources s ON gts.source_id = s.id WHERE gt.symbol IN ('BTC','ETH','SOL') GROUP BY gt.symbol"

# Rejected merge candidates (последние 5)
docker logs token-collector 2>&1 | grep "Rejected merge candidate" | tail -5

# === МОНИТОРИНГ ===

# Статус всех контейнеров новой биржи
docker ps --filter "name=ascendex" --format "table {{.Names}}\t{{.Status}}"

# CPU/Memory usage
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" | grep ascendex

# Последние логи всех сервисов (параллельно)
docker logs token-collector --tail 3 & \
docker logs price_distributor --tail 3 & \
docker logs price-collector-spot-ascendex --tail 3 & \
docker logs price-collector-futures-ascendex --tail 3 & \
wait

# === SPREAD SIGNALS ===

# Проверка что биржа участвует в спредах
docker logs spread_loader 2>&1 | grep -i ascendex | tail -10

# Количество сигналов с участием биржи (за последние 100 строк)
docker logs spread_loader 2>&1 | tail -100 | grep -c ascendex
```

### Типичные сценарии

**Сценарий 1: Токены не загружаются в Redis**
```bash
# 1. Проверить есть ли в БД
docker exec database psql -U dex-dev -d dex-dev -tAc "SELECT COUNT(*) FROM tokens WHERE source_id=(SELECT id FROM sources WHERE name='ascendex')"

# 2. Перезапустить distributor
docker restart price_distributor

# 3. Ждем 10 секунд
sleep 10

# 4. Проверить Redis
docker exec redis redis-cli SCARD 'available_tokens:ascendex:*:SPOT'

# 5. Если все еще 0 - смотреть логи distributor
docker logs price_distributor --tail 50 | grep ascendex
```

**Сценарий 2: WS monitor показывает total=0**
```bash
# 1. Проверить Redis
docker exec redis redis-cli KEYS 'available_tokens:ascendex:*'

# 2. Если ключей нет - запустить distributor
docker restart price_distributor && sleep 10

# 3. Перезапустить price-collector
docker restart price-collector-spot-ascendex

# 4. Ждем 30 секунд и проверяем
sleep 30
docker logs price-collector-spot-ascendex 2>&1 | grep "WS monitor" | tail -1
```

**Сценарий 3: Futures не активируются (как в 9.8)**
```bash
# 1. Проверить futures в БД
DB_COUNT=$(docker exec database psql -U dex-dev -d dex-dev -tAc "SELECT COUNT(*) FROM tokens WHERE source_id=(SELECT id FROM sources WHERE name='ascendex') AND market_type='FUTURES'")

# 2. Проверить futures в Redis
REDIS_COUNT=$(docker exec redis redis-cli SCARD 'available_tokens:ascendex:*:FUTURES')

# 3. Сравнить
echo "DB: $DB_COUNT, Redis: $REDIS_COUNT"

# 4. Если разница >20% - проблема с distributor fallback (см. 9.8)
# Проверить что код из 9.8 добавлен в distributor.py
```

**Сценарий 4: Phantom spreads (99000%+)**
```bash
# 1. Найти проблемные токены в spread signals
docker logs spread_loader 2>&1 | grep -E "[0-9]{5,}%" | tail -10

# 2. Проверить мультипликаторы (kPEPE не должен мержиться с PEPE)
docker exec database psql -U dex-dev -d dex-dev -c "SELECT gt.symbol, array_agg(s.name) FROM global_tokens gt JOIN global_token_sources gts ON gt.id=gts.global_token_id JOIN sources s ON gts.source_id=s.id WHERE gt.symbol IN ('PEPE','kPEPE') GROUP BY gt.symbol"

# 3. Проверить что плечевые токены отфильтрованы
docker exec database psql -U dex-dev -d dex-dev -tAc "SELECT base_asset FROM tokens WHERE source_id=(SELECT id FROM sources WHERE name='ascendex') AND base_asset ~ '(3S|3L|5S|5L|2S|2L)$' LIMIT 5"
# Должно быть пусто!
```

---

## Ссылки на примеры в коде

- [BaseTokenParser](../backend/infrastructure/services/collectors/token_collector/base.py) - Базовый класс для token collectors
- [BaseWSCexPriceParser](../backend/infrastructure/services/collectors/price_collector/base.py) - Базовый класс для price collectors
- [ChainNormalizer](../backend/infrastructure/services/collectors/structures/normalizer.py) - Нормализация названий сетей
- [Distributor](../backend/infrastructure/services/collectors/price_collector/distributor.py) - Загрузка токенов в Redis (см. fallback механизм в строке 64-71)

**Примеры реализаций:**
- [Gate.io](../backend/infrastructure/services/collectors/token_collector/sources/gate.py) - CEX с chain info, Binance-compatible API
- [Asterdex](../backend/infrastructure/services/collectors/token_collector/sources/asterdex.py) - CEX с chain info, разделенные домены (spot/futures)
- [AscendEX](../backend/infrastructure/services/collectors/token_collector/sources/ascendex.py) - CEX с chain info, custom protocol (не Binance)
- [Hyperliquid](../backend/infrastructure/services/collectors/token_collector/sources/hyperliquid.py) - DEX без chain info, POST API, мультипликаторы (kPEPE)

**WebSocket примеры:**
- [Asterdex WS](../backend/infrastructure/services/collectors/price_collector/ws/asterdex.py) - Binance-style, разделенные URL
- [AscendEX WS](../backend/infrastructure/services/collectors/price_collector/ws/ascendex.py) - Custom protocol, единый URL, manual ping/pong
- [Hyperliquid WS](../backend/infrastructure/services/collectors/price_collector/ws/hyperliquid.py) - Custom subscription format, только название монеты

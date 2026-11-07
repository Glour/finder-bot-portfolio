# Price Collector

## Назначение

Сервис для сбора orderbooks (книг ордеров) с криптовалютных бирж в реальном времени. Каждый инстанс отвечает за одну биржу и один тип рынка (spot или futures).

## Архитектура

```
┌──────────────────┐
│ Price Collector  │
└────────┬─────────┘
         │
         ├─→ Читает available_tokens из Redis
         │
         ├─→ Запрашивает orderbooks с биржи (API)
         │
         ├─→ Сохраняет в Redis:
         │   • prices:{spot|futures}:{symbol}
         │   • orderbook:{gtid}:{gts_id}:{token_id}:{MARKET_TYPE}
         │
         └─→ Непрерывный цикл (без задержек)
```

## Инстансы

### Docker Compose конфигурация
```yaml
# 8 инстансов = 4 биржи × 2 типа рынка

price-collector-spot-mexc:
  environment:
    MARKET_TYPE: spot
    SOURCE_NAME: mexc

price-collector-futures-mexc:
  environment:
    MARKET_TYPE: futures
    SOURCE_NAME: mexc

price-collector-spot-gate:
  environment:
    MARKET_TYPE: spot
    SOURCE_NAME: gate

price-collector-futures-gate:
  environment:
    MARKET_TYPE: futures
    SOURCE_NAME: gate

# И так далее для xt, lbank
```

## Основные файлы

- **Entry point**: `infrastructure/services/collectors/price_collector/main.py`
- **Manager**: `infrastructure/services/collectors/price_collector/manager.py`
- **Collectors**: `infrastructure/services/collectors/price_collector/collectors/`
  - `mexc.py` - MEXC exchange
  - `gate.py` - Gate.io exchange
  - `xt.py` - XT exchange
  - `lbank.py` - LBank exchange

## Флоу работы

### 1. Инициализация

```python
# main.py
market_type = os.getenv("MARKET_TYPE", "spot").upper()  # SPOT или FUTURES
source_name = os.getenv("SOURCE_NAME")  # mexc, gate, xt, lbank

# Проверка blacklist
source = await repo.sources.get_by_name_and_type(source_name, SourceType.CEX)
is_muted = await repo.exchange_blacklist.is_muted(source.id, market_type)

if is_muted:
    logger.info(f"Source {source_name} ({market_type}) is muted. Exiting.")
    return

price_collector = get_price_collector_manager(
    repo=repo,
    cache=redis_cache,
    market_type=market_type,
    source_name=source_name
)

await price_collector.run()
```

### 2. Основной цикл сбора

```
Бесконечный цикл (без sleep):

1. Получить список токенов из Redis
   ↓
   redis_key = f"available_tokens:{source_name}:{source_id}:{market_type}"
   tokens = await redis.smembers(redis_key)
   ↓
   Десериализовать JSON токены

2. Разделить токены на батчи (по 50-100)
   ↓
   Параллельно для каждого батча:

3. Для каждого токена в батче:
   ↓
   a. Запросить orderbook с биржи через API:
      - GET /api/v3/depth?symbol=BTCUSDT&limit=20 (MEXC)
      - GET /api/v4/spot/order_book?currency_pair=BTC_USDT (Gate)
   ↓
   b. Нормализовать orderbook:
      {
        "bids": [[price, amount], ...],  # Заявки на покупку
        "asks": [[price, amount], ...]   # Заявки на продажу
      }
   ↓
   c. Сериализовать в msgpack:
      payload = {
        "symbol": "BTC",
        "market_type": "SPOT",
        "token_id": "uuid",
        "global_token_id": "uuid",
        "global_source_id": "uuid",
        "orderbook": {...},
        "get_at": timestamp_ms
      }
   ↓
   d. Сохранить в Redis (2 ключа):

      # Для Price Saver (сохранение в БД)
      SET prices:spot:BTC msgpack(payload)

      # Для Spread Loader (анализ арбитража)
      SET orderbook:{gtid}:{gts_id}:{token_id}:SPOT msgpack(payload)

4. Логировать статистику
   ↓
   [INFO] Collected 1523 prices in 45.23s
```

### 3. Структура Orderbook

**API Response (пример MEXC):**
```json
{
  "bids": [
    ["43250.50", "0.125"],  // [цена, объем]
    ["43250.00", "0.450"],
    ["43249.50", "1.200"]
  ],
  "asks": [
    ["43251.00", "0.300"],
    ["43251.50", "0.850"],
    ["43252.00", "2.100"]
  ]
}
```

**Нормализованный формат:**
```python
{
  "bids": [
    [43250.50, 0.125],
    [43250.00, 0.450],
    [43249.50, 1.200]
  ],
  "asks": [
    [43251.00, 0.300],
    [43251.50, 0.850],
    [43252.00, 2.100]
  ]
}
```

**Сохраняется в Redis (msgpack):**
```python
{
  "symbol": "BTC",
  "market_type": "SPOT",
  "token_id": "54d09abe-88cb-46f2-858d-e50bf9f8b96f",
  "global_token_id": "ffd7573c-eb73-4b9f-896a-00107cdcdcbe",
  "global_source_id": "1c0b5751-bd74-4001-8671-e507edab1b3d",
  "orderbook": {
    "bids": [[43250.50, 0.125], ...],
    "asks": [[43251.00, 0.300], ...]
  },
  "get_at": 1706437825340  # timestamp в миллисекундах
}
```

## Особенности реализации по биржам

### MEXC
- **API**: REST API v3
- **Endpoint**: `/api/v3/depth`
- **Rate Limit**: 20 req/sec
- **Особенности**:
  - Поддержка spot и futures
  - Глубина orderbook: 5, 10, 20, 50, 100, 500, 1000
  - Требуется API key для высоких лимитов

### Gate.io
- **API**: REST API v4
- **Endpoints**:
  - Spot: `/api/v4/spot/order_book`
  - Futures: `/api/v4/futures/{settle}/order_book`
- **Rate Limit**: 300 req/10sec
- **Особенности**:
  - Разные эндпоинты для spot/futures
  - Глубина: 5, 10, 20, 50, 100
  - Futures требует указания settle (usdt, btc)

### XT
- **API**: REST API v4
- **Endpoint**: `/v4/public/{market_type}/depth`
- **Rate Limit**: 100 req/min
- **Особенности**:
  - Единый эндпоинт для spot/futures
  - Глубина: 20, 50, 100
  - Не требует auth

### LBank
- **API**: REST API v2
- **Endpoint**: `/v2/depth.do`
- **Rate Limit**: 60 req/min
- **Особенности**:
  - Только spot market
  - Глубина: 60
  - Старый API, менее стабильный

## Конфигурация

### Docker Compose
```yaml
price-collector-spot-mexc:
  build:
    context: ..
    dockerfile: backend/api/Dockerfile
  command: python infrastructure/services/collectors/price_collector/main.py
  volumes:
    - ./data/proxies.txt:/app/data/proxies.txt
  env_file:
    - .env.dev
  environment:
    PYTHONPATH: /app
    MARKET_TYPE: spot
    SOURCE_NAME: mexc
  depends_on:
    - redis
    - postgres
    - bot
    - price_distributor
  restart: unless-stopped
```

### Environment Variables
```bash
# Обязательные
MARKET_TYPE=spot           # spot или futures
SOURCE_NAME=mexc           # mexc, gate, xt, lbank

# PostgreSQL
DB_HOST=postgres
DB_PORT=5432

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# API Keys (опционально, для высоких лимитов)
AUTH__MEXC_ACCESS_KEY=mx0vglQqiAsada6nre
AUTH__MEXC_SECRET_KEY=5d7d9fa76c474c81a62e61d97c847872

# Proxies
# Файл data/proxies.txt:
# http://user:pass@host:port
# http://user:pass@host2:port
```

## Логи

### Успешный сбор
```
[INFO] Price collector started
[INFO] Collected 1523 prices in 45.23s
[INFO] Batch 1/15: Collected 100 orderbooks
[INFO] Batch 2/15: Collected 100 orderbooks
```

### Ошибки
```
[ERROR] Failed to fetch orderbook for BTC from MEXC: Rate limit exceeded
[WARNING] Empty orderbook for ETH, skipping
[ERROR] Connection timeout for symbol PEPE
```

### Мониторинг производительности
```
[INFO] Performance stats:
  Total tokens: 1523
  Successful: 1498
  Failed: 25
  Duration: 45.23s
  Rate: 33.1 tokens/sec
```

## Мониторинг

### Redis проверка
```bash
# Количество orderbooks
redis-cli KEYS "orderbook:*" | wc -l

# Пример orderbook
redis-cli --raw GET "orderbook:ffd7573c-eb73-4b9f-896a-00107cdcdcbe:1c0b5751-bd74-4001-8671-e507edab1b3d:54d09abe-88cb-46f2-858d-e50bf9f8b96f:SPOT" | xxd

# Проверка prices keys
redis-cli KEYS "prices:spot:*" | head -10
redis-cli KEYS "prices:futures:*" | head -10

# TTL проверка (обычно без TTL)
redis-cli TTL "orderbook:..."
```

### Docker логи
```bash
# Реал-тайм логи
docker logs -f price-collector-spot-mexc

# Последние 100 строк
docker logs --tail 100 price-collector-spot-mexc

# Ошибки
docker logs price-collector-spot-mexc 2>&1 | grep ERROR

# Статистика
docker logs price-collector-spot-mexc 2>&1 | grep "Collected.*prices"
```

## Производительность

### Метрики
- **Токенов за цикл**: 500-2000
- **Время цикла**: 30-60 секунд
- **Rate**: 25-50 токенов/сек
- **Memory**: 200-500 MB на инстанс
- **CPU**: 10-30% на инстанс

### Bottlenecks
1. **API Rate Limits** - основное ограничение
2. **Network latency** - задержки запросов
3. **Serialization** - msgpack encoding

### Оптимизации
- **Batch requests**: Где поддерживается биржей
- **Parallel batches**: asyncio.gather для батчей
- **HTTP connection pooling**: Переиспользование соединений
- **Msgpack serialization**: Быстрее JSON
- **Proxies rotation**: Обход rate limits

## Типичные проблемы

### Проблема: Rate limit exceeded
**Причина**: Слишком много запросов к API биржи

**Решение**:
```python
# Добавить задержку между батчами
await asyncio.sleep(0.5)

# Уменьшить размер батча
batch_size = 50  # вместо 100

# Использовать API keys для высших лимитов
```

### Проблема: Empty orderbooks
**Причина**: Токен делистнут или низкая ликвидность

**Решение**:
```python
# Пропускать токены с пустыми orderbooks
if not orderbook or (not orderbook.get("bids") and not orderbook.get("asks")):
    logger.warning(f"Empty orderbook for {symbol}, skipping")
    continue
```

### Проблема: Медленный сбор
**Причина**: Много токенов или медленная биржа

**Решение**:
- Увеличить количество параллельных батчей
- Использовать прокси для распределения нагрузки
- Фильтровать токены с низкой ликвидностью

### Проблема: Ошибки сериализации
**Причина**: Некорректные данные от биржи (NaN, Infinity)

**Решение**:
```python
# Валидация перед сериализацией
def validate_orderbook(orderbook):
    for side in ["bids", "asks"]:
        for price, amount in orderbook.get(side, []):
            if not isinstance(price, (int, float)) or price <= 0:
                return False
            if not isinstance(amount, (int, float)) or amount <= 0:
                return False
    return True
```

## Интеграция с другими сервисами

### Price Distributor → Price Collector
```
Price Distributor публикует available_tokens в Redis
    ↓
Price Collector читает эти Sets
    ↓
Запрашивает orderbooks для токенов
```

### Price Collector → Price Saver
```
Price Collector сохраняет prices:{market}:{symbol} в Redis
    ↓
Price Saver читает эти ключи каждые 60 сек
    ↓
Сохраняет в PostgreSQL (token_prices таблица)
```

### Price Collector → Spread Loader
```
Price Collector сохраняет orderbook:{...} в Redis
    ↓
Spread Loader читает orderbooks
    ↓
Детектирует изменения и публикует в stream
```

## Безопасность

### API Keys
```bash
# Хранить в .env.dev, не коммитить!
AUTH__MEXC_ACCESS_KEY=...
AUTH__MEXC_SECRET_KEY=...
```

### Rate Limiting
```python
# Респект к лимитам биржи
MAX_REQUESTS_PER_SECOND = 10
await rate_limiter.acquire()
```

### Error Handling
```python
try:
    orderbook = await exchange_api.get_orderbook(symbol)
except RateLimitError:
    await asyncio.sleep(60)  # Wait 1 minute
except ConnectionError:
    logger.error(f"Connection failed for {symbol}")
    continue
```

## Расширение функционала

### Добавление новой биржи
1. Создать collector в `collectors/new_exchange.py`
2. Реализовать методы:
   - `get_orderbook(symbol, market_type)`
   - `normalize_orderbook(raw_data)`
3. Добавить в `manager.py`
4. Добавить docker-compose сервисы

### Поддержка WebSocket
```python
# Для real-time orderbook updates
async def stream_orderbooks(symbols):
    async with websocket.connect(ws_url) as ws:
        await ws.send(json.dumps({"subscribe": symbols}))
        async for message in ws:
            orderbook = parse_ws_message(message)
            await save_to_redis(orderbook)
```

### Кэширование ответов
```python
# Избегать повторных запросов для одного токена
orderbook_cache = TTLCache(maxsize=1000, ttl=5)

cached = orderbook_cache.get(symbol)
if cached:
    return cached

orderbook = await fetch_from_api(symbol)
orderbook_cache[symbol] = orderbook
return orderbook
```

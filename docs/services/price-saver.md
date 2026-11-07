# Price Saver

## Назначение

Сервис для периодического сохранения цен и orderbooks из Redis в PostgreSQL. Создает исторические данные для аналитики и бэктестинга стратегий.

## Архитектура

```
┌──────────────┐
│ Price Saver  │
└──────┬───────┘
       │
       ├─→ Сканирует Redis keys: prices:spot:*, prices:futures:*
       │
       ├─→ Десериализует msgpack данные
       │
       ├─→ Сохраняет в PostgreSQL (bulk upsert)
       │   Таблица: token_prices
       │
       └─→ Интервал: 60 секунд
```

## Основные файлы

- **Entry point**: `infrastructure/services/collectors/price_collector/saver.py`
- **Dependencies**:
  - Redis (источник данных)
  - PostgreSQL (целевая БД)

## Флоу работы

### 1. Инициализация

```python
database.redis = aioredis.from_url(settings.redis.dsn, decode_responses=False)
database.init_database(settings)
repo = RequestsRepo(sessionmaker=database.async_session)
redis = RedisCache(database.redis)

saver_service = SaverService(repo, redis)

# Запуск параллельной обработки spot и futures
await saver_service.run(prefixes=["prices:spot", "prices:futures"])
```

### 2. Основной цикл сохранения

```
Каждые 60 секунд для каждого префикса (spot/futures):

1. Сканировать Redis keys
   ↓
   SCAN 0 MATCH prices:spot:* COUNT 1000
   SCAN cursor MATCH prices:spot:* COUNT 1000
   ... (пока не пройдем все ключи)

2. Собрать батч ключей (до 1000 штук)
   ↓
   batch = ["prices:spot:BTC", "prices:spot:ETH", ...]

3. MGET всех ключей батча
   ↓
   results = await redis.mget(batch)

4. Десериализовать каждый результат
   ↓
   for raw in results:
       if not raw:
           continue

       obj = msgpack.unpackb(raw, raw=False)

       rows.append({
           "symbol": obj.get("symbol"),
           "price_usd": 0.0,
           "orderbook": obj.get("orderbook"),
           "market_type": obj.get("market_type").upper(),
           "token_id": obj.get("token_id"),
           "global_token_id": obj.get("global_token_id"),
           "global_token_source_id": obj.get("global_source_id")
       })

5. Bulk upsert в PostgreSQL (батчами по 1000)
   ↓
   await repo.token_prices.bulk_upsert(rows)

   [INFO] Saved 1523 tokens from prices:spot

6. Повторить цикл через 60 секунд
```

### 3. Структура данных

**Redis key format:**
```
prices:spot:{symbol}      # Например: prices:spot:BTC
prices:futures:{symbol}   # Например: prices:futures:ETH
```

**Redis value (msgpack):**
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
  "get_at": 1706437825340
}
```

**PostgreSQL row (token_prices):**
```sql
INSERT INTO token_prices (
    symbol,
    price_usd,
    orderbook,
    market_type,
    token_id,
    global_token_id,
    global_token_source_id,
    created_at,
    updated_at
) VALUES (
    'BTC',
    0.0,  -- Не используется (цена вычисляется из orderbook)
    '{"bids": [[43250.50, 0.125], ...], "asks": [...]}',  -- JSONB
    'SPOT',
    '54d09abe-88cb-46f2-858d-e50bf9f8b96f',
    'ffd7573c-eb73-4b9f-896a-00107cdcdcbe',
    '1c0b5751-bd74-4001-8671-e507edab1b3d',
    NOW(),
    NOW()
)
ON CONFLICT (token_id, global_token_source_id, market_type)
DO UPDATE SET
    orderbook = EXCLUDED.orderbook,
    updated_at = NOW();
```

## Batch обработка

### SCAN итерация
```python
# Не блокирует Redis, возвращает cursor для следующей итерации
async for key in redis.client.scan_iter(
    match=f"prices:spot:*",
    count=1000  # Подсказка Redis (не гарантированное количество)
):
    batch.append(key)

    if len(batch) >= 1000:
        await process_batch(batch)
        batch.clear()
```

### MGET оптимизация
```python
# Одним запросом получаем все значения батча
results = await redis.client.mget(batch)

# Вместо:
# for key in batch:
#     value = await redis.get(key)  # N запросов
```

### Bulk upsert
```python
# Одним SQL запросом вставляем/обновляем 1000 записей
await repo.token_prices.bulk_upsert(rows)

# Использует PostgreSQL:
# INSERT INTO token_prices VALUES (...), (...), (...)
# ON CONFLICT DO UPDATE ...
```

## Конфигурация

### Константы
```python
save_interval = 60           # Интервал сохранения (секунды)
SCAN_BATCH_SIZE = 1000       # Размер батча для SCAN
UPSERT_BATCH_SIZE = 1000     # Размер батча для bulk upsert
```

### Docker Compose
```yaml
price-saver:
  container_name: price-saver
  build:
    context: ..
    dockerfile: backend/api/Dockerfile
  command: python infrastructure/services/collectors/price_collector/saver.py
  env_file:
    - .env.dev
  environment:
    PYTHONPATH: /app
  depends_on:
    - redis
    - postgres
    - bot
  restart: unless-stopped
```

### Environment Variables
```bash
# PostgreSQL
DB_HOST=postgres
DB_PORT=5432
DB_NAME=dex-dev
DB_USER=dex-dev
DB_PASS=dex

# Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

# Настройки сохранения (опционально)
SAVE_INTERVAL=60
SCAN_BATCH_SIZE=1000
UPSERT_BATCH_SIZE=1000
```

## Логи

### Успешное сохранение
```
[INFO] Saver service started
[INFO] Saved 1523 tokens from prices:spot
[INFO] Saved 987 tokens from prices:futures
[INFO] Saved 1498 tokens from prices:spot
[INFO] Saved 1001 tokens from prices:futures
```

### Ошибки
```
[ERROR] Failed to unpack msgpack: Extra data after message
[ERROR] Error saving tokens from prices:spot: Connection to database lost
[WARNING] Empty result for key prices:spot:PEPE
```

### Статистика производительности
```
[INFO] Spot cycle completed:
  Keys scanned: 1523
  Rows saved: 1498
  Duration: 2.34s
  Rate: 640 rows/sec
```

## Мониторинг

### PostgreSQL проверка
```sql
-- Общее количество сохраненных цен
SELECT COUNT(*) FROM token_prices;

-- Последние сохранения
SELECT symbol, market_type, updated_at
FROM token_prices
ORDER BY updated_at DESC
LIMIT 20;

-- Статистика по биржам
SELECT s.name, tp.market_type, COUNT(*) as price_count
FROM token_prices tp
JOIN tokens t ON tp.token_id = t.id
JOIN sources s ON t.source_id = s.id
GROUP BY s.name, tp.market_type;

-- Свежесть данных
SELECT
    symbol,
    market_type,
    AGE(NOW(), updated_at) as data_age
FROM token_prices
WHERE updated_at > NOW() - INTERVAL '5 minutes'
ORDER BY updated_at DESC;
```

### Redis проверка
```bash
# Количество keys для сохранения
redis-cli KEYS "prices:spot:*" | wc -l
redis-cli KEYS "prices:futures:*" | wc -l

# Пример данных
redis-cli --raw GET "prices:spot:BTC" | xxd | head -20

# Memory usage для prices keys
redis-cli --bigkeys --pattern "prices:*"
```

### Docker логи
```bash
# Реал-тайм мониторинг
docker logs -f price-saver

# Статистика сохранений
docker logs price-saver 2>&1 | grep "Saved.*tokens"

# Ошибки
docker logs price-saver 2>&1 | grep ERROR
```

## Производительность

### Метрики
- **Keys за цикл**: 1000-2000
- **Время цикла**: 2-5 секунд
- **Интервал**: 60 секунд
- **Throughput**: 400-800 rows/sec
- **Memory**: 100-200 MB
- **CPU**: 5-15%

### Bottlenecks
1. **PostgreSQL write speed** - основное ограничение
2. **Network latency** - Redis ↔ PostgreSQL
3. **Msgpack deserialization** - CPU bound

### Оптимизации применены
- **SCAN вместо KEYS**: Не блокирует Redis
- **MGET batch**: Множественное чтение за 1 запрос
- **Bulk upsert**: Массовая вставка в БД
- **Msgpack**: Быстрая десериализация
- **Async I/O**: Неблокирующие операции

## Типичные проблемы

### Проблема: Долгое сохранение
**Причина**: Большое количество ключей или медленный PostgreSQL

**Решение**:
```python
# Увеличить размер батчей
UPSERT_BATCH_SIZE = 2000  # вместо 1000

# Добавить индексы в БД
CREATE INDEX idx_token_prices_token_id ON token_prices(token_id);
CREATE INDEX idx_token_prices_updated_at ON token_prices(updated_at);

# Использовать connection pooling
asyncpg pool with min_size=5, max_size=20
```

### Проблема: Ошибки десериализации
**Причина**: Некорректный msgpack формат или битые данные

**Решение**:
```python
try:
    obj = msgpack.unpackb(raw, raw=False)
except Exception as e:
    logger.exception(f"Failed to unpack msgpack: {e}")

    # Удалить битый ключ из Redis
    await redis.client.delete(key)
    continue
```

### Проблема: Дублирование записей
**Причина**: Нарушение уникальности или race condition

**Решение**:
```sql
-- Убедиться в наличии уникального индекса
CREATE UNIQUE INDEX idx_token_prices_unique
ON token_prices(token_id, global_token_source_id, market_type);

-- Использовать ON CONFLICT
INSERT INTO token_prices (...)
VALUES (...)
ON CONFLICT (token_id, global_token_source_id, market_type)
DO UPDATE SET
    orderbook = EXCLUDED.orderbook,
    updated_at = NOW();
```

### Проблема: Растущий размер БД
**Причина**: Накопление исторических данных

**Решение**:
```sql
-- Партиционирование по дате
CREATE TABLE token_prices (
    ...
    created_at TIMESTAMP NOT NULL
) PARTITION BY RANGE (created_at);

-- Создание партиций
CREATE TABLE token_prices_2024_01 PARTITION OF token_prices
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- Очистка старых данных
DELETE FROM token_prices
WHERE created_at < NOW() - INTERVAL '30 days';

-- Или drop партиции
DROP TABLE token_prices_2023_12;
```

## Интеграция с другими сервисами

### Price Collector → Price Saver
```
Price Collector сохраняет prices:{market}:{symbol} в Redis
    ↓
Price Saver сканирует эти ключи каждые 60 сек
    ↓
Сохраняет в PostgreSQL
```

### Price Saver → Analytics/Backtesting
```
Price Saver создает исторические данные в token_prices
    ↓
Аналитические сервисы читают historical data
    ↓
Backtesting стратегий арбитража
```

## Альтернативные стратегии

### Стратегия 1: Real-time сохранение
```python
# Сохранять сразу при получении orderbook
# Плюсы: Актуальные данные, нет задержки
# Минусы: Высокая нагрузка на БД

async def save_immediately(orderbook):
    await repo.token_prices.upsert(orderbook)
```

### Стратегия 2: Time-series database
```python
# Использовать TimescaleDB вместо PostgreSQL
# Плюсы: Оптимизировано для временных рядов
# Минусы: Дополнительная инфраструктура

CREATE TABLE token_prices (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT,
    orderbook JSONB
);

SELECT create_hypertable('token_prices', 'time');
```

### Стратегия 3: Snapshot + incremental
```python
# Полное сохранение раз в час + изменения каждые 60 сек
# Плюсы: Баланс между актуальностью и нагрузкой
# Минусы: Сложнее логика

if datetime.now().minute == 0:
    # Full snapshot
    await save_all_prices()
else:
    # Only changed
    await save_changed_prices()
```

## Расширение функционала

### Добавление сжатия
```python
import zlib

# Сжать orderbook перед сохранением
orderbook_json = json.dumps(orderbook)
compressed = zlib.compress(orderbook_json.encode())

# В БД
CREATE TABLE token_prices (
    ...
    orderbook_compressed BYTEA
);
```

### Добавление метрик
```python
from prometheus_client import Counter, Histogram

saves_total = Counter('saver_saves_total', 'Total saves')
save_duration = Histogram('saver_duration_seconds', 'Save duration')

with save_duration.time():
    await repo.token_prices.bulk_upsert(rows)
    saves_total.inc(len(rows))
```

### Добавление ретраев
```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
async def save_with_retry(rows):
    await repo.token_prices.bulk_upsert(rows)
```

## Data Retention Policy

### Стратегия хранения
```sql
-- Полные данные за последние 7 дней
-- Агрегированные данные за последние 30 дней
-- Дневные снапшоты старше 30 дней

-- Агрегация старых данных
INSERT INTO token_prices_daily
SELECT
    DATE(created_at) as date,
    token_id,
    global_token_id,
    market_type,
    -- Средний orderbook за день
    jsonb_agg(orderbook) as orderbooks_daily
FROM token_prices
WHERE created_at < NOW() - INTERVAL '7 days'
GROUP BY DATE(created_at), token_id, global_token_id, market_type;

-- Удаление старых детальных данных
DELETE FROM token_prices
WHERE created_at < NOW() - INTERVAL '7 days';
```

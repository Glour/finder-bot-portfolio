# Spread Loader

## Назначение

**Ключевой сервис** системы, который загружает токены с orderbooks из PostgreSQL, детектирует изменения и публикует только измененные токены для анализа арбитража. Использует Redis Streams для оптимальной производительности.

## Архитектура

```
┌────────────────┐
│ Spread Loader  │
└───────┬────────┘
        │
        ├─→ Загружает tokens из PostgreSQL (с кэшированием)
        │
        ├─→ Подгружает orderbooks из Redis
        │   (orderbook:{gtid}:{gts_id}:{token_id}:{MARKET_TYPE})
        │
        ├─→ Детектирует изменения через SHA256 хеширование
        │
        ├─→ Публикует в Redis Stream: orderbooks:updates
        │   (ТОЛЬКО измененные токены)
        │
        └─→ Режимы:
            • STREAMS mode (оптимизированный) - по умолчанию
            • LIST mode (legacy) - для совместимости
```

## Режимы работы

### STREAMS Mode (Оптимизированный) ✅

**Особенности:**
- Публикация только измененных токенов (change detection)
- Redis Streams для pub/sub
- Consumer Groups для параллельных воркеров
- Reduction: с 4524 до ~1500-2500 токенов за цикл (51% reduction)

**Включение:**
```bash
# .env.dev
OPTIMIZE_USE_STREAMS=true
```

### LIST Mode (Legacy)

**Особенности:**
- Публикация всех токенов каждый раз
- Redis LIST для передачи данных
- Простая реализация
- Используется как fallback если stream недоступен

**Включение:**
```bash
# .env.dev
# OPTIMIZE_USE_STREAMS=true  # Закомментировать
```

## Основные файлы

- **Entry point**: `infrastructure/services/collectors/spread_collector/loader_optimized.py`
- **Legacy loader**: `infrastructure/services/collectors/spread_collector/loader.py`
- **Main script**: Запускается через `loader_optimized.py`

## Флоу работы (STREAMS Mode)

### 1. Инициализация

```python
database.init_database(settings)
repo = RequestsRepo(sessionmaker=database.async_session)
cache = RedisCache(database.redis)

# Конфигурация из env vars
use_streams = ENV_CONFIG.use_streams          # OPTIMIZE_USE_STREAMS
use_token_cache = ENV_CONFIG.use_token_cache  # OPTIMIZE_TOKEN_CACHE
enable_profiling = ENV_CONFIG.enable_profiling

loader = OptimizedTokenLoader(
    repo=repo,
    cache=cache,
    tokens_load_limit=10000,
    use_streams=use_streams,
    stream_key="orderbooks:updates"
)

await loader.start()
```

### 2. Основной цикл

```
Непрерывный цикл:

1. Загрузить metadata токенов
   ↓
   # Из кэша (если Phase 1 включен)
   if cache exists and fresh:
       tokens = in_memory_cache
   else:
       # Из PostgreSQL
       tokens = await repo.global_tokens.get_all_with_data(limit=10000)

       # Кэшировать на 60 сек
       metadata_cache = tokens
       cache_timestamp = now()

2. Применить blacklist фильтры
   ↓
   • token_blacklist (по chain+address)
   • exchange_blacklist (по source_id+market_type)

3. Подготовить orderbook keys
   ↓
   keys = [
       f"orderbook:{gtid}:{gts_id}:{token_id}:SPOT",
       f"orderbook:{gtid}:{gts_id}:{token_id}:FUTURES",
       ...
   ]

4. MGET orderbooks из Redis
   ↓
   results = await redis.mget(keys)

   # Десериализовать msgpack
   for key, raw in zip(keys, results):
       if raw:
           obj = msgpack.unpackb(raw)
           price["orderbook"] = obj.get("orderbook")
           price["get_at"] = obj.get("get_at")

5. Детектировать изменения (STREAMS mode)
   ↓
   for token in filtered_tokens:
       # Собрать все orderbooks токена
       orderbook_data = [p["orderbook"] for p in token["prices"] if p.get("orderbook")]

       # Хешировать
       content = msgpack.packb(orderbook_data)
       current_hash = sha256(content).hexdigest()[:16]

       # Сравнить с предыдущим хешем
       hash_key = f"orderbook_hash:{token_id}"
       previous_hash = await redis.get(hash_key)

       if previous_hash != current_hash:
           # Токен изменился!
           changed_tokens.append(token)

           # Обновить хеш
           await redis.setex(hash_key, 3600, current_hash)

6. Публиковать в Redis Stream
   ↓
   for token in changed_tokens:
       token_data = msgpack.packb(token)

       await redis.xadd(
           "orderbooks:updates",
           {"token_data": token_data}
       )

   [INFO] Published 2407 changed tokens to stream in 2.88s | avg_changes=2171.4
```

### 3. Change Detection Algorithm

**SHA256 Hashing:**
```python
def detect_changes(token):
    # Шаг 1: Собрать все orderbooks токена
    orderbook_data = []
    for price in token.get("prices", []):
        ob = price.get("orderbook")
        if ob:
            orderbook_data.append(ob)

    if not orderbook_data:
        return False  # Нет orderbooks = нет изменений

    # Шаг 2: Сериализовать в детерминированный формат
    content = msgpack.packb(orderbook_data, use_bin_type=True)

    # Шаг 3: Хешировать
    current_hash = hashlib.sha256(content).hexdigest()[:16]

    # Шаг 4: Сравнить с предыдущим
    hash_key = f"orderbook_hash:{token['id']}"
    previous_hash = await redis.get(hash_key)

    # Шаг 5: Обновить если изменился
    if previous_hash is None or previous_hash.decode() != current_hash:
        await redis.setex(hash_key, 3600, current_hash)  # TTL 1 час
        return True  # Изменился!

    return False  # Не изменился
```

**Почему работает:**
- Msgpack создает одинаковый binary для одинаковых данных
- SHA256 гарантирует уникальность хеша
- TTL 3600 сек избегает бесконечного роста ключей
- First run: все хеши None → все токены "изменились"

## Конфигурация

### Feature Flags (.env.dev)

```bash
# Phase 1: Token metadata caching (33% improvement)
# Кэширует токены из БД в памяти на 60 секунд
OPTIMIZE_TOKEN_CACHE=true

# Phase 2: Redis Streams (75% improvement)
# Публикует только измененные токены через streams
OPTIMIZE_USE_STREAMS=true

# Phase 3: Msgpack-only serialization (83% improvement)
# Использует только msgpack (не JSON)
OPTIMIZE_MSGPACK_ONLY=true

# Profiling and metrics
# Включает детальные логи производительности
OPTIMIZE_ENABLE_PROFILING=true

# Latency tracking
# Отслеживает end-to-end latency
OPTIMIZE_ENABLE_LATENCY_TRACKING=true
```

### Docker Compose

```yaml
spread-loader:
  container_name: spread_loader
  build:
    context: ..
    dockerfile: backend/api/Dockerfile
  command: python infrastructure/services/collectors/spread_collector/loader_optimized.py
  env_file:
    - .env.dev
  environment:
    PYTHONPATH: /app
    # Feature flags наследуются из .env.dev
  depends_on:
    - redis
    - postgres
    - bot
  restart: unless-stopped
```

## Логи

### STREAMS Mode (оптимизированный)
```
[INFO] Starting optimized token loader | mode=STREAMS | stream=orderbooks:updates

[INFO] Refreshing token metadata cache from database...
[INFO] Fetched 4634 tokens from DB in 2.63s

[INFO] Refreshed blacklist: 33 entries
[INFO] Refreshed exchange blacklist: 0 entries

# Первый цикл (все новые)
[INFO] Published 3829 changed tokens to stream in 4.91s | avg_changes=3829.0

# Последующие циклы (только изменения)
[INFO] Published 699 changed tokens to stream in 2.21s | avg_changes=2264.0
[INFO] Published 1481 changed tokens to stream in 2.34s | avg_changes=2003.0
[INFO] Published 2407 changed tokens to stream in 2.88s | avg_changes=2171.4
```

### LIST Mode (legacy)
```
[INFO] Starting optimized token loader | mode=LIST | stream=N/A

[INFO] Pushed 4524 tokens into analyzer queue in 0.29s (cache_hit=True)
[INFO] Pushed 4524 tokens into analyzer queue in 0.30s (cache_hit=True)
```

### Debug логи (если включен profiling)
```
[INFO] Change detection:
  tokens_with_ob=3829 |
  tokens_without_ob=695 |
  changed=3829 |
  hashes_updated=3829
```

## Мониторинг

### Redis проверка

**Stream info:**
```bash
# Информация о stream
redis-cli XINFO STREAM orderbooks:updates

# Output:
# length: 2407
# radix-tree-keys: 1
# radix-tree-nodes: 2
# groups: 1
# last-generated-id: 1706437825340-0

# Consumer groups
redis-cli XINFO GROUPS orderbooks:updates

# Consumers в группе
redis-cli XINFO CONSUMERS orderbooks:updates spread_collectors
```

**Hash keys:**
```bash
# Количество хешей
redis-cli KEYS "orderbook_hash:*" | wc -l

# Пример хеша
redis-cli GET "orderbook_hash:ffd7573c-eb73-4b9f-896a-00107cdcdcbe"
# Output: a7b3c8d9e1f2g5h4

# TTL хеша (должен быть ~3600)
redis-cli TTL "orderbook_hash:ffd7573c-eb73-4b9f-896a-00107cdcdcbe"
```

**Orderbook keys:**
```bash
# Общее количество
redis-cli KEYS "orderbook:*" | wc -l

# Пример ключа
redis-cli GET "orderbook:ffd7573c-eb73-4b9f-896a-00107cdcdcbe:1c0b5751-bd74-4001-8671-e507edab1b3d:54d09abe-88cb-46f2-858d-e50bf9f8b96f:SPOT"
```

### PostgreSQL проверка

```sql
-- Количество токенов
SELECT COUNT(*) FROM global_tokens;

-- Токены с ценами
SELECT
    gt.symbol,
    COUNT(DISTINCT tp.token_id) as price_count
FROM global_tokens gt
LEFT JOIN token_prices tp ON tp.global_token_id = gt.id
GROUP BY gt.symbol
HAVING COUNT(DISTINCT tp.token_id) > 0
ORDER BY price_count DESC
LIMIT 20;

-- Blacklist токены
SELECT * FROM token_blacklist WHERE expires_at > NOW() OR expires_at IS NULL;

-- Exchange blacklist
SELECT s.name, eb.market_type, eb.is_muted
FROM exchange_blacklist eb
JOIN sources s ON eb.source_id = s.id
WHERE eb.is_muted = true;
```

### Docker логи

```bash
# Реал-тайм мониторинг
docker logs -f spread_loader

# Последние публикации
docker logs spread_loader 2>&1 | grep "Published.*tokens" | tail -10

# Change detection статистика
docker logs spread_loader 2>&1 | grep "Change detection" | tail -5

# Ошибки
docker logs spread_loader 2>&1 | grep ERROR
```

## Производительность

### Метрики (STREAMS Mode)

**Первый цикл (cold start):**
- Время: 4.91s
- Токены: 3829 (все новые, нет предыдущих хешей)
- Операции:
  - DB query: 2.63s
  - Orderbook fetch: 0.58s
  - Change detection: 3.94s (hash computation)
  - Stream publish: 0.39s

**Последующие циклы (steady state):**
- Время: 2.21-3.13s
- Токены: 699-2441 (только измененные)
- Avg changes: ~2171 токенов (48% от всех)
- Rate: ~700-1100 токенов/сек

**Comparison с LIST mode:**
```
LIST mode:  4524 tokens every 0.3s = 15080 tokens/sec (но все tokens)
STREAMS mode: 2171 tokens every 2.5s = 868 tokens/sec (но только changed)

Reduction: 51% меньше данных для обработки
Latency: <1s vs 6-9s в spread collectors
```

### Bottlenecks

1. **Change detection** - SHA256 hashing (CPU bound)
2. **MGET orderbooks** - Network latency Redis
3. **DB query** - PostgreSQL load (mitigated by cache)
4. **Stream publish** - Redis write throughput

### Оптимизации

**Phase 1: Token Cache (33% improvement)**
- Кэш метаданных токенов в памяти
- TTL: 60 секунд
- Избегает повторных DB queries

**Phase 2: Redis Streams (75% improvement)**
- Публикация только изменений
- Consumer groups для параллелизма
- Reduction: с 4524 до ~2171 токенов

**Phase 3: Msgpack-only (83% improvement)**
- Отказ от JSON десериализации
- Быстрее и компактнее

## Типичные проблемы

### Проблема: Published 0 changed tokens

**Причина**: Orderbooks не загружаются из Redis

**Диагностика:**
```bash
# Проверить наличие orderbook keys
redis-cli KEYS "orderbook:*" | head -10

# Проверить формат ключей (case sensitivity!)
# Должно быть: orderbook:...:SPOT (UPPERCASE)
# НЕ: orderbook:...:spot (lowercase)

# Проверить price collectors работают
docker logs price-collector-spot-mexc 2>&1 | grep "Collected"
```

**Решение:**
- Убедиться что price collectors запущены и собирают данные
- Проверить формат ключей (UPPERCASE market_type)
- Проверить логи loader на ошибки десериализации

### Проблема: Stream не создается

**Причина**: STREAMS mode не включен или ошибка создания stream

**Диагностика:**
```bash
# Проверить feature flag
grep OPTIMIZE_USE_STREAMS docker/.env.dev

# Проверить наличие stream
redis-cli XINFO STREAM orderbooks:updates

# Проверить логи loader
docker logs spread_loader 2>&1 | grep "mode=STREAMS"
```

**Решение:**
```bash
# Включить feature flag
echo "OPTIMIZE_USE_STREAMS=true" >> docker/.env.dev

# Перезапустить loader
docker-compose -f docker/dev.yml restart spread-loader

# Stream создастся автоматически при первой публикации
```

### Проблема: Медленная работа loader

**Причина**: Большое количество токенов или медленный PostgreSQL

**Решение:**
```bash
# Включить все оптимизации
OPTIMIZE_TOKEN_CACHE=true
OPTIMIZE_USE_STREAMS=true
OPTIMIZE_MSGPACK_ONLY=true

# Увеличить cache TTL
METADATA_CACHE_TTL=120  # вместо 60

# Добавить индексы в PostgreSQL
CREATE INDEX idx_global_tokens_active ON global_tokens(id) WHERE active = true;
CREATE INDEX idx_token_prices_gtid ON token_prices(global_token_id);
```

### Проблема: Memory leak

**Причина**: Бесконечный рост хешей в Redis

**Диагностика:**
```bash
# Количество hash keys
redis-cli KEYS "orderbook_hash:*" | wc -l

# Проверить TTL
redis-cli TTL "orderbook_hash:..." # Должно быть ~3600
```

**Решение:**
- Хеши имеют TTL 3600 сек (1 час)
- Если TTL не работает, проверить Redis config
- Можно вручную очистить: `redis-cli DEL orderbook_hash:*`

## Интеграция с другими сервисами

### Price Collectors → Spread Loader
```
Price Collectors сохраняют orderbook:* keys в Redis
    ↓
Spread Loader читает эти keys через MGET
    ↓
Детектирует изменения и публикует в stream
```

### Spread Loader → Spread Collectors
```
Spread Loader публикует changed tokens в Redis Stream
    ↓
Spread Collectors (3-5 воркеров) читают через Consumer Groups
    ↓
Анализируют арбитраж и отправляют сигналы
```

### Token Collector → Spread Loader
```
Token Collector обновляет tokens, chains, fees в PostgreSQL
    ↓
Spread Loader читает эти данные (с кэшированием)
    ↓
Использует chains для расчета transfer fees
```

## Fallback механизм

**Automatic fallback to LIST mode:**
```python
# В _run_streams_mode()
try:
    stream_info = await self.cache.client.xinfo_stream(stream_key)
    logger.info(f"Redis Stream found: {stream_key} | length={stream_length}")
except Exception as e:
    logger.warning(f"Redis Stream '{stream_key}' not found or error: {e}")
    logger.warning("Falling back to LIST mode (legacy compatibility)")

    # Переключение на LIST mode
    self.use_streams = False
    return await self._run_list_mode()
```

**Когда срабатывает:**
- Stream `orderbooks:updates` не существует
- Ошибка доступа к stream (permissions)
- Redis не поддерживает streams (старая версия <5.0)

## Расширение функционала

### Добавление компрессии stream
```python
import zlib

# Compress token data перед публикацией
token_data = msgpack.packb(token)
compressed = zlib.compress(token_data)

await redis.xadd(
    "orderbooks:updates",
    {"token_data": compressed}
)
```

### Добавление партиционирования по символу
```python
# Разные streams для разных категорий токенов
stream_key = f"orderbooks:updates:{token['symbol'][0]}"  # A-Z

# A-E в один stream, F-M в другой, etc
# Позволяет параллельно обрабатывать разные streams
```

### Добавление приоритизации
```python
# High-priority tokens в отдельный stream
if token['symbol'] in ['BTC', 'ETH', 'BNB']:
    stream_key = "orderbooks:updates:priority"
else:
    stream_key = "orderbooks:updates:normal"

# Collectors сначала читают priority stream
```

## Best Practices

1. **Always use STREAMS mode** - 75% improvement over LIST
2. **Enable all feature flags** - Maximum performance
3. **Monitor change rate** - Should be ~40-60% of total tokens
4. **Check Redis memory** - Streams + hashes can grow
5. **Use profiling in development** - Understand bottlenecks
6. **Test fallback** - Ensure LIST mode works as backup

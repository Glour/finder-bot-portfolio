# Price Distributor

## Назначение

Сервис для публикации списков доступных токенов из PostgreSQL в Redis для использования Price Collectors. Это мост между базой данных и коллекторами цен.

## Архитектура

```
┌───────────────────┐
│Price Distributor  │
└─────────┬─────────┘
          │
          ├─→ Читает tokens из PostgreSQL
          │
          ├─→ Публикует в Redis Sets:
          │   available_tokens:{source}:{id}:{SPOT|FUTURES}
          │
          └─→ Интервал: 60 секунд
```

## Основные файлы

- **Entry point**: `infrastructure/services/collectors/price_collector/distributor.py`
- **Dependencies**:
  - PostgreSQL (источник данных)
  - Redis (целевое хранилище)

## Флоу работы

### 1. Инициализация
```python
database.redis = aioredis.from_url(settings.redis.dsn)
database.init_database(settings)
repo = RequestsRepo(sessionmaker=database.async_session)
redis = RedisCache(database.redis)

await distributor(repo, redis)
```

### 2. Основной цикл

```
Каждые 60 секунд:

1. Получить все CEX биржи из БД
   ↓
2. Параллельно для каждой биржи:
   ↓
   a. Для каждого market_type (SPOT, FUTURES):
      ↓
      • Загрузить токены из PostgreSQL:
        SELECT * FROM tokens
        WHERE source_id = ?
        AND market_type = ?
        LIMIT 50000
      ↓
      • Проверить существующие токены в Redis Set
      ↓
      • Найти новые токены (отсутствующие в Redis)
      ↓
      • Для каждого нового токена:
        - Получить или создать global_token_source
        - Сформировать JSON payload:
          {
            "id": "token_id",
            "symbol": "BTC",
            "market_type": "SPOT",
            "global_token_id": "uuid",
            "global_source_id": "uuid"
          }
      ↓
      • Добавить новые токены в Redis Set:
        SADD available_tokens:{source}:{id}:{market_type} {json}...
```

### 3. Структура Redis

**Redis Key Pattern:**
```
available_tokens:{source_name}:{source_id}:{market_type}
```

**Примеры:**
```
available_tokens:mexc:550e8400-e29b-41d4-a716-446655440000:SPOT
available_tokens:mexc:550e8400-e29b-41d4-a716-446655440000:FUTURES
available_tokens:gate:650e8400-e29b-41d4-a716-446655440000:SPOT
available_tokens:gate:650e8400-e29b-41d4-a716-446655440000:FUTURES
```

**Value Format (JSON в Set):**
```json
{
  "id": "54d09abe-88cb-46f2-858d-e50bf9f8b96f",
  "symbol": "BTC",
  "market_type": "SPOT",
  "global_token_id": "ffd7573c-eb73-4b9f-896a-00107cdcdcbe",
  "global_source_id": "1c0b5751-bd74-4001-8671-e507edab1b3d"
}
```

## Пример работы

### Сценарий: Добавлен новый токен PEPE на MEXC

```
1. Token Collector обновил БД:
   INSERT INTO tokens (symbol, source_id, market_type)
   VALUES ('PEPE', 'mexc_uuid', 'SPOT')

2. Price Distributor (через 60 сек):

   a. Читает токены MEXC SPOT из БД
      → Находит PEPE

   b. Проверяет Redis Set:
      SMEMBERS available_tokens:mexc:{uuid}:SPOT
      → PEPE отсутствует

   c. Получает global_token_source:
      global_token_id = get_global_token("PEPE")
      global_source = get_or_create(global_token_id, mexc_uuid)

   d. Добавляет в Redis:
      SADD available_tokens:mexc:{uuid}:SPOT '{
        "id": "pepe_token_id",
        "symbol": "PEPE",
        "market_type": "SPOT",
        "global_token_id": "pepe_global_id",
        "global_source_id": "mexc_pepe_source_id"
      }'

   e. Логирует:
      [INFO] Added 1 new tokens into Redis for mexc/SPOT

3. Price Collector для MEXC SPOT:

   a. Читает available_tokens:mexc:{uuid}:SPOT
      → Находит PEPE

   b. Запрашивает orderbook для PEPE с MEXC API

   c. Сохраняет в Redis:
      prices:spot:PEPE
      orderbook:{global_token_id}:{global_source_id}:{token_id}:SPOT
```

## Инкрементальное обновление

**Почему важно:**
- Избегает дублирования токенов в Redis
- Минимизирует объем данных для передачи
- Ускоряет работу Price Collectors

**Алгоритм:**
```python
# Получить существующие токены из Redis
existing_tokens = await redis.client.smembers(redis_key)
existing_ids = set()

for token_json in existing_tokens:
    data = orjson.loads(token_json)
    existing_ids.add(data.get("global_token_id"))

# Добавить только новые
new_tokens = []
for token in tokens_from_db:
    if token.global_token_id not in existing_ids:
        new_tokens.append(token)

# Batch add
if new_tokens:
    await redis.client.sadd(redis_key, *new_tokens)
```

## Конфигурация

### Docker Compose
```yaml
price_distributor:
  container_name: price-distributor
  build:
    context: ..
    dockerfile: backend/api/Dockerfile
  command: python infrastructure/services/collectors/price_collector/distributor.py
  env_file:
    - .env.dev
  environment:
    PYTHONPATH: /app
  depends_on:
    - redis
    - postgres
  restart: unless-stopped
```

### Environment Variables
```bash
# PostgreSQL
DB_HOST=postgres
DB_PORT=5432
DB_NAME=dex-dev

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# Limits
TOKENS_LOAD_LIMIT=50000  # Max tokens per query
```

## Логи

### Успешная работа
```
[INFO] Added 15 new tokens into Redis for mexc/SPOT
[INFO] Added 0 new tokens into Redis for mexc/FUTURES
[INFO] Added 8 new tokens into Redis for gate/SPOT
[INFO] Added 23 new tokens into Redis for gate/FUTURES
```

### Ошибки
```
[ERROR] Error in distributor loop: connection to PostgreSQL lost
[ERROR] Failed to parse token data: Invalid JSON
[WARNING] global_token_id not found for token BTC
```

## Мониторинг

### Redis проверка
```bash
# Количество токенов для биржи
redis-cli SCARD available_tokens:mexc:550e8400-e29b-41d4-a716-446655440000:SPOT

# Просмотр токенов
redis-cli SMEMBERS available_tokens:mexc:550e8400-e29b-41d4-a716-446655440000:SPOT | head -5

# Все ключи distributor
redis-cli KEYS "available_tokens:*"
```

### PostgreSQL проверка
```sql
-- Сколько токенов в БД для биржи
SELECT market_type, COUNT(*)
FROM tokens
WHERE source_id = '550e8400-e29b-41d4-a716-446655440000'
GROUP BY market_type;

-- Последние добавленные токены
SELECT symbol, market_type, created_at
FROM tokens
WHERE source_id = '550e8400-e29b-41d4-a716-446655440000'
ORDER BY created_at DESC
LIMIT 10;
```

## Производительность

### Метрики
- **Цикл обработки**: ~1-3 секунды
- **Токенов на биржу**: ~500-2000
- **Новых токенов за цикл**: 0-50 (обычно)
- **Memory usage**: ~100-200 MB

### Оптимизации
- **Инкрементальное обновление**: Добавляем только новые токены
- **Parallel processing**: Обработка всех бирж параллельно через `asyncio.gather`
- **Batch operations**: SADD для множественных токенов одной командой
- **JSON parsing**: Используем orjson для быстрой сериализации

## Типичные проблемы

### Проблема: Токены не появляются в Redis
**Причины**:
1. Токены уже существуют в Redis (инкрементальная логика)
2. Нет связи token → global_token
3. Ошибка сериализации JSON

**Решение**:
```bash
# Проверить наличие токенов в БД
psql -c "SELECT COUNT(*) FROM tokens WHERE source_id = '...' AND market_type = 'SPOT'"

# Проверить Redis
redis-cli SCARD available_tokens:mexc:...:SPOT

# Удалить Redis Set для пересоздания
redis-cli DEL available_tokens:mexc:...:SPOT
# Дождаться следующего цикла distributor (60 сек)
```

### Проблема: Дублирование токенов
**Причина**: Несколько инстансов distributor или race condition

**Решение**:
- Запускать только 1 инстанс distributor (container_name в docker-compose)
- Использовать Redis locks если нужно несколько инстансов

### Проблема: Медленная работа
**Причина**: Большое количество токенов или медленный PostgreSQL

**Решение**:
- Увеличить `tokens_load_limit`
- Добавить индексы на `source_id` и `market_type`
- Использовать read replica для PostgreSQL

## Интеграция с другими сервисами

### Token Collector → Price Distributor
```
Token Collector сохраняет новые tokens в PostgreSQL
    ↓
Price Distributor читает tokens каждые 60 сек
    ↓
Публикует новые в Redis
```

### Price Distributor → Price Collectors
```
Price Distributor публикует available_tokens:* в Redis
    ↓
Price Collectors читают эти Sets
    ↓
Запрашивают orderbooks для этих токенов с бирж
```

## Безопасность

### Rate Limiting
- **БД запросы**: Ограничены `tokens_load_limit=50000`
- **Redis операции**: Batch SADD (множественные токены за 1 запрос)

### Error Handling
```python
try:
    # Процесс обработки биржи
    await load_source_tokens_incremental(source, repo, redis, tokens_load_limit)
except Exception as e:
    logger.error(f"Error in distributor loop: {e}", exc_info=True)
    # Продолжает работу для других бирж
```

## Расширение функционала

### Добавление фильтрации токенов
```python
# Например, только токены с ликвидностью > $100k
tokens = await repo.tokens.get_all(
    source_id=source.id,
    market_type=market_type,
    limit=tokens_load_limit,
    # Новый параметр:
    min_liquidity_usd=100000
)
```

### Поддержка DEX
```python
# Добавить в цикл обработки
for source_type in [SourceType.CEX, SourceType.DEX]:
    sources = await repo.sources.get_all_by_type(source_type)
    # ... процесс аналогичен
```

### Очистка устаревших токенов
```python
# Удалять токены, которые больше не в БД
db_token_ids = {token.global_token_id for token in tokens}
redis_token_ids = get_existing_ids(redis_key)

removed_ids = redis_token_ids - db_token_ids
if removed_ids:
    # SREM для удаления устаревших
    await redis.client.srem(redis_key, *removed_ids)
```

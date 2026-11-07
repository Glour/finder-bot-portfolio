# Token Collector

## Назначение

Сервис для сбора и обновления информации о токенах на различных блокчейн сетях, включая:
- Contract addresses (адреса контрактов)
- Withdrawal fees (комиссии за вывод)
- Deposit/withdrawal status (статусы депозита/вывода)
- Chain information (информация о сетях)

## Архитектура

```
┌─────────────────┐
│ Token Collector │
└────────┬────────┘
         │
         ├─→ Запрашивает данные с бирж через API
         │
         ├─→ Обновляет PostgreSQL:
         │   • global_tokens
         │   • tokens
         │   • token_on_chains
         │
         └─→ Интервал: 120 секунд
```

## Основные файлы

- **Entry point**: `infrastructure/services/collectors/token_collector/main.py`
- **Manager**: `infrastructure/services/collectors/token_collector/manager.py`
- **Collectors**: `infrastructure/services/collectors/token_collector/collectors/`
  - `mexc.py` - MEXC exchange
  - `gate.py` - Gate.io exchange
  - `xt.py` - XT exchange
  - `lbank.py` - LBank exchange

## Флоу работы

### 1. Инициализация
```python
# main.py
database.init_database(settings)
repo = RequestsRepo(sessionmaker=database.async_session)
redis_cache = RedisCache(database.redis)

token_collector = get_token_collector_manager(
    repo=repo,
    cache=redis_cache,
    interval=120.0  # 2 минуты
)
```

### 2. Цикл сбора данных

```
Каждые 120 секунд:

1. Получить список бирж из БД (CEX)
   ↓
2. Для каждой биржи:
   ↓
   a. Запросить информацию о токенах через API
      - GET /api/v3/capital/config/getall (MEXC)
      - GET /api/v4/wallet/currency_chains (Gate)
   ↓
   b. Для каждого токена:
      ↓
      • Найти или создать global_token (BTC, ETH, etc)
      • Создать token записи для биржи (SPOT/FUTURES)
      • Создать token_on_chains с chain data:
        - chain_name (ETH, BSC, TRX, etc)
        - contract_address
        - withdrawal_fee
        - withdrawal_fee_in_usdt
        - deposit_enabled, withdraw_enabled
   ↓
3. Сохранить все изменения в PostgreSQL (bulk operations)
```

### 3. Пример данных

**API Response (MEXC):**
```json
{
  "coin": "BTC",
  "networkList": [
    {
      "network": "BTC",
      "withdrawFee": "0.0005",
      "depositEnable": true,
      "withdrawEnable": true,
      "contract": ""
    },
    {
      "network": "ERC20",
      "withdrawFee": "0.0035",
      "depositEnable": true,
      "withdrawEnable": true,
      "contract": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
    }
  ]
}
```

**Сохраняется в БД:**
```
global_tokens:
  id: uuid
  symbol: "BTC"
  name: "Bitcoin"

tokens:
  id: uuid
  symbol: "BTC"
  source_id: uuid (MEXC)
  market_type: SPOT

token_on_chains:
  id: uuid
  token_id: uuid
  chain_name: "BTC"
  contract_address: ""
  withdrawal_fee: 0.0005
  withdrawal_fee_in_usdt: ~25 USDT (calculated)
  deposit_enabled: true
  withdraw_enabled: true
```

## Важные функции

### Нормализация chain names
```python
# Приводит названия сетей к единому формату:
# - "ERC20" → "ETH"
# - "BEP20(BSC)" → "BSC"
# - "TRC20" → "TRX"
```

### Расчет комиссий в USDT
```python
# Конвертирует withdrawal_fee в USDT
# Использует актуальные цены из БД или API
```

### Фильтрация по blacklist
```python
# Пропускает токены из token_blacklist
# Можно временно (expires_at) или навсегда
```

## Конфигурация

### Docker Compose
```yaml
token-collector:
  build:
    context: ..
    dockerfile: backend/api/Dockerfile
  command: python infrastructure/services/collectors/token_collector/main.py
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

# Proxies (optional)
# Файл data/proxies.txt с HTTP прокси
```

## Логи

### Успешный сбор
```
[INFO] Token collector started
[INFO] Collecting tokens for source: MEXC
[INFO] Collected 523 tokens from MEXC
[INFO] Saved 523 tokens to database
```

### Ошибки
```
[ERROR] Failed to fetch tokens from MEXC: Connection timeout
[WARNING] Token BTC already exists, skipping
[ERROR] Failed to calculate withdrawal fee for ETH: Price not found
```

## Мониторинг

### Проверка работоспособности
```bash
# Логи контейнера
docker logs token-collector

# Проверка БД
psql -h localhost -U dex-dev -d dex-dev
SELECT COUNT(*) FROM global_tokens;
SELECT COUNT(*) FROM tokens;
SELECT COUNT(*) FROM token_on_chains;

# Последние обновления
SELECT symbol, updated_at
FROM global_tokens
ORDER BY updated_at DESC
LIMIT 10;
```

## Типичные проблемы

### Проблема: Не обновляются токены
**Причина**: API биржи недоступна или изменился формат
**Решение**:
1. Проверить логи на ошибки API
2. Проверить доступность API через curl
3. Обновить collector если изменился формат

### Проблема: Дублирование токенов
**Причина**: Неправильная нормализация symbol или chain_name
**Решение**:
1. Проверить функции нормализации
2. Добавить дополнительные правила в normalize_chain_name

### Проблема: Медленный сбор
**Причина**: Слишком много запросов к API или медленные прокси
**Решение**:
1. Увеличить интервал сбора
2. Использовать batch requests где возможно
3. Проверить скорость прокси

## Интеграция с другими сервисами

### → Price Distributor
```
Token Collector сохраняет tokens в БД
    ↓
Price Distributor читает tokens
    ↓
Публикует списки в Redis
```

### → Spread Loader
```
Token Collector обновляет token_on_chains
    ↓
Spread Loader читает chains для расчета fees
    ↓
Используется в анализе арбитража
```

## API эндпоинты бирж

### MEXC
- **URL**: https://api.mexc.com/api/v3/capital/config/getall
- **Method**: GET
- **Auth**: API Key + Secret (опционально)
- **Rate Limit**: 20 requests/second

### Gate.io
- **URL**: https://api.gateio.ws/api/v4/wallet/currency_chains
- **Method**: GET
- **Auth**: API Key + Secret
- **Rate Limit**: 300 requests/10 seconds

### XT
- **URL**: https://sapi.xt.com/v4/public/wallet/support/currency
- **Method**: GET
- **Auth**: None
- **Rate Limit**: 100 requests/minute

### LBank
- **URL**: https://www.lbank.info/v2/supplement/coinConfig.do
- **Method**: GET
- **Auth**: None
- **Rate Limit**: 60 requests/minute

## Расширение функционала

### Добавление новой биржи
1. Создать файл `collectors/{exchange_name}.py`
2. Реализовать класс наследующий `BaseCollector`
3. Implement метод `collect_tokens()`
4. Добавить в `manager.py`:
   ```python
   if source.name == "new_exchange":
       collector = NewExchangeCollector(source, repo, cache)
   ```

### Добавление нового поля
1. Добавить поле в модель `token_on_chains`
2. Создать миграцию Alembic
3. Обновить collector для заполнения поля
4. Запустить миграцию на prod

## Производительность

### Метрики
- **Время сбора**: ~30-60 секунд на биржу
- **Количество токенов**: ~500-2000 на биржу
- **Запросов к API**: 1-5 на биржу
- **Запросов к БД**: 3-10 bulk operations

### Оптимизации
- Bulk insert/update для токенов
- Кэширование global_tokens в памяти
- Параллельный сбор с разных бирж
- Использование connection pool для БД

# Архитектура системы арбитражного анализа

## Общий обзор

Система построена на микросервисной архитектуре и предназначена для сбора цен с криптовалютных бирж, анализа арбитражных возможностей и отправки торговых сигналов в Telegram.

## Основной флоу данных

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ЭТАП 1: СБОР ДАННЫХ                          │
└─────────────────────────────────────────────────────────────────────┘

1. Token Collector
   ↓ Собирает информацию о токенах (chains, withdrawal fees, contract addresses)
   ↓ Каждые 2 минуты обновляет данные в PostgreSQL

2. Price Distributor
   ↓ Загружает токены из БД и публикует списки доступных токенов в Redis
   ↓ Каждую минуту обновляет Redis Sets: available_tokens:{source}:{id}:{SPOT|FUTURES}

3. Price Collectors (параллельно для каждой биржи и типа рынка)
   ↓ Читают списки токенов из Redis
   ↓ Запрашивают orderbooks с бирж (MEXC, Gate, XT, LBank)
   ↓ Сохраняют в Redis с ключами:
   ↓   • prices:{spot|futures}:{symbol}  - быстрый доступ для saver
   ↓   • orderbook:{gtid}:{gts_id}:{token_id}:{SPOT|FUTURES} - для spread loader

┌─────────────────────────────────────────────────────────────────────┐
│                    ЭТАП 2: СОХРАНЕНИЕ В БД                           │
└─────────────────────────────────────────────────────────────────────┘

4. Price Saver
   ↓ Каждые 60 секунд сканирует Redis keys: prices:spot:*, prices:futures:*
   ↓ Десериализует msgpack данные
   ↓ Массово сохраняет в PostgreSQL (bulk upsert батчами по 1000)

┌─────────────────────────────────────────────────────────────────────┐
│                   ЭТАП 3: АНАЛИЗ АРБИТРАЖА                           │
└─────────────────────────────────────────────────────────────────────┘

5. Spread Loader (ОПТИМИЗИРОВАННЫЙ с Redis Streams)
   ↓ Загружает токены с ценами из PostgreSQL
   ↓ Подгружает orderbooks из Redis (orderbook:* keys)
   ↓ Детектирует изменения через SHA256 хеширование orderbooks
   ↓ Публикует ТОЛЬКО ИЗМЕНЕННЫЕ токены в Redis Stream: orderbooks:updates

6. Spread Collectors (3-5 параллельных воркеров с Consumer Groups)
   ↓ Подписываются на Redis Stream через Consumer Groups
   ↓ Получают только измененные токены (не все 4500+)
   ↓ Анализируют арбитражные возможности:
   ↓   • Spot-Spot (между биржами)
   ↓   • Futures-Futures
   ↓   • Spot-Futures (funding rate arbitrage)
   ↓   • CEX-DEX (centralized vs decentralized)
   ↓ Фильтруют по порогам прибыли и спреда
   ↓ Отправляют сигналы в Telegram через bot messenger

┌─────────────────────────────────────────────────────────────────────┐
│                      РЕЗУЛЬТАТ: СИГНАЛЫ                              │
└─────────────────────────────────────────────────────────────────────┘

7. Telegram Bot
   ↓ Получает сигналы с уровнями: LOW, MEDIUM, HIGH, VERY_HIGH
   ↓ Отправляет в соответствующие каналы
```

## Ключевые компоненты

### PostgreSQL
- **Назначение**: Постоянное хранилище данных
- **Таблицы**:
  - `global_tokens` - глобальные токены (BTC, ETH и т.д.)
  - `tokens` - токены на конкретных биржах
  - `token_on_chains` - сопоставление токенов с блокчейн сетями
  - `token_prices` - исторические цены и orderbooks
  - `sources` - биржи (MEXC, Gate, XT, LBank)
  - `token_blacklist` - токены для игнорирования
  - `exchange_blacklist` - биржи/рынки для игнорирования

### Redis
- **Назначение**: Кэш и шина обмена данными в реальном времени

**Структура ключей:**

```
# Списки доступных токенов (для price collectors)
available_tokens:{source_name}:{source_id}:{SPOT|FUTURES}
  → Redis Set с JSON токенами
  → Обновляется price_distributor каждую минуту

# Свежие цены (для price_saver)
prices:spot:{symbol}
prices:futures:{symbol}
  → msgpack сериализация
  → TTL: без ограничений

# Orderbooks (для spread_loader)
orderbook:{global_token_id}:{global_source_id}:{token_id}:{SPOT|FUTURES}
  → msgpack сериализация с полями: orderbook, get_at
  → Обновляется price collectors

# Хеши для детекции изменений
orderbook_hash:{global_token_id}
  → SHA256 хеш (16 символов) от всех orderbooks токена
  → TTL: 3600 секунд
  → Используется spread_loader для change detection

# Redis Stream для STREAMS mode
orderbooks:updates
  → Поля: token_data (msgpack)
  → Consumer Groups: spread_collectors
  → Consumers: worker-{pid}
```

### Telegram Bot
- **Назначение**: Получение сигналов и отправка в каналы
- **Каналы**: Отдельные для каждого уровня сигнала (LOW, MEDIUM, HIGH, VERY_HIGH)

## Оптимизации

### Phase 1: Token Cache (33% improvement)
- Кэширование метаданных токенов в памяти loader
- TTL: 60 секунд
- Избегает повторных SQL запросов

### Phase 2: Redis Streams (75% improvement)
- Публикация только измененных токенов
- Consumer Groups для параллельной обработки
- Reduction: с 4524 токенов до ~1500-2500 измененных

### Phase 3: Msgpack-only (83% improvement)
- Отказ от JSON в пользу msgpack
- Быстрая десериализация
- Компактное хранение

### Результат оптимизаций:
- **До**: 6-9 секунд на полный цикл (LIST mode)
- **После**: <1 секунда на цикл (STREAMS mode)
- **Latency reduction**: >85%

## Микросервисы

### 1. Token Collector
- **Файл**: `token_collector/main.py`
- **Назначение**: Сбор информации о токенах (chains, fees)
- **Интервал**: 120 секунд
- **Документация**: [Token Collector](./services/token-collector.md)

### 2. Price Distributor
- **Файл**: `price_collector/distributor.py`
- **Назначение**: Публикация списков токенов в Redis
- **Интервал**: 60 секунд
- **Документация**: [Price Distributor](./services/price-distributor.md)

### 3. Price Collectors
- **Файл**: `price_collector/main.py`
- **Назначение**: Сбор orderbooks с бирж
- **Инстансы**: По одному на биржу и тип рынка (spot/futures)
  - price-collector-spot-mexc
  - price-collector-futures-mexc
  - price-collector-spot-gate
  - price-collector-futures-gate
  - price-collector-spot-xt
  - price-collector-futures-xt
  - price-collector-spot-lbank
  - price-collector-futures-lbank
- **Документация**: [Price Collector](./services/price-collector.md)

### 4. Price Saver
- **Файл**: `price_collector/saver.py`
- **Назначение**: Сохранение цен из Redis в PostgreSQL
- **Интервал**: 60 секунд
- **Документация**: [Price Saver](./services/price-saver.md)

### 5. Spread Loader
- **Файл**: `spread_collector/loader_optimized.py`
- **Назначение**: Загрузка токенов и публикация изменений в stream
- **Режимы**: LIST (legacy) или STREAMS (optimized)
- **Документация**: [Spread Loader](./services/spread-loader.md)

### 6. Spread Collectors
- **Файл**: `spread_collector/main_optimized.py`
- **Назначение**: Анализ арбитража и отправка сигналов
- **Инстансы**: 3-5 параллельных воркеров
- **Документация**: [Spread Collector](./services/spread-collector.md)

## Управление через Docker Compose

```yaml
# docker/dev.yml
services:
  spread-loader: 1 инстанс
  spread-collector: 3-5 инстансов (scale)
  price-collector-*: 8 инстансов (spot+futures для каждой биржи)
  price-distributor: 1 инстанс
  price-saver: 1 инстанс
  token-collector: 1 инстанс
  bot: 1 инстанс
  postgres: 1 инстанс
  redis: 1 инстанс
```

## Feature Flags

Оптимизации управляются через `.env.dev`:

```bash
# Phase 1: Token metadata caching (33% improvement)
OPTIMIZE_TOKEN_CACHE=true

# Phase 2: Redis Streams (75% improvement - requires loader_optimized.py!)
OPTIMIZE_USE_STREAMS=true

# Phase 3: Msgpack-only serialization (83% improvement)
OPTIMIZE_MSGPACK_ONLY=true

# Profiling and metrics
OPTIMIZE_ENABLE_PROFILING=true
OPTIMIZE_ENABLE_LATENCY_TRACKING=true
```

## Мониторинг

### Логи (Dozzle)
- **URL**: http://localhost:8080
- **Назначение**: Просмотр логов всех контейнеров в реальном времени

### Ключевые метрики в логах

**Spread Loader (STREAMS mode):**
```
Published 2407 changed tokens to stream in 2.88s | avg_changes=2171.4
```

**Spread Collector (STREAMS mode):**
```
STREAMS cycle | messages=100 | tokens=100 |
read=0.012s | parse=0.014s | analyze=0.051s | ack=0.009s |
total=0.087s | results=2
```

**Price Collector:**
```
Collected 1523 prices in 45.23s
```

## Диагностика проблем

### Проблема: Нет сигналов
1. Проверить логи spread-collector (результаты анализа)
2. Проверить логи spread-loader (публикация в stream)
3. Проверить Redis: `XINFO STREAM orderbooks:updates`
4. Проверить логи price-collector (сбор orderbooks)

### Проблема: Медленная работа
1. Проверить режим: STREAMS vs LIST mode
2. Проверить feature flags в .env.dev
3. Проверить логи profiling (если включен)
4. Проверить load average на сервере

### Проблема: Ошибки десериализации
1. Проверить msgpack vs JSON формат в Redis
2. Проверить логи price-collector (сериализация)
3. Проверить логи spread-loader (десериализация)

## Дальнейшие улучшения

1. **Горизонтальное масштабирование**: Увеличить количество spread-collector воркеров
2. **Кэширование chain fees**: Избежать повторных расчетов комиссий
3. **Adaptive thresholds**: Динамические пороги прибыли на основе волатильности
4. **Historical analysis**: Анализ исторических данных для оптимизации стратегий

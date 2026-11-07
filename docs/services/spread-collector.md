# Spread Collector

## Назначение

**Финальный сервис** в pipeline, который анализирует арбитражные возможности между биржами и типами рынков. Получает токены из Redis Stream, вычисляет спреды, фильтрует по порогам и отправляет торговые сигналы в Telegram.

## Архитектура

```
┌─────────────────────┐
│  Spread Collector   │
│  (3-5 воркеров)     │
└──────────┬──────────┘
           │
           ├─→ Читает из Redis Stream через Consumer Groups
           │   orderbooks:updates (только changed tokens)
           │
           ├─→ Анализирует арбитраж:
           │   • Spot-Spot (между биржами)
           │   • Futures-Futures
           │   • Spot-Futures (funding rate)
           │   • CEX-DEX
           │
           ├─→ Фильтрует по порогам:
           │   • Spread percent > threshold
           │   • Profit (USDT) > minimum
           │   • Signal level: LOW/MEDIUM/HIGH/VERY_HIGH
           │
           └─→ Отправляет сигналы в Telegram Bot
```

## Режимы работы

### STREAMS Mode (Оптимизированный) ✅

**Особенности:**
- Читает только измененные токены через Redis Streams
- Consumer Groups для параллельной обработки
- Каждый воркер обрабатывает свою часть сообщений
- Latency: 60-337ms на батч 100 токенов

**Включение:**
```bash
# .env.dev
OPTIMIZE_USE_STREAMS=true
```

### LIST Mode (Legacy)

**Особенности:**
- Читает все токены из Redis LIST
- Все воркеры обрабатывают одинаковый набор
- Latency: 0.24-0.52s на 4524 токена
- Используется как fallback

**Включение:**
```bash
# .env.dev
# OPTIMIZE_USE_STREAMS=true  # Закомментировать
```

## Основные файлы

- **Entry point**: `infrastructure/services/collectors/spread_collector/main_optimized.py`
- **Manager**: `infrastructure/services/collectors/spread_collector/manager_optimized.py`
- **Analyzer**: `infrastructure/services/collectors/spread_collector/analyzer.py`
- **Broadcaster**: `infrastructure/services/collectors/spread_collector/broadcaster.py`
- **Filters**: `infrastructure/services/collectors/spread_collector/filters.py`
- **Legacy**: `infrastructure/services/collectors/spread_collector/main.py`, `manager.py`

## Флоу работы (STREAMS Mode)

### 1. Инициализация

```python
database.init_database(settings)
repo = RequestsRepo(sessionmaker=database.async_session)
cache = RedisCache(database.redis)

# Конфигурация
use_streams = ENV_CONFIG.use_streams
consumer_group = "spread_collectors"
consumer_name = f"worker-{os.getpid()}"

manager = OptimizedArbitrageAnalyzerManager(
    repo=repo,
    cache=cache,
    tokens_load_limit=10000,
    use_streams=use_streams,
    consumer_group=consumer_group,
    consumer_name=consumer_name
)

await manager.run()
```

### 2. Consumer Group Setup

```python
# Создать consumer group (если не существует)
try:
    await redis.xgroup_create(
        "orderbooks:updates",
        "spread_collectors",
        id="0",
        mkstream=True
    )
except Exception as e:
    if "BUSYGROUP" not in str(e):
        logger.warning(f"Failed to create consumer group: {e}")
```

**Consumer Group позволяет:**
- Параллельную обработку сообщений несколькими воркерами
- Каждое сообщение обрабатывается только одним воркером
- Автоматический rebalancing при добавлении/удалении воркеров
- Acknowledgement для надежной доставки

### 3. Чтение из Stream

```
Непрерывный цикл:

1. XREADGROUP из stream
   ↓
   messages = await redis.xreadgroup(
       group_name="spread_collectors",
       consumer_name="worker-12345",
       streams={"orderbooks:updates": ">"},
       count=100,          # Батч до 100 сообщений
       block=1000          # Ждать до 1 сек новых сообщений
   )

2. Parse messages
   ↓
   for stream, entries in messages:
       for message_id, data in entries:
           token_data = data.get("token_data")
           token = msgpack.unpackb(token_data)
           tokens_to_analyze.append(token)

3. Analyze tokens (батчами по 50)
   ↓
   for batch in batched(tokens, batch_size=50):
       results = await asyncio.gather(
           *[analyze_token_group(t) for t in batch]
       )

4. Send results (non-blocking)
   ↓
   if results:
       asyncio.create_task(messenger.send_results(results))

5. Acknowledge messages
   ↓
   await redis.xack(
       "orderbooks:updates",
       "spread_collectors",
       *message_ids
   )

   [INFO] STREAMS cycle | messages=100 | tokens=100 |
          read=0.012s | parse=0.014s | analyze=0.051s |
          ack=0.009s | total=0.087s | results=2
```

### 4. Анализ арбитража

**Типы арбитража:**

**1. Spot-Spot (между биржами)**
```python
# Пример: BTC на MEXC vs Gate
# MEXC: bid=43250, ask=43251
# Gate: bid=43300, ask=43301

# Купить на MEXC по 43251, продать на Gate по 43300
spread = 43300 - 43251 = 49 USDT
spread_percent = (49 / 43251) * 100 = 0.113%

# Учесть withdrawal fee
withdrawal_fee = 0.0005 BTC * 43275 = 21.64 USDT
profit = 49 - 21.64 = 27.36 USDT

# Проверить порог
if spread_percent > 0.1% and profit > 20:
    # Сигнал!
```

**2. Futures-Futures**
```python
# Пример: ETH futures на разных биржах
# Аналогично spot-spot, но для futures контрактов
```

**3. Spot-Futures (Funding Rate)**
```python
# Пример: Funding rate арбитраж
# Long spot ETH на MEXC, Short futures ETH на Gate
# Получать funding rate каждые 8 часов

# Если funding rate > 0.1% per 8h = 1.2% per day
# И spread позволяет открыть позицию с прибылью
```

**4. CEX-DEX**
```python
# Пример: PEPE на MEXC (CEX) vs Uniswap (DEX)
# Учесть:
# - Gas fees на Ethereum
# - Slippage на DEX
# - Withdrawal fees с CEX
```

### 5. Вычисление спреда

```python
def calculate_spread(buy_orderbook, sell_orderbook, volume_usdt=100):
    """
    buy_orderbook: {"asks": [[price, amount], ...]}
    sell_orderbook: {"bids": [[price, amount], ...]}
    """

    # Найти среднюю цену покупки для объема
    buy_price = 0
    buy_volume = 0
    remaining = volume_usdt

    for price, amount in buy_orderbook["asks"]:
        order_value = price * amount
        if order_value <= remaining:
            buy_volume += amount
            buy_price += price * amount
            remaining -= order_value
        else:
            # Частичное исполнение
            partial_amount = remaining / price
            buy_volume += partial_amount
            buy_price += remaining
            remaining = 0
            break

    avg_buy_price = buy_price / buy_volume if buy_volume > 0 else 0

    # Аналогично для продажи
    sell_price = 0
    sell_volume = 0
    remaining = volume_usdt

    for price, amount in sell_orderbook["bids"]:
        order_value = price * amount
        if order_value <= remaining:
            sell_volume += amount
            sell_price += price * amount
            remaining -= order_value
        else:
            partial_amount = remaining / price
            sell_volume += partial_amount
            sell_price += remaining
            remaining = 0
            break

    avg_sell_price = sell_price / sell_volume if sell_volume > 0 else 0

    # Спред
    spread = avg_sell_price - avg_buy_price
    spread_percent = (spread / avg_buy_price) * 100

    # Profit в USDT
    profit = spread * min(buy_volume, sell_volume)

    return {
        "buy_price": avg_buy_price,
        "sell_price": avg_sell_price,
        "spread": spread,
        "spread_percent": spread_percent,
        "profit": profit,
        "volume": min(buy_volume, sell_volume)
    }
```

### 6. Фильтрация и уровни сигналов

```python
class SignalLevelType(str, Enum):
    NONE = "NONE"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    VERY_HIGH = "VERY_HIGH"

def get_level(spread_percent, profit_usdt):
    """Определить уровень сигнала"""

    # Spot арбитраж
    if profit_usdt > 100 and spread_percent > 1.0:
        return SignalLevelType.VERY_HIGH

    if profit_usdt > 50 and spread_percent > 0.5:
        return SignalLevelType.HIGH

    if profit_usdt > 25 and spread_percent > 0.3:
        return SignalLevelType.MEDIUM

    if profit_usdt > 10 and spread_percent > 0.15:
        return SignalLevelType.LOW

    return SignalLevelType.NONE
```

### 7. Отправка сигналов

```python
# Формат сигнала
signal = {
    "symbol": "BTC",
    "buy_from": "spot_mexc",
    "sell_to": "spot_gate",
    "buy_price": 43251.0,
    "sell_price": 43300.0,
    "spread": 49.0,
    "spread_percent": 0.113,
    "profit": 27.36,
    "profit_before_fee": 49.0,
    "applied_fee": 21.64,
    "volume": 0.002315,  # BTC
    "level": "MEDIUM",
    "chains": [
        {
            "chain_name": "BTC",
            "withdrawal_fee": 0.0005,
            "deposit_enabled": true,
            "withdraw_enabled": true
        }
    ],
    "analyzed_at": 1706437825340
}

# Отправка в Telegram
await messenger.send_results([signal])
```

**Telegram сообщение:**
```
🔔 MEDIUM SIGNAL

Symbol: BTC
Direction: MEXC → Gate.io
Type: Spot-Spot

Buy: $43,251.00 (MEXC)
Sell: $43,300.00 (Gate.io)

Spread: 0.113% ($49.00)
Profit: $27.36 (after fees)
Volume: 0.002315 BTC

Fees: $21.64 (BTC network + withdrawal)

Chains available:
• BTC ✅ (withdrawal: 0.0005 BTC)

⏰ 2024-01-28 09:37:05
```

## Конфигурация

### Docker Compose

```yaml
spread-collector:
  build:
    context: ..
    dockerfile: backend/api/Dockerfile
  command: python infrastructure/services/collectors/spread_collector/main_optimized.py
  env_file:
    - .env.dev
  environment:
    PYTHONPATH: /app
    # Feature flags наследуются из .env.dev
  depends_on:
    - redis
    - postgres
    - bot
    - spread-loader
  restart: unless-stopped
  scale: 3  # 3-5 параллельных воркеров
```

### Performance Config

```python
@dataclass
class PerformanceConfig:
    analysis_interval: float = 0.1      # Интервал между циклами (сек)
    batch_size: int = 50                # Размер батча токенов
    enable_profiling: bool = False      # Детальные логи
    enable_latency_tracking: bool = False  # Отслеживание latency
```

## Логи

### STREAMS Mode
```
[INFO] Initialized OptimizedArbitrageAnalyzerManager |
       mode=STREAMS | consumer=worker-12345 |
       group=spread_collectors | batch_size=50

[INFO] Running in STREAMS mode | consumer=worker-12345
[INFO] Redis Stream found: orderbooks:updates | length=2407
[INFO] Created consumer group 'spread_collectors' on stream 'orderbooks:updates'

[INFO] STREAMS cycle | messages=100 | tokens=100 |
       read=0.012s | parse=0.014s | analyze=0.051s |
       ack=0.009s | total=0.087s | results=2

[INFO] STREAMS cycle | messages=91 | tokens=91 |
       read=0.012s | parse=0.014s | analyze=0.051s |
       ack=0.009s | total=0.087s | results=0
```

### LIST Mode
```
[INFO] Running in LIST mode (legacy)

[INFO] LIST mode cycle | tokens=4524 |
       redis=0.05s | deser=0.12s (msgpack=4524) |
       analyze=0.15s | send=0.00s |
       total=0.32s | results=3
```

## Мониторинг

### Redis проверка

**Consumer group info:**
```bash
# Информация о группе
redis-cli XINFO GROUPS orderbooks:updates

# Output:
# name: spread_collectors
# consumers: 3
# pending: 0
# last-delivered-id: 1706437825340-123

# Информация о consumers
redis-cli XINFO CONSUMERS orderbooks:updates spread_collectors

# Output:
# name: worker-12345
# pending: 0
# idle: 523  # ms since last read
```

**Pending messages:**
```bash
# Сообщения ожидающие ACK
redis-cli XPENDING orderbooks:updates spread_collectors

# Если pending > 0 долгое время = проблема с воркером
```

### Docker логи

```bash
# Все воркеры
docker-compose -f docker/dev.yml logs -f spread-collector

# Конкретный воркер
docker logs docker-spread-collector-1

# Статистика результатов
docker logs docker-spread-collector-1 2>&1 | grep "results=" | tail -20

# Ошибки
docker logs docker-spread-collector-1 2>&1 | grep ERROR
```

### Telegram проверка

```bash
# Проверить что сигналы доходят
# Открыть Telegram канал и посмотреть последние сообщения

# Частота сигналов зависит от рынка:
# - Высокая волатильность = больше сигналов
# - Низкая волатильность = меньше сигналов
```

## Производительность

### Метрики (STREAMS Mode)

**Per worker:**
- Messages/cycle: 50-100
- Cycle time: 60-337ms
- Throughput: ~150-1000 tokens/sec
- Results/cycle: 0-5 (зависит от рынка)
- Memory: 200-400 MB
- CPU: 15-35%

**3 workers total:**
- Combined throughput: ~450-3000 tokens/sec
- Processing всех changed tokens: <1 секунда
- End-to-end latency (orderbook update → signal): <2 сек

### Comparison с LIST mode

```
LIST Mode (1 worker):
- Process 4524 tokens in 0.32s
- But всегда обрабатывает все 4524
- No parallelism

STREAMS Mode (3 workers):
- Process ~2171 changed tokens
- Разделено между 3 воркерами (~724 per worker)
- Parallel processing
- Total time: <1 sec

Result: 85% latency reduction
```

## Типичные проблемы

### Проблема: Нет сигналов (results=0)

**Причины:**
1. Нет арбитражных возможностей (нормально в стабильном рынке)
2. Слишком высокие пороги фильтрации
3. Большие withdrawal fees съедают profit
4. Нет общих chains между биржами

**Диагностика:**
```bash
# Проверить что токены приходят
docker logs docker-spread-collector-1 2>&1 | grep "tokens="

# Проверить анализ
docker logs docker-spread-collector-1 2>&1 | grep "analyze"

# Проверить фильтры
# В коде: filters.get_level(spread_percent, profit)
```

**Решение:**
- Снизить пороги в `filters.py` (для тестирования)
- Проверить что chains доступны (deposit/withdraw enabled)
- Проверить withdrawal_fees (не слишком большие)

### Проблема: Воркеры не получают сообщения

**Причина**: Consumer group не настроен или все messages ACKed

**Диагностика:**
```bash
# Проверить consumer group
redis-cli XINFO GROUPS orderbooks:updates

# Проверить pending messages
redis-cli XPENDING orderbooks:updates spread_collectors
```

**Решение:**
```bash
# Пересоздать consumer group
redis-cli XGROUP DESTROY orderbooks:updates spread_collectors
redis-cli XGROUP CREATE orderbooks:updates spread_collectors 0 MKSTREAM

# Перезапустить воркеры
docker-compose -f docker/dev.yml restart spread-collector
```

### Проблема: Duplicate сигналы

**Причина**: Несколько воркеров отправляют один и тот же сигнал

**Решение:**
- Использовать cache dedupe в `filters.py`:
```python
cache_key = f"arbitrage:{token_id}:{buy_from}:{sell_to}"

# Проверить предыдущее значение
previous = await cache.get(cache_key)
if previous and previous == current_value:
    return False  # Duplicate, skip

# Сохранить новое
await cache.set(cache_key, current_value, timeout_secs=60)
return True  # New signal
```

### Проблема: Медленный анализ

**Причина**: Большие батчи или много вычислений

**Решение:**
```python
# Уменьшить batch_size
batch_size = 25  # вместо 50

# Увеличить количество воркеров
scale: 5  # вместо 3 в docker-compose

# Disable depth_analyze для некоторых типов
analyze_same_type("spot", spot_sources, depth_analyze=False)
```

## Интеграция с другими сервисами

### Spread Loader → Spread Collectors
```
Spread Loader публикует changed tokens в Redis Stream
    ↓
Spread Collectors читают через Consumer Groups
    ↓
Каждый воркер обрабатывает свою часть сообщений
```

### Spread Collectors → Telegram Bot
```
Spread Collectors вычисляют арбитражные возможности
    ↓
Формируют сигналы с уровнем (LOW/MEDIUM/HIGH/VERY_HIGH)
    ↓
Отправляют в Telegram Bot через messenger
    ↓
Bot отправляет в соответствующие каналы
```

## Advanced Features

### Depth Analysis

**Когда используется:**
- Spot-Spot арбитраж
- CEX-DEX арбитраж

**Что делает:**
- Анализирует глубину orderbook на нескольких уровнях
- Вычисляет средневзвешенную цену для разных объемов
- Проверяет ликвидность

```python
def depth_analyze(orderbook, volumes=[100, 500, 1000]):
    results = []

    for volume_usdt in volumes:
        avg_price = calculate_avg_price(orderbook, volume_usdt)
        liquidity = check_liquidity(orderbook, volume_usdt)

        results.append({
            "volume": volume_usdt,
            "avg_price": avg_price,
            "liquidity": liquidity
        })

    return results
```

### Chain Compatibility Check

**Проверяет:**
- Общие chains между buy/sell биржами
- Deposit/withdrawal enabled
- Withdrawal fees

```python
def check_chain_compatibility(seller_chains, buyer_chains):
    compatible_chains = []

    for s in seller_chains:
        for b in buyer_chains:
            if s["chain_name"].lower() == b["chain_name"].lower():
                if s.get("withdraw_enabled") and b.get("deposit_enabled"):
                    compatible_chains.append({
                        "chain": s["chain_name"],
                        "fee": s.get("withdrawal_fee", 0)
                    })

    return compatible_chains
```

## Расширение функционала

### Добавление нового типа арбитража

```python
# В analyzer.py
def analyze_dex_dex(dex1_sources, dex2_sources):
    """Арбитраж между DEX-DEX (e.g., Uniswap vs PancakeSwap)"""

    results = []

    for symbol in set(dex1_sources.keys()) & set(dex2_sources.keys()):
        # Calculate spread учитывая gas fees и slippage
        spread = calculate_dex_spread(
            dex1_sources[symbol],
            dex2_sources[symbol]
        )

        if spread["profit"] > MIN_DEX_PROFIT:
            results.append(spread)

    return results
```

### Добавление ML predictions

```python
# Предсказывать движение цены для временного арбитража
def predict_price_movement(token, historical_data):
    # ML model inference
    prediction = model.predict(historical_data)

    # Если predicted spread > current spread
    if prediction["future_spread"] > current_spread * 1.5:
        # Higher confidence signal
        return SignalLevelType.VERY_HIGH

    return SignalLevelType.LOW
```

### Добавление риск-менеджмента

```python
def calculate_risk_score(signal):
    risk_factors = {
        "low_liquidity": 0.3,
        "high_volatility": 0.2,
        "chain_congestion": 0.2,
        "counterparty_risk": 0.3
    }

    risk_score = sum(
        factor * weight
        for factor, weight in risk_factors.items()
    )

    # Adjust signal level based on risk
    if risk_score > 0.7:
        signal["level"] = SignalLevelType.LOW
    elif risk_score < 0.3:
        signal["level"] = SignalLevelType.HIGH

    return signal
```

## Best Practices

1. **Scale workers based on load** - 3-5 воркеров обычно достаточно
2. **Monitor pending messages** - Должно быть ~0, иначе воркеры перегружены
3. **Tune filter thresholds** - Баланс между количеством и качеством сигналов
4. **Cache deduplication** - Избежать duplicate сигналов
5. **Non-blocking send** - Не блокировать анализ на отправке
6. **Batch processing** - Обрабатывать токены батчами для efficiency
7. **Enable profiling in dev** - Понять bottlenecks

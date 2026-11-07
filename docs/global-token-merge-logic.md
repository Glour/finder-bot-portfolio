# Логика объединения токенов в Global Tokens

**Дата актуализации:** 2026-02-15  
**Цель:** Объединить одинаковые токены с разных бирж в единый Global Token (GT) для корректного расчёта арбитражных спредов.

---

## Оглавление

1. [Архитектура процесса](#архитектура-процесса)
2. [Типы объединений](#типы-объединений)
3. [Каскад проверок и валидаций](#каскад-проверок-и-валидаций)
4. [Специальные правила для chainless токенов](#специальные-правила-для-chainless-токенов)
5. [Особенности бирж](#особенности-бирж)
6. [Примеры объединений](#примеры-объединений)
7. [Частые ошибки и их решения](#частые-ошибки-и-их-решения)

---

## Архитектура процесса

### Участники

1. **Token-collector** — собирает токены с бирж, создаёт `tokens` + `token_chains`
2. **Merge_global_token** — объединяет токены в `global_tokens`, создаёт связи через `token_chains.global_token_id`
3. **Price-distributor** — создаёт `global_token_sources` для маршрутизации цен
4. **Spread-collector** — использует GT для поиска арбитража

### Поток данных

```
Биржа → Token-collector → tokens + token_chains (global_token_id = NULL)
                              ↓
                    Merge_global_token
                              ↓
                    global_tokens + token_chains (global_token_id заполнен)
                              ↓
                    Price-distributor → global_token_sources
                              ↓
                    Price-saver → token_prices (с global_token_id)
                              ↓
                    Spread-collector → сигналы арбитража
```

---

## Типы объединений

### 1. Same Symbol (по символу)

**Условие:** `token.base_asset` совпадают (case-insensitive)

**Пример:**
- MEXC: `BTCUSDT` (base_asset=BTC)
- Gate: `BTC_USDT` (base_asset=BTC)
- → Объединяются в один GT с symbol=BTC

**Где происходит:**
```python
# merge_global_token.py, строка ~643
by_base = {}
for gt in global_tokens:
    for tc in gt.token_on_chains:
        base = (tok.base_asset or "").upper()
        by_base.setdefault(base, set()).add(gt.id)
```

---

### 2. Same Contract (по контракту)

**Условие:** `chain.name` + `token_chains.contract_address` совпадают (case-insensitive)

**Пример:**
- MEXC: BSC `0xabc...` (symbol=BOB)
- Gate: BSC `0xABC...` (symbol=BUILDONBOB)
- → Объединяются в один GT (один контракт = один токен)

**Где происходит:**
```python
# merge_global_token.py, строка ~657
seen_contracts = {}
for gt in global_tokens:
    for tc in gt.token_on_chains:
        chain_name = (tc.chain.name or "").lower()
        addr = (tc.contract_address or "").lower()
        if chain_name and addr:
            seen_contracts.setdefault((chain_name, addr), set()).add(gt.id)
```

---

## Каскад проверок и валидаций

Merge происходит **ТОЛЬКО если пройдены ВСЕ проверки:**

### 1. ✅ Наличие цен

```python
price_a, price_b = get_price(a_id), get_price(b_id)
if price_a is None or price_b is None:
    continue  # REJECT: нет цен
```

---

### 2. ✅ Проверка base_asset (для same_symbol без контракта)

**Для токенов БЕЗ контракта** (chainless или same_symbol без same_contract):

```python
if 'same_contract' not in reasons:
    bases_a = self._get_base_assets(gt_a)
    bases_b = self._get_base_assets(gt_b)
    if bases_a and bases_b and not (bases_a & bases_b):
        # Разные base_asset → нужна проверка
```

**Для chainless токенов:**
```python
if self._is_chainless_only(gt_a) and self._is_chainless_only(gt_b):
    diff_pct = abs(price_a - price_b) / ((price_a + price_b) / 2) * 100
    if diff_pct > 3%:
        REJECT  # Разные токены (цены далеки)
    else:
        ALLOW   # Легитимный алиас (цены близки)
```

**Для токенов с контрактом:**
```python
REJECT  # Строгая проверка: разные base_asset = разные токены
```

---

### 3. ✅ Multiplied Token Conflict (умноженные токены)

**Проблема:** `MOG` vs `1000000MOG` — разные токены, но могут иметь один контракт на некоторых биржах.

**Проверка:**
```python
# Проверяем префиксы: 1000X, 1000000X, KILO, MEGA
for prefix in ["1000X", "1000000X", "KILO", "MEGA"]:
    if base_a == f"{prefix}{base_b}":
        REJECT  # Multiplied token conflict
```

**Где:** `merge_global_token.py`, строка ~693

---

### 4. ✅ Contract Conflict (конфликт контрактов)

**Проблема:** Два GT имеют **ОДНУ цепь, но РАЗНЫЕ контракты** → это разные токены!

**Пример:**
- GT_A: BSC `0xaaa...`
- GT_B: BSC `0xbbb...`
- → REJECT (ASTRO на MEXC vs ASTRO на XT)

**Проверка:**
```python
def _has_contract_conflict(gt_a, gt_b):
    contracts_a = {(chain, addr) for tc in gt_a if tc.contract_address}
    contracts_b = {(chain, addr) for tc in gt_b if tc.contract_address}
    
    # Общие цепи
    chains_a = {chain for chain, _ in contracts_a}
    chains_b = {chain for chain, _ in contracts_b}
    common_chains = chains_a & chains_b
    
    for chain in common_chains:
        addrs_a = {addr for c, addr in contracts_a if c == chain}
        addrs_b = {addr for c, addr in contracts_b if c == chain}
        if addrs_a and addrs_b and not (addrs_a & addrs_b):
            return True  # CONFLICT: разные контракты на одной цепи
```

**Где:** `merge_global_token.py`, строка ~707

---

### 5. ✅ Price Ratio Check (для same_contract)

**Проблема:** Один контракт, но **огромная разница цен** → ошибка decimals или данных.

**Проверка:**
```python
if "same_contract" in reasons:
    ratio = max(price_a, price_b) / min(price_a, price_b)
    if ratio > 10:  # max_contract_price_ratio
        REJECT  # Цены слишком разные
```

**Где:** `merge_global_token.py`, строка ~878

**Примеры проблем:**
- TOMI: $0.0000000000314 vs $0.0000416 (ratio 1470249x) ❌
- GAS: $0.0000707 vs $1.639 (ratio 23182x) ❌

---

### 6. ✅ Chainless Tokens on Same Chain (FIX #6)

**Дата:** 2026-02-16  
**Проблема:** Chainless токены (NULL contract) на одной цепи объединяются даже с **разными base_asset**.

**Пример:**
- GT_A: AscendEX ethereum (EIGEN, NULL contract)
- GT_B: AscendEX ethereum (1INCH, NULL contract)
- → Merge разрешён (оба NULL = "совпадают") ❌
- → Результат: фантомные спреды EIGEN 118%

**Корень проблемы:**
Chainless токены объединялись в `assign_global_ids` через `filtered_map` БЕЗ проверки base_asset:
```python
is_chained_token = chain and addr  # chainless: chain="ethereum", addr=None → False
# FIX #5 проверки пропускались для chainless токенов!
```

**Решение (2 части):**

**1. В `assign_global_ids` (строка ~1220):**
```python
# FIX #6: Chainless tokens base_asset validation
is_chainless_token = chain and not addr  # has chain but NO contract

if is_chainless_token:
    # Get existing chainless bases on same chain
    existing_chainless_bases = ...
    
    if new_base_upper not in existing_chainless_bases:
        # Different base_asset → CONTAMINATION!
        continue  # Skip this GT, don't add to filtered_map
```

**2. В `_has_chain_conflict` (строка ~1407):**
```python
# Fallback protection during merge
if contract_a is None and contract_b is None:
    bases_a = await self._get_base_assets_for_gt(gt_a_id)
    bases_b = await self._get_base_assets_for_gt(gt_b_id)
    
    if bases_a and bases_b and not (bases_a & bases_b):
        return True  # CONFLICT: chainless with different base_asset
```

**Где:** `merge_global_token.py`, строки ~1220 (основная защита) и ~1407 (fallback)

**Токены затронуты:**
- EIGEN/1INCH/CHR/PTC/TORA (AscendEX) — 10 контаминированных GT после первого деплоя
- ARKM/COW/EURR/FIGHT/NOM/NUT/ZETA (7 токенов в одном GT)

---

### 7. ✅ Chainless SPOT+FUTURES Separation (FIX #7)

**Дата:** 2026-02-16  
**Проблема:** Chainless SPOT и FUTURES токены с **одной биржи** объединяются в один GT, создавая фантомные спреды.

**Примеры:**
- **GAS** (MEXC): spot $0.0000765 vs futures $1.65 → spread 2,160,684%
- **ZKP** (Bitmart): spot vs futures → spread 880%
- Это **разные активы** с одинаковым тикером!

**Корень проблемы:**
Старая логика разрешала SPOT+FUTURES mix для токенов с одной биржи:
```python
only_current_source = (len(existing_sources) == 1 and source_id in existing_sources)
# ALLOW SPOT+FUTURES mix (same exchange) ❌
```

Но для chainless токенов **нет контракта** чтобы проверить что это один и тот же актив!

**Решение (FIX #7):**
```python
# FIX #7: Check if this is chainless token
is_chainless_token = chain and not addr  # has chain but NO contract

if is_chainless_token:
    # Chainless spot and futures are DIFFERENT assets
    if (has_spot_only and adding_futures) or (has_futures_only and adding_spot):
        continue  # Skip this GT, don't add to filtered_map
```

**Где:** `merge_global_token.py`, строка ~1160

**Результат:**
- Chainless SPOT и FUTURES **НЕ объединяются** даже на одной бирже
- Контракты проверяются для chained токенов (можно объединять если контракт совпадает)
- Phantom spreads GAS/ZKP **не создаются**

---

### 7. ✅ Chain Conflict Prevention (FIX #4)

**Проблема:** Merge создаёт GT с **несколькими контрактами на ОДНОЙ цепи**.

**Пример:**
- GT после merge: BSC `0xaaa...` + BSC `0xbbb...` ❌

**Проверка:**
```python
def _has_chain_conflict(gt_a, gt_b):
    # Считаем контракты на каждой цепи после merge
    contracts_per_chain = defaultdict(set)
    for tc in (gt_a.token_on_chains + gt_b.token_on_chains):
        chain = (tc.chain.name or "").lower()
        addr = (tc.contract_address or "").lower()
        if chain and addr:
            contracts_per_chain[chain].add(addr)
    
    # Если хотя бы одна цепь имеет >1 контракта → CONFLICT
    return any(len(addrs) > 1 for addrs in contracts_per_chain.values())
```

**Где:** `merge_global_token.py`, строка ~793

---

## Специальные правила для chainless токенов

### Что такое chainless токены?

**Chainless токены** — токены **БЕЗ реального контракта** на блокчейне:
- Пустой `contract_address` или NULL
- Биржи: **Hyperliquid, AscendEX, LBank, Bitmart, AsterDEX**
- Chains: **hyperliquid, ascendex** (synthetic addresses)

**Проблема:** Chainless токены НЕ МОГУТ быть проверены по контракту → нужны дополнительные валидации.

---

### Правила для chainless

#### 1. Определение chainless GT

```python
def _is_chainless_only(gt):
    """
    Chainless GT = ВСЕ token_chains имеют chainless chains
    """
    chainless_chains = {"hyperliquid", "ascendex"}
    
    for tc in gt.token_on_chains:
        chain_name = (tc.chain.name or "").lower()
        # Если хотя бы один token_chain из REAL chain → НЕ chainless
        if chain_name and chain_name not in chainless_chains:
            return False
    
    return True  # Все chainless
```

---

#### 2. Chainless + разные base_asset → проверка цен (3%)

**Проблема:** 
- TOON (AscendEX) + NOM (LBank) — оба chainless ETH, но **РАЗНЫЕ токены**
- BLUECHIP (AscendEX) + VIRUS (LBank) — оба chainless SOL, но **РАЗНЫЕ токены**

**Решение:**
```python
if 'same_contract' not in reasons:
    bases_a = self._get_base_assets(gt_a)
    bases_b = self._get_base_assets(gt_b)
    
    if bases_a and bases_b and not (bases_a & bases_b):
        # Разные base_asset
        
        if self._is_chainless_only(gt_a) and self._is_chainless_only(gt_b):
            # Chainless: проверяем цены
            diff_pct = abs(price_a - price_b) / ((price_a + price_b) / 2) * 100
            
            if diff_pct > 3:  # max_price_diff_pct
                REJECT  # TOON ($0.0000173) vs NOM ($0.00537) = diff 194% >> 3%
            else:
                ALLOW   # Легитимный алиас (цены близки <3%)
        else:
            # Не chainless: строгая проверка
            REJECT
```

**Где:** `merge_global_token.py`, строка ~846-879

---

#### 3. Chainless + same_symbol + проверка цен

**Дополнительная проверка для chainless same_symbol** (даже с одинаковым base_asset):

```python
if "same_symbol" in reasons and "same_contract" not in reasons:
    if self._is_chainless_only(gt_a) and self._is_chainless_only(gt_b):
        diff_pct = abs(price_a - price_b) / ((price_a + price_b) / 2) * 100
        if diff_pct > 3:  # max_price_diff_pct
            REJECT  # MEXC SIX ($0.01) vs Hyperliquid SIX ($0.67)
```

**Где:** `merge_global_token.py`, строка ~887-899

---

## Особенности бирж

### Chainless биржи

**Hyperliquid:**
- Chain: `hyperliquid`
- Contract: synthetic (не проверяется)
- Требуется: base_asset match + price match (3%)

**AscendEX:**
- Chain: `ascendex` для chainless
- Chain: `ethereum`, `solana` и т.д. для контрактных
- Смешанная биржа!

**LBank, Bitmart, AsterDEX:**
- Часто БЕЗ contract_address
- Требуется: base_asset match + price match (3%)

---

### Contract Size (FUTURES)

**MEXC, Gate, XT, LBank (FUTURES):**
- Поле `token.contract_size` (обычно 1, 10, 100)
- Price нормализуется через contract_size в price-saver
- Пример: GAS FUTURES contract_size=1 → цена $1.639

---

### Symbol Mapping

**MEXC:**
- SPOT: `BTCUSDT`
- FUTURES: `BTC_USDT`
- base_asset: `BTC` (одинаковый)

**Gate:**
- SPOT/FUTURES: `BTC_USDT`

**XT:**
- SPOT/FUTURES: `BTC_USDT`

**LBank:**
- SPOT: `BTC_USDT`
- FUTURES: `BTCUSDT`

---

## Примеры объединений

### Пример 1: Легитимное объединение (same_contract)

**Токены:**
- MEXC: BSC `0x8457CA5040ad67fdebbCC8EdCE889A335Bc0fbFB`, symbol=`ALTUSDT`, base_asset=`ALT`, price=$0.00855
- Gate: BSC `0x8457CA5040ad67fdebbCC8EdCE889A335Bc0fbFB`, symbol=`ALT_USDT`, base_asset=`ALT`, price=$0.00855

**Проверки:**
1. ✅ same_contract (BSC + один контракт)
2. ✅ price_ratio = 1.0 (<10)
3. ✅ Нет chain conflict
4. ✅ Нет multiplied conflict

**Результат:** Объединяются в один GT (symbol=`ALT`)

---

### Пример 2: Легитимное объединение chainless (алиас)

**Токены:**
- AscendEX: SOL chainless, symbol=`BOB/USDT`, base_asset=`BOB`, price=$1.00
- LBank: SOL chainless, symbol=`BUILDONBOB_USDT`, base_asset=`BUILDONBOB`, price=$0.99

**Проверки:**
1. ✅ same_symbol? Нет (BOB ≠ BUILDONBOB)
2. ✅ Chainless both? Да
3. ✅ base_asset разные? Да (BOB vs BUILDONBOB)
4. ✅ Price diff = |1.00 - 0.99| / 0.995 * 100 = **1.0%** < 3% ✅

**Результат:** Объединяются (легитимный алиас)

---

### Пример 3: REJECT chainless (разные токены)

**Токены:**
- AscendEX: ETH chainless, symbol=`TOON/USDT`, base_asset=`TOON`, price=$0.0000173
- LBank: ETH chainless, symbol=`NOM_USDT`, base_asset=`NOM`, price=$0.00537

**Проверки:**
1. ✅ same_symbol? Нет
2. ✅ same_contract? Нет (оба chainless)
3. ✅ Chainless both? Да
4. ✅ base_asset разные? Да (TOON ≠ NOM)
5. ❌ Price diff = |0.0000173 - 0.00537| / 0.00269 * 100 = **199.6%** >> 3% ❌

**Результат:** НЕ объединяются (разные токены)

---

### Пример 4: REJECT contract conflict

**Токены:**
- MEXC: BSC `0xaaa...`, symbol=`ASTRO`, base_asset=`ASTRO`, price=$0.01
- XT: BSC `0xbbb...`, symbol=`ASTRO`, base_asset=`ASTRO`, price=$0.01

**Проверки:**
1. ✅ same_symbol (ASTRO)
2. ❌ same_contract? Нет (разные контракты на BSC)
3. ❌ Contract conflict? **ДА** (одна цепь, разные контракты) ❌

**Результат:** НЕ объединяются (contract conflict)

---

### Пример 5: REJECT multiplied token

**Токены:**
- MEXC: BSC `0xabc...`, symbol=`MOGUSDT`, base_asset=`MOG`, price=$0.0000001
- Gate: BSC `0xabc...`, symbol=`1000000MOG_USDT`, base_asset=`1000000MOG`, price=$0.10

**Проверки:**
1. ✅ same_contract (BSC + один контракт)
2. ❌ Multiplied conflict? **ДА** (1000000MOG содержит префикс 1000000 + MOG) ❌

**Результат:** НЕ объединяются (multiplied token)

---

### Пример 6: REJECT price ratio (decimals error)

**Токены:**
- Bitmart: ETH `0x4385...`, symbol=`TOMI_USDT`, base_asset=`TOMI`, price=$0.0000000000314
- Gate: ETH `0x4385...`, symbol=`TOMI_USDT`, base_asset=`TOMI`, price=$0.0000416

**Проверки:**
1. ✅ same_contract (ETH + один контракт)
2. ✅ same_symbol (TOMI)
3. ❌ Price ratio = 0.0000416 / 0.0000000000314 = **1470249** >> 10 ❌

**Результат:** НЕ объединяются (ошибка decimals на Bitmart)

---

## Частые ошибки и их решения

### Ошибка 1: Chainless контаминация (ИСПРАВЛЕНО 2026-02-15)

**Проблема:**
- GT содержит chainless токены с **разными base_asset**
- Пример: TOON + NOM + ME в одном GT

**Причина:**
- Транзитивное объединение через промежуточный токен ME
- ME объединился с TOON (same_symbol)
- ME объединился с NOM (same_symbol)
- ME удалён → TOON + NOM в одном GT

**Решение:**
- Добавлена проверка base_asset + price (3%) для chainless (строка 846-879)
- Удалено 54 контаминированных GT
- Новые контаминации не появляются

---

### Ошибка 2: Same contract с разными base_asset (ИСПРАВЛЕНО 2026-02-15)

**Проблема:**
- Токен 114514 ($0.0000000335) + токен 4 ($0.0101) — один контракт BSC
- MEXC неправильно маппит symbol 114514 на контракт токена 4

**Решение:**
- Разделены в разные GT вручную
- GT=2cfb6453: base_asset="4"
- GT=f78356bd: base_asset="114514"

**Вывод:** Ошибка данных биржи, разделение вручную корректно.

---

### Ошибка 3: Decimals/нормализация (НЕ РЕШЕНА)

**Проблема:**
- TOMI: Bitmart $0.0000000000314 vs Gate $0.0000416 (ratio 1470249x)
- GAS: MEXC SPOT $0.0000707 vs FUTURES $1.639 (ratio 23182x)

**Причина:**
- Price-collectors получают цены БЕЗ нормализации по decimals
- Ошибки данных на биржах

**Временное решение:**
- Добавлены в blacklist (TOMI, GAS)

**Долгосрочное решение (TODO):**
- Добавить фильтр в spread-collector: reject price_ratio > 100x
- Добавить валидацию decimals в price-collector

---

### Ошибка 4: Chain conflict (ИСПРАВЛЕНО FIX #4)

**Проблема:**
- GT содержит несколько контрактов на ОДНОЙ цепи
- Пример: BSC `0xaaa...` + BSC `0xbbb...` в одном GT

**Решение:**
- Добавлена проверка `_has_chain_conflict` (строка 793)
- Merge отклоняется если создаёт >1 контракта на одной цепи

---

## Конфигурация (merge_global_token.py)

```python
@dataclass
class MergeConfig:
    # Максимальная разница цен для chainless same_symbol (3%)
    max_price_diff_pct: float = 3.0
    
    # Максимальный price ratio для same_contract (10x)
    max_contract_price_ratio: float = 10.0
    
    # Префиксы умноженных токенов
    multiplied_token_prefixes: list[str] = field(
        default_factory=lambda: ["1000X", "1000000X", "KILO", "MEGA"]
    )
```

---

## Мониторинг и отладка

### Проверка контаминаций

```sql
-- Найти GT с разными base_asset
SELECT 
  gt.id,
  gt.symbol,
  COUNT(DISTINCT t.base_asset) as unique_base_assets,
  string_agg(DISTINCT t.base_asset, ', ') as base_assets
FROM global_tokens gt
JOIN token_chains tc ON tc.global_token_id = gt.id
JOIN tokens t ON t.id = tc.token_id
GROUP BY gt.id, gt.symbol
HAVING COUNT(DISTINCT t.base_asset) > 1
ORDER BY unique_base_assets DESC;
```

### Проверка chainless контаминаций

```sql
-- Chainless GT с разными base_asset
WITH chainless_gt AS (
  SELECT DISTINCT gt.id
  FROM global_tokens gt
  JOIN token_chains tc ON tc.global_token_id = gt.id
  WHERE (tc.contract_address IS NULL OR tc.contract_address = '')
  GROUP BY gt.id
  HAVING COUNT(DISTINCT t.base_asset) > 1
)
SELECT * FROM chainless_gt;
```

### Проверка price ratio проблем

```sql
-- GT с огромной разницей цен
WITH gt_prices AS (
  SELECT 
    tp.global_token_id,
    MIN((tp.orderbook->'asks'->0->>0)::numeric) as min_price,
    MAX((tp.orderbook->'asks'->0->>0)::numeric) as max_price
  FROM token_prices tp
  WHERE tp.orderbook IS NOT NULL
  GROUP BY tp.global_token_id
)
SELECT 
  gt.symbol,
  gp.min_price,
  gp.max_price,
  ROUND(gp.max_price / gp.min_price, 2) as price_ratio
FROM gt_prices gp
JOIN global_tokens gt ON gt.id = gp.global_token_id
WHERE gp.max_price / gp.min_price > 100
ORDER BY price_ratio DESC;
```

---

## Итоговая схема проверок

```
Найден кандидат для merge (same_symbol или same_contract)
        ↓
    [1] Есть цены? → Нет → REJECT
        ↓ Да
    [2] base_asset разные? → Нет → OK
        ↓ Да
    [3] Chainless оба? → Нет → REJECT (strict)
        ↓ Да
    [4] Price diff < 3%? → Нет → REJECT
        ↓ Да (алиас)
    [5] Multiplied conflict? → Да → REJECT
        ↓ Нет
    [6] Contract conflict? → Да → REJECT
        ↓ Нет
    [7] Chain conflict? → Да → REJECT
        ↓ Нет
    [8] Price ratio < 10x? → Нет → REJECT
        ↓ Да
    ✅ MERGE ALLOWED
```

---

## Контакты и поддержка

- Файл логики: `backend/infrastructure/services/collectors/token_collector/supports/merge_global_token.py`
- Документация: `docs/global-token-merge-logic.md`
- История изменений: `memory/2026-02-15.md`

**Последнее обновление:** 2026-02-15 (исправление chainless контаминации)

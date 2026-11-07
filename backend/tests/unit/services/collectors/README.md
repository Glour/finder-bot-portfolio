# Тестовые данные для мерджа глобальных токенов

## Обзор сценариев

| # | Сценарий | Результат | Причина |
|---|----------|-----------|---------|
| 1 | Разные base_asset, одинаковый chain+contract | MERGE (silent) | same_contract |
| 2 | Одинаковые base_asset, diff <= 1% | MERGE (silent) | same_symbol |
| 3 | Одинаковые base_asset, 1% < diff <= 3% | NOTIFY ADMIN | same_symbol |
| 4 | Одинаковые base_asset, конфликт контрактов | NO MERGE | contract_conflict |
| 5 | Одинаковые base_asset, diff > 3% | NO MERGE | price_diff_too_large |
| 6 | Одинаковые base_asset, конфликт бирж | NO MERGE | exchange_conflict |
| 7 | Пустые контракты (не конфликт) | MERGE (silent) | same_symbol |
| 8 | same_contract + same_symbol | MERGE (silent) | same_contract (приоритет) |

## Определения

### Конфликт контрактов
У разных глобалов есть пересекающиеся сети, но контракты **разные** (пустые контракты за конфликт **не** считаются).

```
Global A: PEPE на ethereum с контрактом 0x111...
Global B: PEPE на ethereum с контрактом 0x222...
→ КОНФЛИКТ (одна сеть, разные контракты)
```

### Конфликт бирж
У разных глобалов есть пересекающиеся биржи.

```
Global A: BTC на Binance, OKX
Global B: BTC на Binance, Gate  (Binance пересекается!)
→ КОНФЛИКТ (одна биржа в обоих глобалах)
```

## Запуск тестов

```bash
poetry run python -m pytest backend/tests/unit/services/collectors/test_merge_global_token.py -v
```

## Просмотр всех сценариев

```bash
PYTHONPATH=$PYTHONPATH:backend poetry run python backend/tests/unit/services/collectors/test_merge_global_token.py
```

## Структура тестовых данных

```python
MergeScenarios()
  ├── scenario_1_merge_same_contract()      # MERGE - same contract
  ├── scenario_2_merge_same_symbol_small_diff()  # MERGE - same symbol, <1%
  ├── scenario_3_notify_same_symbol_medium_diff() # NOTIFY - 1-3%
  ├── scenario_4_no_merge_contract_conflict()  # NO MERGE - contract conflict
  ├── scenario_5_no_merge_large_price_diff()   # NO MERGE - >3%
  ├── scenario_6_no_merge_exchange_conflict()  # NO MERGE - exchange conflict
  ├── scenario_7_merge_empty_contract_no_conflict() # MERGE - empty = no conflict
  └── scenario_8_merge_priority_contract_over_symbol() # MERGE - priority
```

## Пороговые значения (из MergeConfig)

| Параметр | Значение | Описание |
|----------|----------|----------|
| max_price_diff_pct | 3.0% | Макс. разница цен для мерджа |
| auto_approve_silent_threshold | 1.0% | Тихий автомердж если diff <= |
| auto_approve_price_diff_threshold | 3.0% | Уведомить если diff > |

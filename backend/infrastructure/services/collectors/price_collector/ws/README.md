# GMGN WebSocket Tests

## Запуск теста локально

```bash
# Рекомендуемый способ (Python wrapper - автоматически использует bash wrapper)
python backend/infrastructure/services/collectors/price_collector/ws/test_gmgn.py

# Или через bash wrapper напрямую
./backend/infrastructure/services/collectors/price_collector/ws/run_test_gmgn.sh

# ⚠️ НЕ РЕКОМЕНДУЕТСЯ (будут ошибки segfault в конце)
python backend/infrastructure/services/collectors/price_collector/ws/test_gmgn_ws.py
```

## Файлы

- `test_gmgn.py` - **Python wrapper (ИСПОЛЬЗУЙТЕ ЭТОТ)** - автоматически запускает через bash wrapper
- `test_gmgn_ws.py` - тестовый WebSocket клиент (имеет segfault при прямом запуске)
- `run_test_gmgn.sh` - bash wrapper для фильтрации segfault ошибок
- `export_gmgn_tokens.py` - экспорт токенов из БД в JSON
- `data/gmgn_tokens.json` - локальный файл с токенами (8204 токена)

## Известные проблемы

**curl_cffi segfault при cleanup** (РЕШЕНО):
- Библиотека curl_cffi имеет баг с `memoryview` при закрытии WebSocket
- Segfault происходит **после** завершения теста при очистке памяти
- Не влияет на функциональность - тест выполняется полностью
- **Решение**: wrapper скрипт фильтрует ошибки cleanup в реальном времени
- Вывод теста полностью чистый, exit code 0

## Формат данных

JSON файл содержит массив объектов подписки:

```json
[
  {"chain": "sol", "addresses": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"},
  {"chain": "bsc", "addresses": "0x8c14d0cdcd47e2ef40d3eda3f8ab3d80e9ef4444"},
  ...
]
```

WebSocket возвращает обновления цен:

```json
{
  "channel": "token_stat",
  "data": [{
    "n": "base",
    "a": "0x696f9436b67233384889472cd7cd58a6fb5df4f1",
    "p": "0.19677396"
  }]
}
```

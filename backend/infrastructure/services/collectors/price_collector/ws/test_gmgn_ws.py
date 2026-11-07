"""Простой тест подключения к GMGN WebSocket с curl_cffi для обхода Cloudflare.

ВАЖНО: Этот скрипт имеет segfault при cleanup curl_cffi.
Используйте wrapper скрипт для чистого вывода:
    ./backend/infrastructure/services/collectors/price_collector/ws/run_test_gmgn.sh

Или запускайте напрямую (будут ошибки segfault в конце):
    python backend/infrastructure/services/collectors/price_collector/ws/test_gmgn_ws.py
"""

import asyncio
import json
import os
import time
import uuid
from urllib.parse import urlencode

from curl_cffi.requests import AsyncSession


def load_tokens_from_json() -> list[dict]:
    """Load GMGN tokens from exported JSON file.

    Returns:
        List of dicts: [{"chain": "sol", "addresses": "addr"}, ...]
    """
    from collections import defaultdict
    from pathlib import Path

    # Try local path first, then container path
    # From backend/infrastructure/services/collectors/price_collector/ws/ go up 7 levels to project root
    project_root = Path(__file__).parent.parent.parent.parent.parent.parent.parent
    local_json = project_root / "data" / "gmgn_tokens.json"
    container_json = Path("/app/data/gmgn_tokens.json")

    json_file = str(local_json) if local_json.exists() else str(container_json)

    # Fallback to test data if file doesn't exist
    if not os.path.exists(json_file):
        print(f"⚠️  {json_file} not found, using test tokens")
        return [
            {"chain": "sol", "addresses": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"},
            {"chain": "bsc", "addresses": "0x8c14d0cdcd47e2ef40d3eda3f8ab3d80e9ef4444"},
            {"chain": "base", "addresses": "0xdb5a42b31f21773cbe5dda0b3edd88c859678b07"},
            {"chain": "eth", "addresses": "0x514910771af9ca656af840dff83e8264ecf986ca"},
            {"chain": "tron", "addresses": "TEyzUNwZMuMsAXqdcz5HZrshs3iWfydGAW"},
        ]

    with open(json_file, "r") as f:
        tokens = json.load(f)

    # Count tokens per chain
    chain_counts = defaultdict(int)
    for token in tokens:
        chain_counts[token["chain"]] += 1

    print(f"📦 Loaded from {json_file}:")
    for chain, count in sorted(chain_counts.items()):
        print(f"  {chain}: {count} tokens")

    print(f"\n✅ Total loaded: {len(tokens)} tokens from {len(chain_counts)} chains\n")

    return tokens


async def test_gmgn():
    """Тестовое подключение к GMGN WebSocket через curl_cffi."""

    # Время работы теста (можно задать через переменную окружения)
    test_duration = int(os.environ.get('GMGN_TEST_DURATION', '15'))

    # Параметры URL
    params = {
        "device_id": str(uuid.uuid4()),
        "fp_did": uuid.uuid4().hex,
        "client_id": f"gmgn_web_20260202-10623-98faccb",
        "from_app": "gmgn",
        "app_ver": "20260202-10623-98faccb",
        "tz_name": "Europe/Paris",
        "tz_offset": "3600",
        "app_lang": "en-US",
        "os": "web",
        "worker": "0",
        "uuid": uuid.uuid4().hex[:16],
        "reconnect": "0",
    }

    url = f"wss://gmgn.ai/ws?{urlencode(params)}"

    # Заголовки (как в браузере)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 OPR/126.0.0.0",
        "Origin": "https://gmgn.ai",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    print("🔌 Подключаемся к GMGN WebSocket через curl_cffi...")
    print(f"URL: wss://gmgn.ai/ws?...")

    # Load tokens from JSON file
    print("\n📚 Загружаем токены из JSON файла...")
    db_tokens = load_tokens_from_json()

    try:
        session = AsyncSession(impersonate="chrome124")
        ws = await session.ws_connect(url, headers=headers)
        print("✅ Подключено!\n")

        # Подписка на каналы для SOL
        channels = ["new_pair_update", "token_stat"]

        for channel in channels:
            sub_id = uuid.uuid4().hex[:16]

            if channel == "token_stat":
                # token_stat - используем токены из JSON файла
                payload = {
                    "action": "subscribe",
                    "channel": channel,
                    "f": "w",
                    "id": sub_id,
                    "data": db_tokens,  # Все токены из JSON файла
                }
            else:
                # new_pair_update использует broadcast по chain
                payload = {
                    "action": "subscribe",
                    "channel": channel,
                    "f": "w",
                    "id": sub_id,
                    "data": [{"chain": "sol"}],
                }

            await ws.send(json.dumps(payload))
            print(f"📤 Подписались на канал: {channel}")

        print("\n" + "=" * 80)
        print(f"📨 Слушаем сообщения ({test_duration} секунд или Ctrl+C для остановки)...\n")

        # Слушаем сообщения
        count = 0
        start_time = time.time()

        while time.time() - start_time < test_duration:
            try:
                # ws.recv() с timeout для heartbeat
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                # Отправляем heartbeat
                hb = json.dumps({"action": "heartbeat", "client_ts": int(time.time() * 1000)})
                await ws.send(hb)
                print(f"💓 Heartbeat отправлен ({time.time() - start_time:.0f}s)")
                continue

            # curl_cffi возвращает (data, opcode) tuple
            if isinstance(raw, tuple):
                raw = raw[0]

            try:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")

                parsed = json.loads(raw)
                channel = parsed.get("channel", "unknown")

                # Пропускаем ack и heartbeat для чистоты вывода
                if channel in ("ack", "heartbeat"):
                    print(f"✓ {channel}")
                    continue

                count += 1
                data = parsed.get("data", [])

                print(f"\n{'=' * 80}")
                print(f"📬 Сообщение #{count} ({time.time() - start_time:.1f}s)")
                print(f"Канал: {channel}")
                print(f"Ключи: {list(parsed.keys())}")
                print(f"Данных в массиве: {len(data)}")

                if data and isinstance(data[0], dict):
                    print(f"\nПервые 3 поля data[0]:")
                    first_3 = dict(list(data[0].items())[:3])
                    print(json.dumps(first_3, indent=2))

            except Exception as e:
                print(f"\n❌ Ошибка парсинга: {e}")
                print(f"Сырое сообщение: {str(raw)[:200]}")

        print(f"\n⏱️  Время вышло ({test_duration}s). Всего сообщений с данными: {count}")
        print("\n✅ Тест завершён успешно")

        # Skip cleanup to avoid segfault - just exit immediately
        os._exit(0)

    except KeyboardInterrupt:
        print("\n\n⏹️  Остановлено")
        os._exit(0)
    except Exception as e:
        print(f"\n❌ Ошибка подключения: {e}")
        import traceback
        traceback.print_exc()
        os._exit(1)


if __name__ == "__main__":
    # Note: test_gmgn() calls os._exit() directly to avoid curl_cffi cleanup segfault
    asyncio.run(test_gmgn())

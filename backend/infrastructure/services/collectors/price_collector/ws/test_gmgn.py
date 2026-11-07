#!/usr/bin/env python3
"""Wrapper для запуска GMGN WebSocket теста без segfault ошибок.

Запуск:
    python backend/infrastructure/services/collectors/price_collector/ws/test_gmgn.py

С указанием времени работы (в секундах):
    python backend/infrastructure/services/collectors/price_collector/ws/test_gmgn.py 60
    python backend/infrastructure/services/collectors/price_collector/ws/test_gmgn.py 300  # 5 минут
"""

import subprocess
import sys
from pathlib import Path


def main():
    """Запускает test_gmgn_ws.py через bash wrapper для чистого вывода."""

    # Проверяем аргумент времени работы
    duration = 120  # По умолчанию 15 секунд
    if len(sys.argv) > 1:
        try:
            duration = int(sys.argv[1])
            if duration < 1:
                print("❌ Время должно быть больше 0 секунд")
                return 1
            print(f"⏱️  Тест будет работать {duration} секунд\n")
        except ValueError:
            print(f"❌ Неверный аргумент: {sys.argv[1]}")
            print("Использование: python test_gmgn.py [секунды]")
            print("Пример: python test_gmgn.py 60")
            return 1

    # Путь к bash wrapper скрипту
    script_dir = Path(__file__).parent
    wrapper_script = script_dir / "run_test_gmgn.sh"

    if not wrapper_script.exists():
        print(f"❌ Wrapper скрипт не найден: {wrapper_script}")
        print("Запускаем тест напрямую (будут ошибки segfault в конце)...")

        # Fallback к прямому запуску
        test_script = script_dir / "test_gmgn_ws.py"
        result = subprocess.run([sys.executable, str(test_script)])
        return result.returncode

    # Устанавливаем переменную окружения для времени работы
    import os
    env = os.environ.copy()
    env['GMGN_TEST_DURATION'] = str(duration)

    # Запускаем через bash wrapper с live output (без буферизации)
    process = subprocess.Popen(
        [str(wrapper_script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # Line buffered для real-time вывода
        universal_newlines=True,
        env=env
    )

    # Выводим строки в реальном времени
    try:
        for line in process.stdout:
            print(line, end='', flush=True)
    except KeyboardInterrupt:
        process.terminate()
        process.wait()
        return 130

    returncode = process.wait()
    return returncode


if __name__ == "__main__":
    sys.exit(main())

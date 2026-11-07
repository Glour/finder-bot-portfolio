"""Checks that the API can be imported without Telegram credentials."""

import os
from pathlib import Path
import subprocess
import sys


def test_api_import_and_routes_without_bot_tokens(tmp_path):
    backend = Path(__file__).resolve().parents[3]
    example = backend.parent / ".env.example"
    (tmp_path / ".env.prod").write_text(example.read_text())
    env = os.environ.copy()
    env["PYTHONPATH"] = str(backend)
    for name in list(env):
        if name.startswith(("BOT__", "POSTGRES__", "REDIS__")):
            env.pop(name)
    code = """
from api.main import app, get_version
from settings.db_settings import RedisSettings
from urllib.parse import unquote, urlsplit
paths = {route.path for route in app.routes}
assert {'/ping', '/version', '/api/v1/tokens/'} <= paths
assert get_version.__wrapped__() == {'version': '1.0.0'}
dsn = urlsplit(RedisSettings(redis_db=3, redis_password='test/@:pass').dsn)
assert dsn.path == '/3'
assert unquote(dsn.password) == 'test/@:pass'
"""
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=tmp_path, env=env,
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr

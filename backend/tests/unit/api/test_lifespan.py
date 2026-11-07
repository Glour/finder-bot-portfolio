"""Verify the API owns and releases its resources, including on startup failure."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize("failure", ["none", "limiter", "database", "clients", "body"])
def test_lifespan_cleanup(tmp_path, failure):
    backend = Path(__file__).resolve().parents[3]
    (tmp_path / ".env.prod").write_text((backend.parent / ".env.example").read_text())
    env = os.environ.copy()
    env["PYTHONPATH"] = str(backend)
    env["FAILURE"] = failure
    for name in list(env):
        if name.startswith(("BOT__", "POSTGRES__", "REDIS__")):
            env.pop(name)
    code = '''
import asyncio
import os
from unittest.mock import AsyncMock, Mock
import api.main as module

failure = os.environ['FAILURE']
redis = Mock()
redis.close = AsyncMock()
module.aioredis.from_url = Mock(return_value=redis)
module.FastAPICache.init = Mock()
module.FastAPICache.reset = Mock()
module.FastAPILimiter.init = AsyncMock(side_effect=RuntimeError('limiter') if failure == 'limiter' else None)
module.database.init_database = Mock(side_effect=RuntimeError('database') if failure == 'database' else None)
module.database.db_manager.close_database = AsyncMock()
module.init_clients = Mock(side_effect=RuntimeError('clients') if failure == 'clients' else None)
module.close_clients = AsyncMock()
module.load_proxies = Mock(return_value=[])

async def run():
    try:
        async with module.lifespan(module.app):
            assert failure in ('none', 'body')
            assert module.database.redis is redis
            if failure == 'body':
                raise RuntimeError('body')
    except RuntimeError as exc:
        assert str(exc) == failure
    else:
        assert failure == 'none'
    redis.close.assert_awaited_once()
    module.FastAPICache.reset.assert_called_once()
    assert module.database.redis is None
    assert module.database.db_manager.close_database.await_count == (failure != 'limiter')
    assert module.close_clients.await_count == (failure in ('none', 'clients', 'body'))

asyncio.run(run())
'''
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=tmp_path, env=env,
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr

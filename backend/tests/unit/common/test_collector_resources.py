"""Collectors release resources on normal exit, cancellation and failed startup."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize("failure", ["none", "clients", "database", "body", "cancel"])
def test_resource_cleanup(tmp_path, failure):
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
import infrastructure.services.common.collector_resources as module

failure = os.environ['FAILURE']
redis = Mock()
redis.close = AsyncMock()
module.aioredis.from_url = Mock(return_value=redis)
module.load_proxies = Mock(return_value=[])
module.init_clients = Mock(side_effect=RuntimeError('clients') if failure == 'clients' else None)
module.close_clients = AsyncMock()
module.database.init_database = Mock(side_effect=RuntimeError('database') if failure == 'database' else None)
module.database.db_manager.close_database = AsyncMock()

async def run():
    try:
        async with module.collector_resources():
            assert module.database.redis is redis
            if failure == 'body':
                raise RuntimeError('body')
            if failure == 'cancel':
                raise asyncio.CancelledError()
    except RuntimeError as exc:
        assert str(exc) == failure
    except asyncio.CancelledError:
        assert failure == 'cancel'
    else:
        assert failure == 'none'
    redis.close.assert_awaited_once()
    module.close_clients.assert_awaited_once()
    assert module.database.redis is None
    assert module.database.db_manager.close_database.await_count == (failure != 'clients')
    if failure != 'clients':
        assert module.database.async_session is None

asyncio.run(run())
'''
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=tmp_path, env=env,
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr

"""Optional API smoke test against explicitly configured disposable services."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.skipif(
    os.environ.get("FINDER_INTEGRATION") != "1",
    reason="Set FINDER_INTEGRATION=1 with disposable PostgreSQL and Redis settings",
)
def test_api_with_postgres_and_redis(tmp_path):
    backend = Path(__file__).resolve().parents[2]
    (tmp_path / ".env.prod").write_text((backend.parent / ".env.example").read_text())
    env = os.environ.copy()
    env["PYTHONPATH"] = str(backend)
    code = '''
from fastapi.testclient import TestClient
import asyncio
import uuid
import asyncpg
from api.main import app
from infrastructure.services.common.http_client import http_client_pool
from settings import settings

source_id = uuid.uuid4()
token_id = uuid.uuid4()

async def set_up():
    db = settings.postgres
    connection = await asyncpg.connect(host=db.db_host, port=db.db_port, user=db.db_user,
                                       password=db.db_password, database=db.db_name)
    try:
        async with connection.transaction():
            await connection.execute(
                "INSERT INTO sources (id, name, type, created_at, updated_at) VALUES ($1, $2, 'CEX', now(), now())",
                source_id, 'test-' + str(source_id))
            await connection.execute(
                "INSERT INTO tokens (id, symbol, base_asset, quote_asset, market_type, is_active, source_id, created_at, updated_at) "
                "VALUES ($1, 'TEST_USDT', 'TEST', 'USDT', 'SPOT', true, $2, now(), now())",
                token_id, source_id)
    finally:
        await connection.close()

async def tear_down():
    db = settings.postgres
    connection = await asyncpg.connect(host=db.db_host, port=db.db_port, user=db.db_user,
                                       password=db.db_password, database=db.db_name)
    try:
        await connection.execute('DELETE FROM tokens WHERE id = $1', token_id)
        await connection.execute('DELETE FROM sources WHERE id = $1', source_id)
    finally:
        await connection.close()

asyncio.run(set_up())
try:
    first = None
    for _ in range(2):
        with TestClient(app) as client:
            assert client.get('/ping').json() == {'ping': 'pong!'}
            assert client.get('/version').json() == {'version': '1.0.0'}
            response = client.get('/api/v1/tokens/')
            assert response.status_code == 200
            assert any(token['id'] == str(token_id) for token in response.json())
            current = http_client_pool.get_default_client()
            assert not current.is_closed
            if first is not None:
                assert current is not first
            first = current
        assert first.is_closed
finally:
    asyncio.run(tear_down())
'''
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=tmp_path, env=env,
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.skipif(
    os.environ.get("FINDER_INTEGRATION") != "1",
    reason="Set FINDER_INTEGRATION=1 with a disposable, migrated PostgreSQL database",
)
def test_migrated_schema_matches_models(tmp_path):
    backend = Path(__file__).resolve().parents[2]
    (tmp_path / ".env.prod").write_text((backend.parent / ".env.example").read_text())
    env = os.environ.copy()
    env["PYTHONPATH"] = str(backend)
    env["FINDER_BACKEND_PATH"] = str(backend)
    code = '''
import os
from pathlib import Path
from alembic import command
from alembic.config import Config

backend = Path(os.environ['FINDER_BACKEND_PATH'])
config = Config(str(backend / 'alembic.ini'))
config.set_main_option('script_location', str(backend / 'infrastructure/migrations'))
command.check(config)
'''
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=tmp_path, env=env,
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr

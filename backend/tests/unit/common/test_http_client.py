"""Proxy parsing must not disclose credentials in errors or logs."""

import asyncio

import httpx
import pytest

from infrastructure.services.common.http_client import (
    HTTPClientPool,
    format_proxy_url,
    load_proxies,
)


def test_invalid_proxy_error_does_not_include_input():
    secret = "private-password"
    with pytest.raises(ValueError) as exc:
        format_proxy_url(f"user:{secret}@invalid@host")
    assert secret not in str(exc.value)


def test_invalid_proxy_is_skipped_without_logging_credentials(tmp_path, caplog):
    secret = "private-password"
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text(f"user:{secret}@invalid@host\n")
    assert load_proxies(str(proxy_file)) == []
    assert secret not in caplog.text


def test_default_client_is_recreated_after_shutdown():
    async def exercise_pool():
        pool = HTTPClientPool()
        first = pool.get_default_client()
        await pool.close_all()
        assert first.is_closed
        second = pool.get_default_client()
        assert second is not first
        assert not second.is_closed
        await pool.close_all()

    asyncio.run(exercise_pool())


def test_reinitialization_cannot_discard_open_proxy_clients():
    async def exercise_pool():
        pool = object.__new__(HTTPClientPool)
        client = httpx.AsyncClient()
        pool._clients_pool = [client]
        pool._default_client = None
        pool._client_iterator = None
        pool._proxy_cycle = None
        with pytest.raises(RuntimeError, match="Close existing proxy clients"):
            pool.init_proxy_clients([])
        assert pool._clients_pool == [client]
        await pool.close_all()
        assert client.is_closed

    asyncio.run(exercise_pool())

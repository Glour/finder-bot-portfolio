"""HTTP retries, timeout settings and concurrency behaviour."""

import asyncio

import httpx
import pytest

from infrastructure.services.common import request as request_module


def test_request_applies_timeout_without_mutating_headers(monkeypatch):
    async def run():
        seen = []

        def handle(request):
            seen.append(request)
            return httpx.Response(200, json={"ok": True})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            monkeypatch.setattr(request_module.http_client_pool, "get_default_client", lambda: client)
            headers = {"Accept": "application/json"}
            result = await request_module.make_request(
                "GET", "/data", "example.test", use_proxy=False, headers=headers,
                config=request_module.RequestConfig(timeout_seconds=2.5),
            )
            assert result == {"ok": True}
            assert seen[0].extensions["timeout"]["read"] == 2.5
            assert "User-Agent" not in headers

    asyncio.run(run())


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_write_methods_use_configured_timeout(monkeypatch, method):
    async def run():
        seen = []

        def handle(request):
            seen.append(request)
            return httpx.Response(200, json={"ok": True})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            monkeypatch.setattr(request_module.http_client_pool, "get_default_client", lambda: client)
            result = await request_module.make_request(
                method, "/data", "example.test", use_proxy=False,
                config=request_module.RequestConfig(timeout_seconds=3.5),
            )
            assert result == {"ok": True}
            assert seen[0].method == method
            assert seen[0].extensions["timeout"]["read"] == 3.5

    asyncio.run(run())


def test_retry_wait_does_not_hold_concurrency_slot(monkeypatch):
    async def run():
        waiting = asyncio.Event()
        resume = asyncio.Event()
        attempts = []

        def handle(request):
            attempts.append(request.url.path)
            if request.url.path == "/first" and attempts.count("/first") == 1:
                return httpx.Response(503)
            return httpx.Response(200, json={"path": request.url.path})

        async def backoff(_delay):
            waiting.set()
            await resume.wait()

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            monkeypatch.setattr(request_module.http_client_pool, "get_default_client", lambda: client)
            monkeypatch.setattr(request_module.asyncio, "sleep", backoff)
            config = request_module.RequestConfig(max_concurrent_requests=1, max_retries=2)
            first = asyncio.create_task(request_module.make_request(
                "GET", "/first", "example.test", use_proxy=False, config=config,
            ))
            await asyncio.wait_for(waiting.wait(), timeout=1)
            second = await asyncio.wait_for(request_module.make_request(
                "GET", "/second", "example.test", use_proxy=False, config=config,
            ), timeout=1)
            assert second == {"path": "/second"}
            resume.set()
            assert await first == {"path": "/first"}
            assert attempts == ["/first", "/second", "/first"]

    asyncio.run(run())


def test_semaphores_respect_limit_and_event_loop():
    async def run():
        assert request_module.get_semaphore(1) is request_module.get_semaphore(1)
        assert request_module.get_semaphore(1) is not request_module.get_semaphore(2)
        with pytest.raises(ValueError):
            request_module.get_semaphore(0)
        return request_module.get_semaphore(1)

    first = asyncio.run(run())
    second = asyncio.run(run())
    assert first is not second

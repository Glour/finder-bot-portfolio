import asyncio
from typing import Any

from infrastructure.services.common.request import make_request
from settings import logger

request_queue: asyncio.Queue[
    tuple[str, str, str, int | None, dict | None, dict | str | None, dict | None, dict[str, str] | None, asyncio.Future]
] = asyncio.Queue()


async def queue_request(
        method: str,
        route: str,
        host: str,
        port: int | None = None,
        params: dict | None = None,
        data: dict | str | None = None,
        json_data: dict | None = None,
        headers: dict[str, str] | None = None,
) -> Any:
    loop = asyncio.get_event_loop()
    future = loop.create_future()

    await request_queue.put((
        method, route, host, port, params, data, json_data, headers, future
    ))

    return await future


async def worker():
    while True:
        method, route, host, port, params, data, json, headers, future = await request_queue.get()
        try:
            response = await make_request(
                method=method,
                route=route,
                host=host,
                port=port,
                params=params,
                data=data,
                json=json,
                headers=headers,
            )
            future.set_result(response)
        except Exception as e:
            logger.error(f"Worker error: {e}")
            future.set_exception(e)
        finally:
            request_queue.task_done()


async def start_workers(count: int = 100):
    for _ in range(count):
        asyncio.create_task(worker())

    logger.info(f"Started {count} workers")

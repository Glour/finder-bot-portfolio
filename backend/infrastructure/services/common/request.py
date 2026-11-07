"""
HTTP request handler with retry logic and comprehensive error handling.

This module provides a robust HTTP client wrapper with automatic retries,
proper timeout handling, and structured error reporting.
"""
import asyncio
from dataclasses import dataclass
from json import loads, JSONDecodeError
from typing import Any, Optional
from enum import Enum

import httpcore
import httpx

from infrastructure.services.common.http_client import get_next_client, http_client_pool
from settings import logger


class HTTPMethod(str, Enum):
    """Supported HTTP methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"


@dataclass
class RequestConfig:
    """Configuration for HTTP requests."""
    max_retries: int = 3
    timeout_seconds: float = 10.0
    max_concurrent_requests: int = 1000
    retry_delay: float = 0.5
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )


# Global semaphore for request rate limiting
_semaphore: Optional[asyncio.Semaphore] = None


def get_semaphore(max_concurrent: int = 1000) -> asyncio.Semaphore:
    """
    Get or create semaphore for request rate limiting.

    Args:
        max_concurrent: Maximum concurrent requests

    Returns:
        Asyncio semaphore
    """
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(max_concurrent)
    return _semaphore


def build_url(
    host: str,
    route: str,
    port: Optional[int] = None,
    use_https: bool = True
) -> str:
    """
    Build full URL from components.

    Args:
        host: Hostname or IP
        route: API route/path
        port: Optional port number
        use_https: Use HTTPS protocol

    Returns:
        Full URL string
    """
    scheme = "https" if use_https else "http"
    netloc = f"{host}:{port}" if port else host
    clean_route = route.lstrip('/')
    return f"{scheme}://{netloc}/{clean_route}"


async def make_request(
    method: str,
    route: str,
    host: str,
    port: Optional[int] = None,
    params: Optional[dict] = None,
    data: Optional[dict | str] = None,
    json: Optional[dict] = None,
    headers: Optional[dict[str, str]] = None,
    use_https: bool = True,
    use_proxy: bool = True,
    config: Optional[RequestConfig] = None,
    source_name: Optional[str] = None,
) -> Any:
    """
    Make HTTP request with retry logic and error handling.

    This function handles various network errors, retries on transient failures,
    and provides structured error logging.

    Args:
        method: HTTP method (GET, POST, etc.)
        route: API endpoint path
        host: Target hostname
        port: Optional port number
        params: Query parameters
        data: Form data
        json: JSON body data
        headers: Custom headers
        use_https: Use HTTPS protocol
        use_proxy: Use proxy rotation
        config: Request configuration
        source_name: Optional source identifier (e.g., 'lbank', 'gate') for better error logging

    Returns:
        Parsed JSON response or None on error

    Example:
        result = await make_request(
            method="GET",
            route="/api/tokens",
            host="api.example.com",
            params={"limit": 100},
            use_proxy=False,
            source_name="lbank"
        )
    """
    if config is None:
        config = RequestConfig()

    # Build URL
    url = build_url(host, route, port, use_https)

    # Prepare headers
    headers = headers or {}
    headers.setdefault("User-Agent", config.user_agent)

    # Validate method
    try:
        http_method = HTTPMethod(method.upper())
    except ValueError:
        raise ValueError(f"Unsupported HTTP method: {method}")

    # Get semaphore for rate limiting
    semaphore = get_semaphore(config.max_concurrent_requests)

    # Prepare source prefix for logging
    source_prefix = f"[{source_name or host}] " if source_name or host else ""

    # Track last error for final logging
    last_error: Optional[Exception] = None

    # Retry loop
    for attempt in range(1, config.max_retries + 1):
        async with semaphore:
            try:
                # Select client (proxy or default)
                if use_proxy:
                    try:
                        client = get_next_client()
                        if attempt == 1:  # Log only on first attempt to avoid spam
                            logger.debug(f"{source_prefix}Using proxy client for request")
                    except Exception as e:
                        logger.warning(f"{source_prefix}Proxy client unavailable (attempt {attempt}), using default: {e}")
                        client = http_client_pool.get_default_client()
                else:
                    client = http_client_pool.get_default_client()

                if client is None:
                    logger.error("No HTTP clients available")
                    return None

                # Execute request based on method
                if http_method == HTTPMethod.GET:
                    response = await client.get(url, params=params, headers=headers)
                elif http_method == HTTPMethod.POST:
                    response = await client.post(
                        url,
                        params=params,
                        data=data,
                        json=json,
                        headers=headers
                    )
                elif http_method == HTTPMethod.PUT:
                    response = await client.put(
                        url,
                        params=params,
                        data=data,
                        json=json,
                        headers=headers
                    )
                elif http_method == HTTPMethod.DELETE:
                    response = await client.delete(url, params=params, headers=headers)
                elif http_method == HTTPMethod.PATCH:
                    response = await client.patch(
                        url,
                        params=params,
                        data=data,
                        json=json,
                        headers=headers
                    )

                # Check status
                response.raise_for_status()

                # Parse JSON response
                raw_content = await response.aread()
                return loads(raw_content)

            except JSONDecodeError as e:
                last_error = e
                logger.error(
                    f"{source_prefix}JSON decode error on attempt {attempt}/{config.max_retries}: {str(e)}",
                    extra={
                        "url": url,
                        "source": source_name or host,
                        "content_preview": raw_content[:200].decode('utf-8', errors='ignore')
                    }
                )
                return None

            except httpx.PoolTimeout as e:
                last_error = e
                logger.warning(
                    f"{source_prefix}Pool timeout on attempt {attempt}/{config.max_retries} - connection pool exhausted",
                    extra={"url": url, "source": source_name or host, "use_proxy": use_proxy}
                )

                # Retry with longer delay for pool timeout (2-10 seconds)
                if attempt < config.max_retries:
                    pool_retry_delay = 2.0 * (2 ** (attempt - 1))  # 2s, 4s, 8s
                    await asyncio.sleep(pool_retry_delay)
                    continue

            except (
                httpx.ConnectTimeout,
                httpx.ReadTimeout,
                httpx.WriteTimeout,
                httpx.ReadError,
                httpx.ProxyError,
                httpx.RemoteProtocolError,
                httpx.ConnectError,
                httpcore.WriteTimeout,
                httpcore.LocalProtocolError,
            ) as e:
                last_error = e
                logger.debug(
                    f"{source_prefix}Network error on attempt {attempt}/{config.max_retries}: {e.__class__.__name__} - {str(e)}",
                    extra={"url": url, "source": source_name or host, "use_proxy": use_proxy}
                )

                # Retry with delay
                if attempt < config.max_retries:
                    await asyncio.sleep(config.retry_delay * attempt)
                    continue

            except httpx.HTTPStatusError as e:
                last_error = e
                status_code = e.response.status_code

                # Don't retry client errors (400-499)
                if 400 <= status_code < 500:
                    if status_code == 400:
                        logger.error(
                            f"{source_prefix}Bad request (400) - {url}",
                            extra={"url": url, "source": source_name or host, "method": method}
                        )
                        return "invalid_symbol"  # Legacy behavior
                    elif status_code == 404:
                        logger.warning(f"{source_prefix}Resource not found (404) - {url}")
                        return None
                    elif status_code in {403, 429}:
                        logger.warning(
                            f"{source_prefix}Rate limited or forbidden ({status_code}) on attempt {attempt}/{config.max_retries} - {url}",
                            extra={"url": url, "source": source_name or host, "attempt": attempt}
                        )
                        # Retry with longer exponential backoff for rate limits
                        if attempt < config.max_retries:
                            # 5s, 10s, 20s instead of 0.5s, 1s, 2s
                            delay = 5.0 * (2 ** (attempt - 1))
                            await asyncio.sleep(delay)
                            continue
                    else:
                        response_preview = e.response.text[:200] if hasattr(e.response, 'text') else ""
                        logger.error(
                            f"{source_prefix}Client error ({status_code}) - {url}",
                            extra={"url": url, "source": source_name or host, "response": response_preview}
                        )
                        return None

                # Retry server errors (500-599)
                elif 500 <= status_code < 600:
                    logger.warning(
                        f"{source_prefix}Server error ({status_code}) on attempt {attempt}/{config.max_retries} - {url}",
                        extra={"url": url, "source": source_name or host}
                    )
                    if attempt < config.max_retries:
                        await asyncio.sleep(config.retry_delay * attempt)
                        continue

                return None

            except Exception as e:
                last_error = e
                logger.exception(
                    f"{source_prefix}Unexpected error on attempt {attempt}/{config.max_retries}: {type(e).__name__} - {str(e)}",
                    extra={
                        "url": url[:100],
                        "source": source_name or host,
                        "error_type": type(e).__name__,
                        "method": method
                    }
                )

                # Don't retry on unexpected errors
                return None

    # Max retries reached - log with last error details
    error_detail = f": {type(last_error).__name__} - {str(last_error)}" if last_error else ""
    logger.error(
        f"{source_prefix}Max retries ({config.max_retries}) reached{error_detail}",
        extra={
            "url": url[:100],
            "source": source_name or host,
            "method": method,
            "last_error": str(last_error) if last_error else None
        }
    )
    return None

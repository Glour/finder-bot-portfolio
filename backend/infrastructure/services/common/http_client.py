"""
HTTP Client Pool Management with proper lifecycle and resource management.

This module provides a thread-safe HTTP client pool with proxy support,
eliminating global state and implementing proper singleton pattern.
"""
import itertools
import logging
from pathlib import Path
from typing import Optional, Iterator
from contextlib import asynccontextmanager

import httpx

logger = logging.getLogger("settings")


class ProxyError(Exception):
    """Raised when proxy configuration is invalid."""
    pass


class ClientPoolNotInitializedError(Exception):
    """Raised when trying to use client pool before initialization."""
    pass


def format_proxy_url(proxy_str: str) -> str:
    """
    Format proxy string into URL format.

    Args:
        proxy_str: Proxy in format 'username:password@host:port'

    Returns:
        Formatted proxy URL

    Raises:
        ValueError: If proxy format is invalid
    """
    proxy_str = proxy_str.strip()

    # Check for new format: username:password@host:port
    if "@" in proxy_str:
        parts = proxy_str.split("@")
        if len(parts) != 2:
            raise ValueError("Invalid proxy format; expected username:password@host:port")

        auth_part = parts[0]  # username:password
        server_part = parts[1]  # host:port

        # Validate auth part
        auth_parts = auth_part.split(":")
        if len(auth_parts) != 2:
            raise ValueError("Invalid proxy authentication; expected username:password")

        # Validate server part
        server_parts = server_part.split(":")
        if len(server_parts) != 2:
            raise ValueError("Invalid proxy server; expected host:port")

        return f"http://{proxy_str}"

    # Fallback to old format: host:port:username:password
    parts = proxy_str.split(":")
    if len(parts) != 4:
        raise ValueError("Invalid proxy format; expected username:password@host:port")
    host, port, username, password = parts
    return f"http://{username}:{password}@{host}:{port}"


def load_proxies(file_path: str = "/app/data/proxies.txt") -> list[str]:
    """
    Load and validate proxies from file.

    Args:
        file_path: Path to proxy file

    Returns:
        List of formatted proxy URLs

    Raises:
        ProxyError: If file not found or proxies invalid
    """
    try:
        path = Path(file_path)
        if not path.exists():
            logger.warning(f"Proxies file not found at {file_path}")
            return []

        with path.open("r") as f:
            raw_proxies = [
                line.strip()
                for line in f
                if line.strip() and not line.startswith("#")
            ]

        if not raw_proxies:
            logger.warning(f"No valid proxies found in {file_path}")
            return []

        proxies = []
        for i, proxy_str in enumerate(raw_proxies, 1):
            try:
                proxies.append(format_proxy_url(proxy_str))
            except ValueError as e:
                logger.warning(f"Skipping invalid proxy at line {i}: {e}")

        logger.info(f"Loaded {len(proxies)} valid proxies from {file_path}")
        return proxies

    except Exception as e:
        logger.error(f"Failed to load proxies from {file_path}: {e}")
        return []


class HTTPClientPool:
    """
    Singleton HTTP client pool with proxy rotation support.

    This class manages a pool of HTTP clients with proper lifecycle management,
    eliminating global state and providing thread-safe operations.
    """

    _instance: Optional['HTTPClientPool'] = None
    _initialized: bool = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # Only initialize once
        if HTTPClientPool._initialized:
            return

        self._clients_pool: list[httpx.AsyncClient] = []
        self._client_iterator: Optional[Iterator[httpx.AsyncClient]] = None
        self._proxy_cycle: Optional[Iterator[str]] = None
        self._default_client: Optional[httpx.AsyncClient] = None

        # Create default non-proxy client
        self._default_client = self._create_client()

        HTTPClientPool._initialized = True
        logger.info("HTTPClientPool initialized")

    @staticmethod
    def _create_client(
        proxy: Optional[str] = None,
        timeout: Optional[httpx.Timeout] = None,
        limits: Optional[httpx.Limits] = None,
    ) -> httpx.AsyncClient:
        """Create a configured HTTP client."""
        if timeout is None:
            timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=90.0)
        if limits is None:
            limits = httpx.Limits(max_connections=1000, max_keepalive_connections=1000)

        return httpx.AsyncClient(
            proxy=proxy,
            timeout=timeout,
            limits=limits,
            http2=False,  # Disabled: causes "Max outbound streams" errors with some APIs
            follow_redirects=True,
        )

    def init_proxy_clients(
        self,
        proxies_list: Optional[list[str]] = None,
        clients_count: int = 100
    ) -> None:
        """
        Initialize client pool with proxies.

        Args:
            proxies_list: List of proxy URLs. If None, loads from default location
            clients_count: Maximum number of clients to create
        """
        # Clean up existing clients
        if self._clients_pool:
            logger.warning("Reinitializing client pool - old clients will be closed")

        self._clients_pool.clear()

        # Load proxies if not provided
        if proxies_list is None:
            proxies_list = load_proxies()

        if not proxies_list:
            logger.error("⚠️  CRITICAL: No proxies available! All requests will use direct connection (may cause rate limiting)")
            self._client_iterator = None
            self._proxy_cycle = None
            return

        # Create clients with proxies
        limits = httpx.Limits(max_connections=1000, max_keepalive_connections=1000)
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=90.0)

        # Distribute proxies across requested client count
        chunk_size = max(1, len(proxies_list) // clients_count)
        proxy_chunks = [
            proxies_list[i:i + chunk_size]
            for i in range(0, len(proxies_list), chunk_size)
        ]

        for chunk in proxy_chunks:
            for proxy_url in chunk:
                try:
                    client = self._create_client(
                        proxy=proxy_url,
                        timeout=timeout,
                        limits=limits
                    )
                    self._clients_pool.append(client)
                except Exception:
                    logger.error("Failed to create a proxy client; check proxy configuration")

        if self._clients_pool:
            self._client_iterator = itertools.cycle(self._clients_pool)
            self._proxy_cycle = itertools.cycle(proxies_list)
            logger.info(f"✅ Successfully initialized {len(self._clients_pool)} HTTP clients with {len(proxies_list)} proxies")
        else:
            logger.error("⚠️  CRITICAL: Failed to create any proxy clients! Check proxies.txt format")
            self._client_iterator = None
            self._proxy_cycle = None

    def get_next_client(self) -> httpx.AsyncClient:
        """
        Get next client from the pool using round-robin.

        Returns:
            HTTP client from pool

        Raises:
            ClientPoolNotInitializedError: If proxy pool not initialized
        """
        if self._client_iterator is None or not self._clients_pool:
            raise ClientPoolNotInitializedError(
                "Proxy client pool not initialized. Call init_proxy_clients() first."
            )
        return next(self._client_iterator)

    def get_default_client(self) -> httpx.AsyncClient:
        """Get the default non-proxy client."""
        return self._default_client

    def get_next_proxy(self) -> Optional[str]:
        """Get next proxy URL from the cycle."""
        if self._proxy_cycle is None:
            return None
        return next(self._proxy_cycle, None)

    async def close_all(self) -> None:
        """Close all clients in the pool."""
        closed_count = 0

        # Close proxy clients
        for client in self._clients_pool:
            try:
                await client.aclose()
                closed_count += 1
            except Exception as e:
                logger.error(f"Error closing client: {e}")

        # Close default client
        if self._default_client:
            try:
                await self._default_client.aclose()
                closed_count += 1
            except Exception as e:
                logger.error(f"Error closing default client: {e}")

        self._clients_pool.clear()
        self._client_iterator = None

        logger.info(f"Closed {closed_count} HTTP clients")

    @asynccontextmanager
    async def lifespan(self):
        """Context manager for proper lifecycle management."""
        try:
            yield self
        finally:
            await self.close_all()


# Singleton instance for backward compatibility
_pool = HTTPClientPool()


# Legacy API for backward compatibility
def init_clients(proxies_list: list[str], clients_count: int = 100) -> None:
    """Legacy function - delegates to HTTPClientPool."""
    _pool.init_proxy_clients(proxies_list, clients_count)


def get_next_client() -> httpx.AsyncClient:
    """Legacy function - delegates to HTTPClientPool."""
    return _pool.get_next_client()


def get_next_proxy() -> Optional[str]:
    """Legacy function - delegates to HTTPClientPool."""
    return _pool.get_next_proxy()


async def close_clients() -> None:
    """Legacy function - delegates to HTTPClientPool."""
    await _pool.close_all()


# Export singleton instance for modern usage
http_client_pool = _pool
http_client = _pool.get_default_client()

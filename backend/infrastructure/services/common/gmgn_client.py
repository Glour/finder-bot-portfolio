"""GMGN HTTP client with curl_cffi for Cloudflare bypass.

Uses curl_cffi to impersonate Chrome browser TLS fingerprint,
bypassing Cloudflare anti-bot protection (403 Forbidden).
"""

from typing import Any
from curl_cffi import requests
from settings import logger


class GmgnClient:
    """
    HTTP client for GMGN API using curl_cffi browser impersonation.

    Bypasses Cloudflare protection by mimicking Chrome 124 TLS fingerprint.
    """

    BASE_URL = "https://gmgn.ai"
    IMPERSONATE = "chrome124"  # Match GmGnAPI library

    def __init__(self, proxies: list[str] | None = None):
        """
        Initialize GMGN client.

        Args:
            proxies: Optional list of proxy URLs (e.g., ["http://user:pass@host:port"])
        """
        self.proxies = proxies or []
        self._proxy_index = 0

        # Browser-like headers matching WebSocket client
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Origin": "https://gmgn.ai",
            "Referer": "https://gmgn.ai/",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

    def _get_next_proxy(self) -> dict[str, str] | None:
        """Get next proxy in round-robin fashion."""
        if not self.proxies:
            return None

        proxy_url = self.proxies[self._proxy_index]
        self._proxy_index = (self._proxy_index + 1) % len(self.proxies)

        return {"https": proxy_url, "http": proxy_url}

    def post_json(
        self,
        endpoint: str,
        data: dict[str, Any],
        max_retries: int = 3
    ) -> dict[str, Any] | None:
        """
        Make POST request with JSON payload.

        Args:
            endpoint: API endpoint (e.g., "/api/v1/mutil_window_token_info")
            data: JSON payload
            max_retries: Maximum retry attempts

        Returns:
            Parsed JSON response or None on error
        """
        url = f"{self.BASE_URL}{endpoint}"

        for attempt in range(1, max_retries + 1):
            try:
                # Prepare headers with Content-Type
                headers = self.headers.copy()
                headers["Content-Type"] = "application/json"

                # Get proxy for this attempt
                proxies = self._get_next_proxy()

                # Make request with curl_cffi impersonation
                response = requests.post(
                    url,
                    json=data,
                    headers=headers,
                    proxies=proxies,
                    impersonate=self.IMPERSONATE,
                    timeout=30,
                    verify=False  # Disable SSL verification like GmGnAPI
                )

                # Check status
                if response.status_code == 200:
                    try:
                        return response.json()
                    except Exception as e:
                        logger.error(f"GMGN: Failed to parse JSON response: {e}")
                        return None

                elif response.status_code == 403:
                    logger.warning(
                        f"GMGN: 403 Forbidden on attempt {attempt}/{max_retries} (proxy: {bool(proxies)})"
                    )
                    if attempt == max_retries:
                        return None

                elif response.status_code == 429:
                    logger.warning(
                        f"GMGN: Rate limited (429) on attempt {attempt}/{max_retries}"
                    )
                    if attempt == max_retries:
                        return None

                else:
                    logger.error(
                        f"GMGN: HTTP {response.status_code} - {url}",
                        extra={"response": response.text[:200]}
                    )
                    return None

            except Exception as e:
                logger.error(
                    f"GMGN: Request failed on attempt {attempt}/{max_retries}: {e}",
                    extra={"url": url}
                )
                if attempt == max_retries:
                    return None

        return None

    def get_json(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        max_retries: int = 3
    ) -> dict[str, Any] | None:
        """
        Make GET request.

        Args:
            endpoint: API endpoint
            params: Query parameters
            max_retries: Maximum retry attempts

        Returns:
            Parsed JSON response or None on error
        """
        url = f"{self.BASE_URL}{endpoint}"

        for attempt in range(1, max_retries + 1):
            try:
                # Get proxy for this attempt
                proxies = self._get_next_proxy()

                # Make request with curl_cffi impersonation
                response = requests.get(
                    url,
                    params=params,
                    headers=self.headers,
                    proxies=proxies,
                    impersonate=self.IMPERSONATE,
                    timeout=30,
                    verify=False
                )

                # Check status
                if response.status_code == 200:
                    try:
                        return response.json()
                    except Exception as e:
                        logger.error(f"GMGN: Failed to parse JSON response: {e}")
                        return None

                elif response.status_code in {403, 429}:
                    logger.warning(
                        f"GMGN: HTTP {response.status_code} on attempt {attempt}/{max_retries}"
                    )
                    if attempt == max_retries:
                        return None

                else:
                    logger.error(f"GMGN: HTTP {response.status_code} - {url}")
                    return None

            except Exception as e:
                logger.error(f"GMGN: Request failed on attempt {attempt}/{max_retries}: {e}")
                if attempt == max_retries:
                    return None

        return None

import asyncio
import itertools
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import aiohttp

base_dir = Path(__file__).resolve().parents[6]
proxy_file = base_dir / "docker" / "data" / "proxies.txt"


def format_proxy_url(proxy_str: str) -> str:
    parts = proxy_str.strip().split(":")
    if len(parts) != 4:
        raise ValueError("Invalid proxy format, expected host:port:username:password")
    host, port, username, password = parts
    return f"http://{username}:{password}@{host}:{port}"


def load_proxies(file_path: str = str(proxy_file)) -> list[str] | None:
    try:
        with open(file_path, "r") as f:
            raw_proxies = [line.strip() for line in f if line.strip() and not line.startswith("#")]
        proxies = [format_proxy_url(p) for p in raw_proxies]
        return proxies

    except FileNotFoundError:
        logging.warning("Proxies file not found, proxies list is empty")


_proxy_cycle = itertools.cycle(load_proxies() or [])


def get_next_proxy() -> str | None:
    return next(_proxy_cycle, None)


async def fetch_spot_fee_metadata() -> list[dict]:
    """Запрашивает данные о комиссиях со спота."""
    url = f"https://ccapi.lbank.com/coin-wallet-center/wallet/fee/pageListWalletFees"
    page = 1
    page_size = 10000
    all_results: list[dict] = []

    async with aiohttp.ClientSession() as session:
        while True:
            payload = {"pageNo": page, "pageSize": page_size, "assetCode": ""}
            try:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        logging.error(f"Request failed: {resp.status}")
                        break
                    data = await resp.json()
            except Exception as e:
                logging.error(f"Request error: {e}")
                break

            result_list = data.get("data", {}).get("resultList", []) if data else []
            if not result_list:
                break

            all_results.extend(result_list)

            # защита от бесконечного цикла
            if page > 100:
                break

            page += 1

    # сохраняем для отладки
    with open("lbank_fee_meta.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    return all_results


async def fetch_spot_network_data() -> list[dict]:
    """Запрашивает все сети для всех токенов со спота."""
    url = "https://api.lbkex.com/v2/withdrawConfigs.do"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("data", []) or []
            else:
                logging.error(f"Ошибка запроса spot data: {resp.status}")
                return []


async def fetch_contract_address(asset: str, chain: str) -> dict[str, Any]:
    """Запрашивает адрес контракта для токена в конкретной сети."""
    url = "https://ccapi.rerrkvifj.com/coin-wallet-center/wallet/address/deposit/firstQuery"
    payload = {"assetCode": asset, "assetFlag": chain}
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "ex-browser-name": "Chrome",
        "ex-browser-version": "140.0.0.0",
        "ex-client-source": "WEB",
        "ex-client-type": "WEB",
        "ex-device-id": "",
        "ex-language": "en-US",
        "ex-os-name": "Mac OS",
        "ex-os-version": "10.15.7",
        "ex-timestamp": str(int(time.time() * 1000)),
        "ex-token": os.getenv("LBANK_EX_TOKEN", ""),
        "origin": "https://www.lbank.com",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://www.lbank.com/",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "source": "4",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/140.0.0.0 Safari/537.36"
    }

    async with aiohttp.ClientSession() as session:
        try:
            proxy = get_next_proxy()
            async with session.post(url, headers=headers, json=payload, proxy=proxy) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", {}).get("addressData", {})
                else:
                    logging.error(f"Ошибка запроса контракта {asset}-{chain}: {resp.status}")

                await asyncio.sleep(10)

        except Exception as e:
            logging.error(f"Ошибка запроса контракта {asset}-{chain}: {e}")

    return {}


async def collect_full_network_data() -> dict[str, dict[str, dict]]:
    """Собирает полную информацию по сетям всех токенов."""
    spot_network_data = await fetch_spot_network_data()
    tasks: dict[tuple[str, str], asyncio.Task] = {}
    for token_network in spot_network_data:
        asset = token_network["assetCode"].upper()
        chain = token_network.get("chain", "").lower()
        key = (asset, chain)
        tasks[key] = asyncio.create_task(fetch_contract_address(asset.lower(), chain.lower()))

    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    full_network_map: dict[str, dict[str, dict]] = {}
    for (asset, chain), res in zip(tasks.keys(), results):
        if isinstance(res, Exception) or res is None:
            res = {}
        if asset not in full_network_map:
            full_network_map[asset] = {}
        full_network_map[asset][chain] = res

    # Сохраняем в JSON
    with open("full_network_data.json", "w") as f:
        json.dump(full_network_map, f, indent=2)

    return full_network_map


async def main():
    await collect_full_network_data()
    # await fetch_spot_fee_metadata()


if __name__ == "__main__":
    asyncio.run(main())

import json

import httpx

http_client = httpx.Client()


def fetch_gate_withdraw_fees(save_to_file: str | None = None) -> list[dict]:
    base_url = "https://www.gate.com/api/web/v1/withdraw/depositwithdraw/getCoinsDepositWithdrawFee"
    page_size = 200
    all_data = []

    resp = http_client.get(base_url, params={"keyword": "", "page": 1, "page_size": page_size})
    resp.raise_for_status()
    data = resp.json()
    page_count = data["data"].get("page_count", 1)

    all_data.extend(extract_list(data))

    for page in range(2, page_count + 1):
        resp = http_client.get(base_url, params={"keyword": "", "page": page, "page_size": page_size})
        resp.raise_for_status()
        page_data = resp.json()
        all_data.extend(extract_list(page_data))

    if save_to_file:
        with open(save_to_file, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)

    return all_data


def extract_list(page_data):
    out = []
    for coin in page_data["data"].get("list", []):
        symbol = coin.get("coin")
        for ch in coin.get("chains", []):
            entry = {"symbol": symbol}
            entry.update(ch)
            out.append(entry)
    return out


fetch_gate_withdraw_fees("gate_withdraw_fees.json")

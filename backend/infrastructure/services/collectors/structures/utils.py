from datetime import datetime, timezone

from infrastructure.services.collectors.structures.maps import GMGN_CHAIN_MAP


def generate_market_link(
        market: str,
        symbol: str,
        contract_address: str = "",
        chain: str = "",
        source: str = ""
) -> str:
    chain = chain.lower()

    match market:
        case "spot":
            match source:
                case "mexc":
                    return f"https://www.mexc.com/exchange/{symbol}"
                case "gate":
                    return f"https://www.gate.com/trade/{symbol}"
                case "xt":
                    return f"https://www.xt.com/trade/{symbol.lower()}"
                case "lbank":
                    return f"https://www.lbank.com/trade/{symbol.lower()}"
                case "asterdex":
                    return f"https://www.asterdex.com/en/trade/pro/spot/{symbol.replace('_', '')}"
                case "bitmart":
                    return f"https://www.bitmart.com/trade/en-US?symbol={symbol}"
                case "bitget":
                    # Format: BTCUSDT -> /trade/spot/BTCUSDT_SPBL
                    return f"https://www.bitget.com/spot/{symbol}"
                case "okx":
                    # Format: BTC-USDT -> /trade/spot/BTC-USDT
                    return f"https://www.okx.com/trade-spot/{symbol.lower()}"
                case "ascendex":
                    # Format: BTC/USDT or BTC_USDT -> /cashtrade-spottrading/usdt/btc (quote/base, lowercase)
                    if "/" in symbol:
                        base, quote = symbol.split("/", 1)
                    elif "_" in symbol:
                        base, quote = symbol.split("_", 1)
                    else:
                        return ""
                    return f"https://ascendex.com/en/cashtrade-spottrading/{quote.lower()}/{base.lower()}"
                case "hyperliquid":
                    # Format: BASE_QUOTE -> /trade/BASE/QUOTE (e.g., SEDA_USDC -> SEDA/USDC)
                    if "_" in symbol:
                        symbol = symbol.replace("_", "/")
                    return f"https://app.hyperliquid.xyz/trade/{symbol}"
                case "bybit":
                    # Format: BTCUSDT or BTC_USDT -> /trade/spot/BTC/USDT
                    base = symbol.replace("_USDT", "").replace("-USDT", "").replace("USDT", "").strip("_-")
                    return f"https://www.bybit.com/trade/spot/{base}/USDT"
                case "binance":
                    # Format: BTCUSDT -> /trade/BTC_USDT
                    base = symbol.replace("USDT", "")
                    return f"https://www.binance.com/trade/{base}_USDT"
                case "kucoin":
                    # Format: BTC-USDT (already has dash) -> /trade/BTC-USDT
                    return f"https://www.kucoin.com/trade/{symbol}"
                case _:
                    return ""

        case "futures":
            match source:
                case "mexc":
                    return f"https://www.mexc.com/futures/{symbol}"
                case "gate":
                    return f"https://www.gate.com/futures/USDT/{symbol}"
                case "xt":
                    return f"https://www.xt.com/futures/trade/{symbol.lower()}"
                case "lbank":
                    return f"https://www.lbank.com/futures/{symbol.lower()}"
                case "asterdex":
                    return f"https://www.asterdex.com/en/trade/pro/futures/{symbol.replace('_', '')}"
                case "bitmart":
                    # Format: ZKP_USDT -> ZKPUSDT
                    return f"https://www.bitmart.com/en-US/futures/{symbol.replace('_', '')}"
                case "bitget":
                    return f"https://www.bitget.com/futures/{symbol}"
                case "bybit":
                    return f"https://www.bybit.com/trade/futures/usdt/{symbol}"
                case "binance":
                    return f"https://www.binance.com/futures/{symbol}"
                case "kucoin":
                    return f"https://www.kucoin.com/futures/{symbol}"
                case "okx":
                    # Format: BTC-USDT-SWAP -> /trade-swap/BTC-USDT-SWAP
                    return f"https://www.okx.com/trade-swap/{symbol.lower()}"
                case "ascendex":
                    # Format: BTC-PERP -> /futures-perpetualcontract-trading/btc-perp (lowercase)
                    return f"https://ascendex.com/en/futures-perpetualcontract-trading/{symbol.lower()}"
                case "hyperliquid":
                    # Format: BTC -> /trade/BTC (simple coin name)
                    return f"https://app.hyperliquid.xyz/trade/{symbol}"
                case _:
                    return ""

        case "dex":
            # GMGN-specific handling
            if source == "gmgn":
                # Map DB chain name (e.g. "solana") to GMGN short name (e.g. "sol")
                gmgn_chain = GMGN_CHAIN_MAP.get(chain, chain)
                # Format: https://gmgn.ai/sol/token/{contract_address}
                if contract_address:
                    return f"https://gmgn.ai/{gmgn_chain}/token/{contract_address}"
                return f"https://gmgn.ai/{gmgn_chain}"
            # Generic dex (dexscreener)
            return f"https://dexscreener.com/{chain}/{contract_address}"
        case _:
            return ""


def generate_asset_link(source: str, asset: str, action: str) -> str:
    source = source.lower()
    asset = asset.upper()

    match source:
        case "mexc":
            return f"https://www.mexc.com/assets/{action}/{asset}"

        case "gate":
            if action == "deposit":
                return f"https://www.gate.com/myaccount/deposit/{asset}"
            elif action == "withdraw":
                return f"https://www.gate.com/myaccount/funds/withdraw/{asset}"
            else:
                return ""

        case "xt":
            if action == "deposit":
                return f"https://www.xt.com/en/accounts/assets/wallet/deposit?account=SPOT&currency={asset}"
            elif action == "withdraw":
                return f"https://www.xt.com/en/accounts/assets/wallet/withdraw?account=SPOT&currency={asset}"
            else:
                return ""

        case "lbank":
            if action == "deposit":
                return f"https://www.lbank.com/wallet/account/main/deposit/crypto/{asset}"
            elif action == "withdraw":
                return f"https://www.lbank.com/wallet/account/main/withdrawal/crypto/{asset}/chain"
            else:
                return ""

        case "asterdex":
            if action == "deposit":
                return f"https://www.asterdex.com/en/assets/wallet/deposit/{asset}"
            elif action == "withdraw":
                return f"https://www.asterdex.com/en/assets/wallet/withdraw/{asset}"
            else:
                return ""

        case "bitmart":
            if action == "deposit":
                return f"https://www.bitmart.com/asset/en-US?currency={asset}"
            elif action == "withdraw":
                return f"https://www.bitmart.com/asset/en-US?currency={asset}"
            else:
                return ""

        case "ascendex":
            if action == "deposit":
                return f"https://ascendex.com/en/assets/assets-recharge/{asset}"
            elif action == "withdraw":
                return f"https://ascendex.com/en/assets/assets-withdraw/{asset}"
            else:
                return ""

        case "kucoin":
            if action == "deposit":
                return "https://www.kucoin.com/assets/recharge"
            elif action == "withdraw":
                return "https://www.kucoin.com/assets/withdraw"
            else:
                return ""

        case "binance":
            if action == "deposit":
                return "https://www.binance.com/en/my/wallet/account/main/deposit/crypto"
            elif action == "withdraw":
                return "https://www.binance.com/en/my/wallet/account/main/withdrawal"
            else:
                return ""

        case "bybit":
            if action == "deposit":
                return f"https://www.bybit.com/user/assets/deposit"
            elif action == "withdraw":
                return f"https://www.bybit.com/user/assets/withdraw"
            else:
                return ""

        case "bitget":
            if action == "deposit":
                return f"https://www.bitget.com/asset/recharge?coin={asset}"
            elif action == "withdraw":
                return f"https://www.bitget.com/asset/withdrawal?coin={asset}"
            else:
                return ""

        case "okx":
            if action == "deposit":
                return f"https://www.okx.com/balance/recharge?currency={asset}"
            elif action == "withdraw":
                return f"https://www.okx.com/balance/withdrawal?currency={asset}"
            else:
                return ""

        case _:
            return ""


def make_orderbook_key(global_token_id, source_id, token_id, market_type):
    return f"orderbook:{global_token_id}:{source_id}:{token_id}:{market_type}"


def format_dt(iso_dt: str | None) -> str:
    if not iso_dt:
        return ""

    dt = datetime.fromisoformat(iso_dt)
    return dt.astimezone(timezone.utc).strftime("%H:%M:%S")


def format_significant(x: float, sig: int = 3) -> str:
    if x == 0:
        return "0"
    import math
    order = int(math.floor(math.log10(abs(x))))
    decimals = max(sig - order - 1, 0)
    return f"{x:.{decimals}f}"

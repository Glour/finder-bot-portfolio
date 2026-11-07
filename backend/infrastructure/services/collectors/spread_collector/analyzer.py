class ArbitrageAnalyzer:
    def analyze_pair(
            self,
            ob1: dict, name1: str, contract1: float,
            ob2: dict, name2: str, contract2: float,
            depth_analyze: bool = False
    ):
        if depth_analyze:
            return [r for r in (
                self._analyze_direction_depth(ob1, name1, contract1, ob2, name2, contract2),
                self._analyze_direction_depth(ob2, name2, contract2, ob1, name1, contract1),
            ) if r]

        return [r for r in (
            self._analyze_direction(ob1, name1, contract1, ob2, name2, contract2),
            self._analyze_direction(ob2, name2, contract2, ob1, name1, contract1),
        ) if r]

    def _analyze_direction(
            self,
            buyer_ob: dict, buyer_name: str, buyer_contract: float,
            seller_ob: dict, seller_name: str, seller_contract: float
    ):
        best_bid = self._get_best_bid(buyer_ob.get("bids", []))
        best_ask = self._get_best_ask(seller_ob.get("asks", []))
        if not best_bid or not best_ask:
            return None

        bid_price, bid_volume = best_bid
        ask_price, ask_volume = best_ask
        if bid_price is None or ask_price is None or bid_price <= 0 or ask_price <= 0:
            return None

        buyer_multiplier = buyer_contract if "futures" in buyer_name.lower() else 1.0
        seller_multiplier = seller_contract if "futures" in seller_name.lower() else 1.0

        available_base_buyer = bid_volume * buyer_multiplier
        available_base_seller = ask_volume * seller_multiplier

        executed_base = min(available_base_buyer, available_base_seller)
        if executed_base <= 0:
            return None

        sell_usdt = executed_base * bid_price
        buy_usdt = executed_base * ask_price
        profit = sell_usdt - buy_usdt

        spread_pct = ((bid_price - ask_price) / ask_price) * 100

        if profit <= 0:
            return None

        return {
            "buy_from": seller_name,
            "sell_to": buyer_name,
            "buy_price": ask_price,
            "sell_price": bid_price,
            "volume": executed_base,
            "profit": profit,
            "spread_percent": spread_pct,
        }

    @staticmethod
    def _analyze_direction_depth(
            buyer_ob: dict, buyer_name: str, buyer_contract: float,
            seller_ob: dict, seller_name: str, seller_contract: float
    ):
        bids = sorted(
            ((level[0], level[1]) for level in buyer_ob.get("bids", [])
             if len(level) >= 2 and level[0] > 0 and level[1] > 0),
            key=lambda level: -level[0],
        )
        asks = sorted(
            ((level[0], level[1]) for level in seller_ob.get("asks", [])
             if len(level) >= 2 and level[0] > 0 and level[1] > 0),
            key=lambda level: level[0],
        )

        if not bids or not asks:
            return None

        buyer_multiplier = buyer_contract if "futures" in buyer_name.lower() else 1.0
        seller_multiplier = seller_contract if "futures" in seller_name.lower() else 1.0

        total_executed = 0.0
        total_profit = 0.0
        total_buy_usdt = 0.0
        total_sell_usdt = 0.0

        i, j = 0, 0
        while i < len(bids) and j < len(asks):
            bid_price, bid_volume = bids[i]
            ask_price, ask_volume = asks[j]

            if bid_price < ask_price:
                break

            available_buyer = bid_volume * buyer_multiplier
            available_seller = ask_volume * seller_multiplier

            executed = min(available_buyer, available_seller)
            if executed <= 0:
                break

            sell_usdt = executed * bid_price
            buy_usdt = executed * ask_price
            profit = sell_usdt - buy_usdt

            if profit > 0:
                total_executed += executed
                total_profit += profit
                total_buy_usdt += buy_usdt
                total_sell_usdt += sell_usdt

            if available_buyer > available_seller:
                bids[i] = (bid_price, (available_buyer - executed) / buyer_multiplier)
                j += 1
            elif available_seller > available_buyer:
                asks[j] = (ask_price, (available_seller - executed) / seller_multiplier)
                i += 1
            else:
                i += 1
                j += 1

        if total_executed <= 0 or total_profit <= 0:
            return None

        avg_buy_price = total_buy_usdt / total_executed
        avg_sell_price = total_sell_usdt / total_executed
        spread_pct = ((avg_sell_price - avg_buy_price) / avg_buy_price) * 100

        return {
            "buy_from": seller_name,
            "sell_to": buyer_name,
            "buy_price": avg_buy_price,
            "sell_price": avg_sell_price,
            "volume": total_executed,
            "profit": total_profit,
            "spread_percent": spread_pct,
        }

    @staticmethod
    def _get_best_ask(asks: list) -> list[float] | None:
        try:
            return min((lvl for lvl in asks if len(lvl) > 1), key=lambda x: x[0])
        except ValueError:
            return None

    @staticmethod
    def _get_best_bid(bids: list) -> list[float] | None:
        try:
            return max((lvl for lvl in bids if len(lvl) > 1), key=lambda x: x[0])
        except ValueError:
            return None

from itertools import product, combinations

from infrastructure.services.collectors.spread_collector.analyzer import ArbitrageAnalyzer


class ArbitrageAnalyzerService:
    def __init__(self, analyzer: ArbitrageAnalyzer):
        self.analyzer = analyzer

    def analyze_same_type(self, market_type: str, sources: dict, depth_analyze: bool = False):
        results = []
        items = list(sources.values())
        for s1, s2 in combinations(items, 2):
            if s1["source_name"] == s2["source_name"]:
                continue
            res = self.analyzer.analyze_pair(
                s1["orderbook"], f"{market_type}_{s1['source_name']}", s1.get("contract_size", 1.0),
                s2["orderbook"], f"{market_type}_{s2['source_name']}", s2.get("contract_size", 1.0),
                depth_analyze
            )
            for r in res:
                r["symbols"] = {
                    f"{market_type}_{s1['source_name']}": f"{s1['base_asset']}_{s1['quote_asset']}",
                    f"{market_type}_{s2['source_name']}": f"{s2['base_asset']}_{s2['quote_asset']}",
                }
                r["sources"] = {
                    f"{market_type}_{s1['source_name']}": s1["source_name"],
                    f"{market_type}_{s2['source_name']}": s2["source_name"],
                }
            results += res
        return results

    def analyze_cross_type(self, type1: str, sources1: dict, type2: str, sources2: dict, depth_analyze: bool = False):
        results = []
        for s1, s2 in product(sources1.values(), sources2.values()):
            res = self.analyzer.analyze_pair(
                s1["orderbook"], f"{type1}_{s1['source_name']}", s1.get("contract_size", 1.0),
                s2["orderbook"], f"{type2}_{s2['source_name']}", s2.get("contract_size", 1.0),
                depth_analyze
            )
            for r in res:
                r["symbols"] = {
                    f"{type1}_{s1['source_name']}": f"{s1.get('base_asset')}_{s1.get('quote_asset')}",
                    f"{type2}_{s2['source_name']}": f"{s2.get('base_asset')}_{s2.get('quote_asset')}",
                }
                r["sources"] = {
                    f"{type1}_{s1['source_name']}": s1["source_name"],
                    f"{type2}_{s2['source_name']}": s2["source_name"],
                }
            results += res
        return results

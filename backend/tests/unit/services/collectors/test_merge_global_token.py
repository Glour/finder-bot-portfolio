"""
Tests for global token merge candidate finder.

Covers all merge scenarios:
1. MERGE (auto-approve silent): same_contract - same chain+contract on different globals
2. MERGE (auto-approve silent): same_symbol with price diff <= 1%
3. NOTIFY ADMIN: same_symbol with price diff 1% < diff <= 3%
4. NO MERGE: same_symbol with chain+contract conflict (same chain, different contracts)
5. NO MERGE: same_symbol with price diff > 3%
6. NO MERGE: same_symbol with exchange conflict (same exchange, different base_asset)
"""
import uuid
from dataclasses import dataclass, field
from typing import Optional
from uuid import UUID

import pytest

from infrastructure.database.models.base import MarketType, SourceType


# =============================================================================
# Test Data Structures (mock models for unit testing)
# =============================================================================

@dataclass
class MockChain:
    """Mock Chain model."""
    id: UUID = field(default_factory=uuid.uuid4)
    name: str = ""


@dataclass
class MockSource:
    """Mock Source model."""
    id: UUID = field(default_factory=uuid.uuid4)
    name: str = ""
    type: SourceType = SourceType.CEX


@dataclass
class MockToken:
    """Mock Token model."""
    id: UUID = field(default_factory=uuid.uuid4)
    symbol: str = ""
    base_asset: str = ""
    quote_asset: str = "USDT"
    market_type: MarketType = MarketType.SPOT
    source_id: UUID = field(default_factory=uuid.uuid4)
    source: Optional[MockSource] = None


@dataclass
class MockTokenOnChain:
    """Mock TokenOnChain model."""
    id: UUID = field(default_factory=uuid.uuid4)
    token_id: UUID = field(default_factory=uuid.uuid4)
    chain_id: UUID = field(default_factory=uuid.uuid4)
    contract_address: str = ""
    global_token_id: UUID = field(default_factory=uuid.uuid4)
    token: Optional[MockToken] = None
    chain: Optional[MockChain] = None


@dataclass
class MockTokenPrice:
    """Mock TokenPrice model."""
    id: UUID = field(default_factory=uuid.uuid4)
    price_usd: float = 0.0
    orderbook: dict = field(default_factory=dict)
    market_type: MarketType = MarketType.SPOT
    global_token_id: UUID = field(default_factory=uuid.uuid4)


@dataclass
class MockGlobalToken:
    """Mock GlobalToken model."""
    id: UUID = field(default_factory=uuid.uuid4)
    symbol: str = ""
    is_deleted: bool = False
    token_on_chains: list[MockTokenOnChain] = field(default_factory=list)
    prices: list[MockTokenPrice] = field(default_factory=list)
    sources: list[MockSource] = field(default_factory=list)


# =============================================================================
# Test Data Factory
# =============================================================================

class TestDataFactory:
    """Factory for creating test data for merge scenarios."""

    @staticmethod
    def create_chain(name: str) -> MockChain:
        return MockChain(id=uuid.uuid4(), name=name)

    @staticmethod
    def create_source(name: str, source_type: SourceType = SourceType.CEX) -> MockSource:
        return MockSource(id=uuid.uuid4(), name=name, type=source_type)

    @staticmethod
    def create_token(
        symbol: str,
        base_asset: str,
        source: MockSource,
        quote_asset: str = "USDT",
        market_type: MarketType = MarketType.SPOT
    ) -> MockToken:
        return MockToken(
            id=uuid.uuid4(),
            symbol=symbol,
            base_asset=base_asset,
            quote_asset=quote_asset,
            market_type=market_type,
            source_id=source.id,
            source=source
        )

    @staticmethod
    def create_token_on_chain(
        token: MockToken,
        chain: MockChain,
        contract_address: str,
        global_token_id: UUID
    ) -> MockTokenOnChain:
        return MockTokenOnChain(
            id=uuid.uuid4(),
            token_id=token.id,
            chain_id=chain.id,
            contract_address=contract_address,
            global_token_id=global_token_id,
            token=token,
            chain=chain
        )

    @staticmethod
    def create_price(
        global_token_id: UUID,
        bid_price: float,
        ask_price: float,
        market_type: MarketType = MarketType.SPOT
    ) -> MockTokenPrice:
        """Create price with orderbook for mid-price calculation."""
        return MockTokenPrice(
            id=uuid.uuid4(),
            price_usd=(bid_price + ask_price) / 2,
            orderbook={
                "bids": [[str(bid_price), "1.0"]],
                "asks": [[str(ask_price), "1.0"]]
            },
            market_type=market_type,
            global_token_id=global_token_id
        )

    @staticmethod
    def create_global_token(
        symbol: str,
        sources: list[MockSource],
        token_on_chains: list[MockTokenOnChain],
        prices: list[MockTokenPrice]
    ) -> MockGlobalToken:
        gt_id = uuid.uuid4()
        # Update references
        for toc in token_on_chains:
            toc.global_token_id = gt_id
        for price in prices:
            price.global_token_id = gt_id
        return MockGlobalToken(
            id=gt_id,
            symbol=symbol,
            is_deleted=False,
            token_on_chains=token_on_chains,
            prices=prices,
            sources=sources
        )


# =============================================================================
# Merge Scenarios Test Data
# =============================================================================

class MergeScenarios:
    """
    Test data for all merge scenarios.

    Scenarios:
    1. MERGE (silent): Different base_asset, no exchange conflict, same chain+contract (non-empty)
    2. MERGE (silent): Same base_asset, no conflicts, price diff <= 1%
    3. NOTIFY: Same base_asset, no conflicts, 1% < price diff <= 3%
    4. NO MERGE: Same base_asset, chain+contract conflict (same chain, different contracts)
    5. NO MERGE: Same base_asset, price diff > 3%
    6. NO MERGE: Same base_asset, exchange conflict (same exchange, different base_asset)
    """

    def __init__(self):
        self.factory = TestDataFactory()
        self._setup_shared_entities()

    def _setup_shared_entities(self):
        """Create shared chains and sources used across scenarios."""
        # Chains
        self.chain_ethereum = self.factory.create_chain("ethereum")
        self.chain_bsc = self.factory.create_chain("bsc")
        self.chain_polygon = self.factory.create_chain("polygon")
        self.chain_arbitrum = self.factory.create_chain("arbitrum")

        # Sources (exchanges)
        self.source_binance = self.factory.create_source("binance")
        self.source_okx = self.factory.create_source("okx")
        self.source_bybit = self.factory.create_source("bybit")
        self.source_gate = self.factory.create_source("gate")
        self.source_kucoin = self.factory.create_source("kucoin")
        self.source_mexc = self.factory.create_source("mexc")

    # =========================================================================
    # Scenario 1: MERGE (silent) - Same contract on different chains
    # Different base_asset, no exchange conflict, same chain+contract (non-empty)
    # =========================================================================

    def scenario_1_merge_same_contract(self) -> dict:
        """
        Scenario: Different symbols but SAME contract address on same chain.
        Expected: Auto-merge (silent) because same_contract is highest priority.

        Global A: WBTC on Binance, ethereum 0xabc123
        Global B: BTC on OKX, ethereum 0xabc123 (same contract!)

        Result: MERGE (same_contract reason)
        """
        # Contract address (same for both globals)
        contract = "0x2260fac5e5542a773aa44fbcfecd7e6f3b5e5a40"

        # Global A: WBTC on Binance
        token_a = self.factory.create_token("WBTCUSDT", "WBTC", self.source_binance)
        toc_a = self.factory.create_token_on_chain(
            token_a, self.chain_ethereum, contract, uuid.uuid4()
        )
        price_a = self.factory.create_price(uuid.uuid4(), 45000.0, 45010.0)
        global_a = self.factory.create_global_token(
            "WBTC", [self.source_binance], [toc_a], [price_a]
        )

        # Global B: BTC on OKX (different exchange, different symbol, SAME contract)
        token_b = self.factory.create_token("BTCUSDT", "BTC", self.source_okx)
        toc_b = self.factory.create_token_on_chain(
            token_b, self.chain_ethereum, contract, uuid.uuid4()
        )
        price_b = self.factory.create_price(uuid.uuid4(), 45050.0, 45060.0)
        global_b = self.factory.create_global_token(
            "BTC", [self.source_okx], [toc_b], [price_b]
        )

        return {
            "name": "scenario_1_merge_same_contract",
            "description": "Different base_asset, same contract on ethereum -> MERGE (silent)",
            "global_a": global_a,
            "global_b": global_b,
            "expected_result": "MERGE_SILENT",
            "expected_reason": "same_contract",
            "price_diff_pct": abs(45005.0 - 45055.0) / ((45005.0 + 45055.0) / 2) * 100,
        }

    # =========================================================================
    # Scenario 2: MERGE (silent) - Same symbol, price diff <= 1%
    # =========================================================================

    def scenario_2_merge_same_symbol_small_diff(self) -> dict:
        """
        Scenario: Same base_asset, no conflicts, price difference <= 1%.
        Expected: Auto-merge (silent).

        Global A: BTC on Binance, ethereum 0xaaa
        Global B: BTC on OKX, bsc 0xbbb (different chain, different contract)

        Prices: 45000 vs 45200 (0.44% diff)

        Result: MERGE (same_symbol reason, diff <= 1%)
        """
        # Global A: BTC on Binance, Ethereum chain
        token_a = self.factory.create_token("BTCUSDT", "BTC", self.source_binance)
        toc_a = self.factory.create_token_on_chain(
            token_a, self.chain_ethereum,
            "0xaaaa1111aaaa1111aaaa1111aaaa1111aaaa1111",
            uuid.uuid4()
        )
        price_a = self.factory.create_price(uuid.uuid4(), 44995.0, 45005.0)  # mid = 45000
        global_a = self.factory.create_global_token(
            "BTC", [self.source_binance], [toc_a], [price_a]
        )

        # Global B: BTC on OKX, BSC chain (different exchange, different chain)
        token_b = self.factory.create_token("BTCUSDT", "BTC", self.source_okx)
        toc_b = self.factory.create_token_on_chain(
            token_b, self.chain_bsc,
            "0xbbbb2222bbbb2222bbbb2222bbbb2222bbbb2222",
            uuid.uuid4()
        )
        price_b = self.factory.create_price(uuid.uuid4(), 45195.0, 45205.0)  # mid = 45200
        global_b = self.factory.create_global_token(
            "BTC", [self.source_okx], [toc_b], [price_b]
        )

        mid_a = 45000.0
        mid_b = 45200.0
        diff_pct = abs(mid_a - mid_b) / ((mid_a + mid_b) / 2) * 100  # 0.44%

        return {
            "name": "scenario_2_merge_same_symbol_small_diff",
            "description": "Same base_asset (BTC), different chains, price diff 0.44% -> MERGE (silent)",
            "global_a": global_a,
            "global_b": global_b,
            "expected_result": "MERGE_SILENT",
            "expected_reason": "same_symbol",
            "price_diff_pct": diff_pct,
        }

    # =========================================================================
    # Scenario 3: NOTIFY - Same symbol, 1% < price diff <= 3%
    # =========================================================================

    def scenario_3_notify_same_symbol_medium_diff(self) -> dict:
        """
        Scenario: Same base_asset, no conflicts, price difference between 1% and 3%.
        Expected: Notify admin for manual review.

        Global A: ETH on Binance, ethereum 0xccc
        Global B: ETH on Gate, polygon 0xddd

        Prices: 3000 vs 3060 (2% diff)

        Result: NOTIFY ADMIN (same_symbol reason, 1% < diff <= 3%)
        """
        # Global A: ETH on Binance
        token_a = self.factory.create_token("ETHUSDT", "ETH", self.source_binance)
        toc_a = self.factory.create_token_on_chain(
            token_a, self.chain_ethereum,
            "0xcccc3333cccc3333cccc3333cccc3333cccc3333",
            uuid.uuid4()
        )
        price_a = self.factory.create_price(uuid.uuid4(), 2995.0, 3005.0)  # mid = 3000
        global_a = self.factory.create_global_token(
            "ETH", [self.source_binance], [toc_a], [price_a]
        )

        # Global B: ETH on Gate, Polygon chain
        token_b = self.factory.create_token("ETHUSDT", "ETH", self.source_gate)
        toc_b = self.factory.create_token_on_chain(
            token_b, self.chain_polygon,
            "0xdddd4444dddd4444dddd4444dddd4444dddd4444",
            uuid.uuid4()
        )
        price_b = self.factory.create_price(uuid.uuid4(), 3055.0, 3065.0)  # mid = 3060
        global_b = self.factory.create_global_token(
            "ETH", [self.source_gate], [toc_b], [price_b]
        )

        mid_a = 3000.0
        mid_b = 3060.0
        diff_pct = abs(mid_a - mid_b) / ((mid_a + mid_b) / 2) * 100  # 1.98%

        return {
            "name": "scenario_3_notify_same_symbol_medium_diff",
            "description": "Same base_asset (ETH), price diff 1.98% -> NOTIFY ADMIN",
            "global_a": global_a,
            "global_b": global_b,
            "expected_result": "NOTIFY_ADMIN",
            "expected_reason": "same_symbol",
            "price_diff_pct": diff_pct,
        }

    # =========================================================================
    # Scenario 4: NO MERGE - Same symbol, chain+contract conflict
    # Same chain, different non-empty contracts
    # =========================================================================

    def scenario_4_no_merge_contract_conflict(self) -> dict:
        """
        Scenario: Same base_asset, SAME CHAIN but DIFFERENT contracts.
        This is a contract conflict - likely different tokens with same ticker.
        Expected: No merge.

        Global A: PEPE on Binance, ethereum 0x111 (real PEPE)
        Global B: PEPE on OKX, ethereum 0x222 (fake PEPE)

        Both on ethereum but different contracts = CONFLICT

        Result: NO MERGE (contract conflict on same chain)
        """
        # Global A: PEPE on Binance, real contract
        token_a = self.factory.create_token("PEPEUSDT", "PEPE", self.source_binance)
        toc_a = self.factory.create_token_on_chain(
            token_a, self.chain_ethereum,
            "0x6982508145454ce325ddbe47a25d4ec3d2311933",  # Real PEPE
            uuid.uuid4()
        )
        price_a = self.factory.create_price(uuid.uuid4(), 0.00001495, 0.00001505)
        global_a = self.factory.create_global_token(
            "PEPE", [self.source_binance], [toc_a], [price_a]
        )

        # Global B: PEPE on OKX, DIFFERENT contract (fake PEPE)
        token_b = self.factory.create_token("PEPEUSDT", "PEPE", self.source_okx)
        toc_b = self.factory.create_token_on_chain(
            token_b, self.chain_ethereum,  # SAME CHAIN
            "0xfake0000fake0000fake0000fake0000fake0000",  # DIFFERENT CONTRACT
            uuid.uuid4()
        )
        price_b = self.factory.create_price(uuid.uuid4(), 0.00001490, 0.00001510)
        global_b = self.factory.create_global_token(
            "PEPE", [self.source_okx], [toc_b], [price_b]
        )

        return {
            "name": "scenario_4_no_merge_contract_conflict",
            "description": "Same base_asset (PEPE), same chain, DIFFERENT contracts -> NO MERGE",
            "global_a": global_a,
            "global_b": global_b,
            "expected_result": "NO_MERGE",
            "expected_reason": "contract_conflict",
            "conflict_details": {
                "chain": "ethereum",
                "contract_a": "0x6982508145454ce325ddbe47a25d4ec3d2311933",
                "contract_b": "0xfake0000fake0000fake0000fake0000fake0000",
            }
        }

    # =========================================================================
    # Scenario 5: NO MERGE - Same symbol, price diff > 3%
    # =========================================================================

    def scenario_5_no_merge_large_price_diff(self) -> dict:
        """
        Scenario: Same base_asset, no conflicts, but price difference > 3%.
        Expected: No merge (likely different tokens or stale data).

        Global A: SOL on Binance - price $150
        Global B: SOL on KuCoin - price $160 (6.45% diff)

        Result: NO MERGE (price diff exceeds threshold)
        """
        # Global A: SOL on Binance
        token_a = self.factory.create_token("SOLUSDT", "SOL", self.source_binance)
        toc_a = self.factory.create_token_on_chain(
            token_a, self.chain_ethereum,
            "0xsol111sol111sol111sol111sol111sol111",
            uuid.uuid4()
        )
        price_a = self.factory.create_price(uuid.uuid4(), 149.5, 150.5)  # mid = 150
        global_a = self.factory.create_global_token(
            "SOL", [self.source_binance], [toc_a], [price_a]
        )

        # Global B: SOL on KuCoin with significantly different price
        token_b = self.factory.create_token("SOLUSDT", "SOL", self.source_kucoin)
        toc_b = self.factory.create_token_on_chain(
            token_b, self.chain_bsc,
            "0xsol222sol222sol222sol222sol222sol222",
            uuid.uuid4()
        )
        price_b = self.factory.create_price(uuid.uuid4(), 159.5, 160.5)  # mid = 160
        global_b = self.factory.create_global_token(
            "SOL", [self.source_kucoin], [toc_b], [price_b]
        )

        mid_a = 150.0
        mid_b = 160.0
        diff_pct = abs(mid_a - mid_b) / ((mid_a + mid_b) / 2) * 100  # 6.45%

        return {
            "name": "scenario_5_no_merge_large_price_diff",
            "description": "Same base_asset (SOL), price diff 6.45% > 3% -> NO MERGE",
            "global_a": global_a,
            "global_b": global_b,
            "expected_result": "NO_MERGE",
            "expected_reason": "price_diff_too_large",
            "price_diff_pct": diff_pct,
        }

    # =========================================================================
    # Scenario 6: NO MERGE - Exchange conflict
    # Same exchange has different base_asset in each global
    # =========================================================================

    def scenario_6_no_merge_exchange_conflict(self) -> dict:
        """
        Scenario: Same base_asset, but exchange has DIFFERENT base_asset for each global.
        This means the exchange already has both tokens listed separately.
        Expected: No merge.

        Global A: DOGE on Binance + OKX
        Global B: DOGE on Binance + Gate (Binance overlaps!)

        If Binance is in both, they can't be merged (Binance knows they're different).

        Result: NO MERGE (exchange conflict - same source in both globals)
        """
        # Global A: DOGE on Binance and OKX
        token_a1 = self.factory.create_token("DOGEUSDT", "DOGE", self.source_binance)
        toc_a1 = self.factory.create_token_on_chain(
            token_a1, self.chain_ethereum,
            "0xdoge111doge111doge111doge111doge111",
            uuid.uuid4()
        )
        token_a2 = self.factory.create_token("DOGEUSDT", "DOGE", self.source_okx)
        toc_a2 = self.factory.create_token_on_chain(
            token_a2, self.chain_bsc,
            "0xdoge222doge222doge222doge222doge222",
            uuid.uuid4()
        )
        price_a = self.factory.create_price(uuid.uuid4(), 0.1495, 0.1505)  # mid = 0.15
        global_a = self.factory.create_global_token(
            "DOGE",
            [self.source_binance, self.source_okx],
            [toc_a1, toc_a2],
            [price_a]
        )

        # Global B: DOGE on Binance (OVERLAP!) and Gate
        token_b1 = self.factory.create_token("DOGEUSDT", "DOGE", self.source_binance)
        toc_b1 = self.factory.create_token_on_chain(
            token_b1, self.chain_polygon,
            "0xdoge333doge333doge333doge333doge333",
            uuid.uuid4()
        )
        token_b2 = self.factory.create_token("DOGEUSDT", "DOGE", self.source_gate)
        toc_b2 = self.factory.create_token_on_chain(
            token_b2, self.chain_arbitrum,
            "0xdoge444doge444doge444doge444doge444",
            uuid.uuid4()
        )
        price_b = self.factory.create_price(uuid.uuid4(), 0.1490, 0.1510)  # mid = 0.15
        global_b = self.factory.create_global_token(
            "DOGE",
            [self.source_binance, self.source_gate],  # Binance overlaps!
            [toc_b1, toc_b2],
            [price_b]
        )

        return {
            "name": "scenario_6_no_merge_exchange_conflict",
            "description": "Same base_asset (DOGE), Binance in BOTH globals -> NO MERGE",
            "global_a": global_a,
            "global_b": global_b,
            "expected_result": "NO_MERGE",
            "expected_reason": "exchange_conflict",
            "conflict_details": {
                "overlapping_exchanges": ["binance"],
            }
        }

    # =========================================================================
    # Additional edge case scenarios
    # =========================================================================

    def scenario_7_merge_empty_contract_no_conflict(self) -> dict:
        """
        Scenario: Same chain, but contracts are EMPTY (not populated).
        Empty contracts should NOT be considered a conflict.
        Expected: Consider as same_symbol candidate, may merge.

        Global A: XRP on Binance, ethereum "" (empty contract)
        Global B: XRP on OKX, ethereum "" (empty contract)

        Empty contracts are not conflicts.

        Result: Candidate for merge (same_symbol), decision based on price diff
        """
        # Global A: XRP on Binance, empty contract
        token_a = self.factory.create_token("XRPUSDT", "XRP", self.source_binance)
        toc_a = self.factory.create_token_on_chain(
            token_a, self.chain_ethereum,
            "",  # Empty contract
            uuid.uuid4()
        )
        price_a = self.factory.create_price(uuid.uuid4(), 0.595, 0.605)  # mid = 0.60
        global_a = self.factory.create_global_token(
            "XRP", [self.source_binance], [toc_a], [price_a]
        )

        # Global B: XRP on OKX, empty contract
        token_b = self.factory.create_token("XRPUSDT", "XRP", self.source_okx)
        toc_b = self.factory.create_token_on_chain(
            token_b, self.chain_ethereum,  # Same chain
            "",  # Empty contract
            uuid.uuid4()
        )
        price_b = self.factory.create_price(uuid.uuid4(), 0.594, 0.606)  # mid = 0.60
        global_b = self.factory.create_global_token(
            "XRP", [self.source_okx], [toc_b], [price_b]
        )

        return {
            "name": "scenario_7_merge_empty_contract_no_conflict",
            "description": "Same base_asset (XRP), same chain, EMPTY contracts -> NOT a conflict",
            "global_a": global_a,
            "global_b": global_b,
            "expected_result": "MERGE_SILENT",
            "expected_reason": "same_symbol",
            "note": "Empty contracts don't count as conflict"
        }

    def scenario_8_merge_priority_contract_over_symbol(self) -> dict:
        """
        Scenario: Tokens match by BOTH same_contract AND same_symbol.
        Expected: same_contract takes priority (rank 0 vs 1).

        Global A: BTC on Binance, ethereum 0xwbtc
        Global B: BTC on OKX, ethereum 0xwbtc (same contract)

        Result: MERGE with reason "same_contract,same_symbol"
        """
        contract = "0x2260fac5e5542a773aa44fbcfecd7e6f3b5e5a40"

        # Global A: BTC on Binance
        token_a = self.factory.create_token("BTCUSDT", "BTC", self.source_binance)
        toc_a = self.factory.create_token_on_chain(
            token_a, self.chain_ethereum, contract, uuid.uuid4()
        )
        price_a = self.factory.create_price(uuid.uuid4(), 44995.0, 45005.0)
        global_a = self.factory.create_global_token(
            "BTC", [self.source_binance], [toc_a], [price_a]
        )

        # Global B: BTC on OKX (same symbol, same contract)
        token_b = self.factory.create_token("BTCUSDT", "BTC", self.source_okx)
        toc_b = self.factory.create_token_on_chain(
            token_b, self.chain_ethereum, contract, uuid.uuid4()
        )
        price_b = self.factory.create_price(uuid.uuid4(), 45045.0, 45055.0)
        global_b = self.factory.create_global_token(
            "BTC", [self.source_okx], [toc_b], [price_b]
        )

        return {
            "name": "scenario_8_merge_priority_contract_over_symbol",
            "description": "Same symbol + same contract -> MERGE with combined reason",
            "global_a": global_a,
            "global_b": global_b,
            "expected_result": "MERGE_SILENT",
            "expected_reason": "same_contract,same_symbol",
        }

    def get_all_scenarios(self) -> list[dict]:
        """Return all test scenarios."""
        return [
            self.scenario_1_merge_same_contract(),
            self.scenario_2_merge_same_symbol_small_diff(),
            self.scenario_3_notify_same_symbol_medium_diff(),
            self.scenario_4_no_merge_contract_conflict(),
            self.scenario_5_no_merge_large_price_diff(),
            self.scenario_6_no_merge_exchange_conflict(),
            self.scenario_7_merge_empty_contract_no_conflict(),
            self.scenario_8_merge_priority_contract_over_symbol(),
        ]


# =============================================================================
# Helper functions for merge logic validation
# =============================================================================

# Default multiplied token prefixes (from MergeConfig)
DEFAULT_MULTIPLIED_PREFIXES = ("1000000", "1000", "100", "10")


def is_multiplied_token_pair(
    base_a: str,
    base_b: str,
    prefixes: tuple[str, ...] = DEFAULT_MULTIPLIED_PREFIXES
) -> bool:
    """
    Check if two base assets are a multiplied token pair.

    Examples:
        - MOG vs 1000000MOG -> True
        - BONK vs 1000BONK -> True
    """
    base_a_upper = base_a.upper()
    base_b_upper = base_b.upper()

    for prefix in prefixes:
        if base_a_upper == f"{prefix}{base_b_upper}":
            return True
        if base_b_upper == f"{prefix}{base_a_upper}":
            return True
    return False


def check_price_ratio_valid(
    price_a: float,
    price_b: float,
    max_ratio: float = 10.0
) -> bool:
    """
    Check if price ratio between two tokens is within acceptable range.

    Returns True if ratio <= max_ratio.
    """
    if price_a <= 0 or price_b <= 0:
        return False
    ratio = max(price_a, price_b) / min(price_a, price_b)
    return ratio <= max_ratio


def compute_mid_price(global_token: MockGlobalToken) -> Optional[float]:
    """Compute mid-price from orderbook (same logic as MergeCandidateFinder)."""
    best_bid = None
    best_ask = None

    for price in global_token.prices or []:
        ob = price.orderbook or {}
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []

        if bids:
            for b in bids:
                if b and b[0] is not None:
                    bid_val = float(b[0])
                    best_bid = max(best_bid, bid_val) if best_bid is not None else bid_val

        if asks:
            for a in asks:
                if a and a[0] is not None:
                    ask_val = float(a[0])
                    best_ask = min(best_ask, ask_val) if best_ask is not None else ask_val

    if best_bid is None or best_ask is None:
        return None

    return (best_bid + best_ask) / 2


def check_exchange_conflict(global_a: MockGlobalToken, global_b: MockGlobalToken) -> set[str]:
    """Check for overlapping exchanges between two globals."""
    exchanges_a = {s.name.lower() for s in global_a.sources or []}
    exchanges_b = {s.name.lower() for s in global_b.sources or []}
    return exchanges_a & exchanges_b


def check_contract_conflict(global_a: MockGlobalToken, global_b: MockGlobalToken) -> list[dict]:
    """
    Check for contract conflicts (same chain, different non-empty contracts).

    Returns list of conflicts: [{"chain": ..., "contract_a": ..., "contract_b": ...}]
    """
    conflicts = []

    # Build map: chain_name -> contract for global_a
    chain_contracts_a = {}
    for toc in global_a.token_on_chains or []:
        chain_name = (toc.chain.name or "").lower() if toc.chain else ""
        contract = (toc.contract_address or "").lower()
        if chain_name and contract:  # Only non-empty
            chain_contracts_a.setdefault(chain_name, set()).add(contract)

    # Check global_b for conflicts
    for toc in global_b.token_on_chains or []:
        chain_name = (toc.chain.name or "").lower() if toc.chain else ""
        contract = (toc.contract_address or "").lower()
        if chain_name and contract:  # Only non-empty
            if chain_name in chain_contracts_a:
                # Same chain - check if contracts differ
                if contract not in chain_contracts_a[chain_name]:
                    conflicts.append({
                        "chain": chain_name,
                        "contract_a": list(chain_contracts_a[chain_name])[0],
                        "contract_b": contract
                    })

    return conflicts


def check_same_contract(global_a: MockGlobalToken, global_b: MockGlobalToken) -> bool:
    """Check if globals share same chain+contract (non-empty)."""
    # Build set of (chain, contract) for global_a
    pairs_a = set()
    for toc in global_a.token_on_chains or []:
        chain_name = (toc.chain.name or "").lower() if toc.chain else ""
        contract = (toc.contract_address or "").lower()
        if chain_name and contract:
            pairs_a.add((chain_name, contract))

    # Check global_b
    for toc in global_b.token_on_chains or []:
        chain_name = (toc.chain.name or "").lower() if toc.chain else ""
        contract = (toc.contract_address or "").lower()
        if chain_name and contract:
            if (chain_name, contract) in pairs_a:
                return True

    return False


def check_same_symbol(global_a: MockGlobalToken, global_b: MockGlobalToken) -> bool:
    """Check if globals have same base_asset."""
    # Extract base_assets from tokens
    base_assets_a = set()
    for toc in global_a.token_on_chains or []:
        if toc.token:
            base_assets_a.add((toc.token.base_asset or "").upper())

    base_assets_b = set()
    for toc in global_b.token_on_chains or []:
        if toc.token:
            base_assets_b.add((toc.token.base_asset or "").upper())

    return bool(base_assets_a & base_assets_b)


def evaluate_merge_candidate(
    global_a: MockGlobalToken,
    global_b: MockGlobalToken,
    max_price_diff_pct: float = 3.0,
    auto_approve_silent_threshold: float = 1.0
) -> dict:
    """
    Evaluate if two globals should be merged.

    Returns:
        {
            "should_merge": bool,
            "result": "MERGE_SILENT" | "NOTIFY_ADMIN" | "NO_MERGE",
            "reasons": set[str],  # same_contract, same_symbol
            "rejection_reason": str | None,
            "price_diff_pct": float | None
        }
    """
    result = {
        "should_merge": False,
        "result": "NO_MERGE",
        "reasons": set(),
        "rejection_reason": None,
        "price_diff_pct": None
    }

    # Check for same contract match
    if check_same_contract(global_a, global_b):
        result["reasons"].add("same_contract")

    # Check for same symbol match
    if check_same_symbol(global_a, global_b):
        result["reasons"].add("same_symbol")

    # No match criteria
    if not result["reasons"]:
        result["rejection_reason"] = "no_match_criteria"
        return result

    # Check exchange conflict
    overlapping = check_exchange_conflict(global_a, global_b)
    if overlapping:
        result["rejection_reason"] = f"exchange_conflict: {overlapping}"
        return result

    # Check contract conflict (only if same_symbol but NOT same_contract)
    if "same_symbol" in result["reasons"] and "same_contract" not in result["reasons"]:
        conflicts = check_contract_conflict(global_a, global_b)
        if conflicts:
            result["rejection_reason"] = f"contract_conflict: {conflicts}"
            return result

    # Calculate price difference
    price_a = compute_mid_price(global_a)
    price_b = compute_mid_price(global_b)

    if price_a is None or price_b is None:
        result["rejection_reason"] = "missing_price"
        return result

    price_diff_pct = abs(price_a - price_b) / ((price_a + price_b) / 2) * 100
    result["price_diff_pct"] = price_diff_pct

    # Check price threshold
    if price_diff_pct > max_price_diff_pct:
        result["rejection_reason"] = f"price_diff_too_large: {price_diff_pct:.2f}%"
        return result

    # Determine result type
    result["should_merge"] = True

    if "same_contract" in result["reasons"]:
        # same_contract always auto-approves silently
        result["result"] = "MERGE_SILENT"
    elif price_diff_pct <= auto_approve_silent_threshold:
        # same_symbol with small diff - auto-approve silently
        result["result"] = "MERGE_SILENT"
    else:
        # same_symbol with larger diff - notify admin
        result["result"] = "NOTIFY_ADMIN"

    return result


# =============================================================================
# Tests
# =============================================================================

class TestMergeScenarios:
    """Test all merge scenarios."""

    @pytest.fixture
    def scenarios(self):
        return MergeScenarios()

    def test_scenario_1_merge_same_contract(self, scenarios):
        """Test: Different base_asset, same contract -> MERGE (silent)."""
        data = scenarios.scenario_1_merge_same_contract()
        result = evaluate_merge_candidate(data["global_a"], data["global_b"])

        assert result["should_merge"] is True
        assert result["result"] == "MERGE_SILENT"
        assert "same_contract" in result["reasons"]

    def test_scenario_2_merge_same_symbol_small_diff(self, scenarios):
        """Test: Same base_asset, price diff <= 1% -> MERGE (silent)."""
        data = scenarios.scenario_2_merge_same_symbol_small_diff()
        result = evaluate_merge_candidate(data["global_a"], data["global_b"])

        assert result["should_merge"] is True
        assert result["result"] == "MERGE_SILENT"
        assert "same_symbol" in result["reasons"]
        assert result["price_diff_pct"] < 1.0

    def test_scenario_3_notify_same_symbol_medium_diff(self, scenarios):
        """Test: Same base_asset, 1% < price diff <= 3% -> NOTIFY ADMIN."""
        data = scenarios.scenario_3_notify_same_symbol_medium_diff()
        result = evaluate_merge_candidate(data["global_a"], data["global_b"])

        assert result["should_merge"] is True
        assert result["result"] == "NOTIFY_ADMIN"
        assert "same_symbol" in result["reasons"]
        assert 1.0 < result["price_diff_pct"] <= 3.0

    def test_scenario_4_no_merge_contract_conflict(self, scenarios):
        """Test: Same base_asset, contract conflict -> NO MERGE."""
        data = scenarios.scenario_4_no_merge_contract_conflict()
        result = evaluate_merge_candidate(data["global_a"], data["global_b"])

        assert result["should_merge"] is False
        assert result["result"] == "NO_MERGE"
        assert "contract_conflict" in result["rejection_reason"]

    def test_scenario_5_no_merge_large_price_diff(self, scenarios):
        """Test: Same base_asset, price diff > 3% -> NO MERGE."""
        data = scenarios.scenario_5_no_merge_large_price_diff()
        result = evaluate_merge_candidate(data["global_a"], data["global_b"])

        assert result["should_merge"] is False
        assert result["result"] == "NO_MERGE"
        assert "price_diff_too_large" in result["rejection_reason"]
        assert result["price_diff_pct"] > 3.0

    def test_scenario_6_no_merge_exchange_conflict(self, scenarios):
        """Test: Same base_asset, exchange conflict -> NO MERGE."""
        data = scenarios.scenario_6_no_merge_exchange_conflict()
        result = evaluate_merge_candidate(data["global_a"], data["global_b"])

        assert result["should_merge"] is False
        assert result["result"] == "NO_MERGE"
        assert "exchange_conflict" in result["rejection_reason"]

    def test_scenario_7_empty_contracts_no_conflict(self, scenarios):
        """Test: Empty contracts don't count as conflict."""
        data = scenarios.scenario_7_merge_empty_contract_no_conflict()
        result = evaluate_merge_candidate(data["global_a"], data["global_b"])

        assert result["should_merge"] is True
        assert result["result"] == "MERGE_SILENT"
        # Empty contracts mean no contract conflict

    def test_scenario_8_priority_contract_over_symbol(self, scenarios):
        """Test: same_contract takes priority over same_symbol."""
        data = scenarios.scenario_8_merge_priority_contract_over_symbol()
        result = evaluate_merge_candidate(data["global_a"], data["global_b"])

        assert result["should_merge"] is True
        assert result["result"] == "MERGE_SILENT"
        assert "same_contract" in result["reasons"]
        assert "same_symbol" in result["reasons"]


class TestHelperFunctions:
    """Test helper functions for merge logic."""

    @pytest.fixture
    def scenarios(self):
        return MergeScenarios()

    def test_compute_mid_price(self, scenarios):
        """Test mid-price calculation from orderbook."""
        data = scenarios.scenario_2_merge_same_symbol_small_diff()
        price = compute_mid_price(data["global_a"])
        assert price == 45000.0

    def test_check_exchange_conflict_detected(self, scenarios):
        """Test exchange conflict detection."""
        data = scenarios.scenario_6_no_merge_exchange_conflict()
        overlap = check_exchange_conflict(data["global_a"], data["global_b"])
        assert "binance" in overlap

    def test_check_exchange_conflict_none(self, scenarios):
        """Test no exchange conflict."""
        data = scenarios.scenario_2_merge_same_symbol_small_diff()
        overlap = check_exchange_conflict(data["global_a"], data["global_b"])
        assert len(overlap) == 0

    def test_check_contract_conflict_detected(self, scenarios):
        """Test contract conflict detection."""
        data = scenarios.scenario_4_no_merge_contract_conflict()
        conflicts = check_contract_conflict(data["global_a"], data["global_b"])
        assert len(conflicts) > 0
        assert conflicts[0]["chain"] == "ethereum"

    def test_check_same_contract(self, scenarios):
        """Test same contract detection."""
        data = scenarios.scenario_1_merge_same_contract()
        assert check_same_contract(data["global_a"], data["global_b"]) is True

    def test_check_same_symbol(self, scenarios):
        """Test same symbol detection."""
        data = scenarios.scenario_2_merge_same_symbol_small_diff()
        assert check_same_symbol(data["global_a"], data["global_b"]) is True


class TestMultipliedTokens:
    """Test multiplied token detection (1000X, 1000000X tokens)."""

    def test_multiplied_token_1000x(self):
        """Test: 1000BONK vs BONK should be detected as multiplied pair."""
        assert is_multiplied_token_pair("BONK", "1000BONK") is True
        assert is_multiplied_token_pair("1000BONK", "BONK") is True

    def test_multiplied_token_1000000x(self):
        """Test: 1000000MOG vs MOG should be detected as multiplied pair."""
        assert is_multiplied_token_pair("MOG", "1000000MOG") is True
        assert is_multiplied_token_pair("1000000MOG", "MOG") is True

    def test_multiplied_token_1000_btt(self):
        """Test: 1000BTT vs BTT should be detected."""
        assert is_multiplied_token_pair("BTT", "1000BTT") is True

    def test_not_multiplied_different_tokens(self):
        """Test: Different tokens should not be detected as multiplied."""
        assert is_multiplied_token_pair("BTC", "ETH") is False
        assert is_multiplied_token_pair("TRUMP", "TRUMPSOL") is False
        assert is_multiplied_token_pair("DOGE", "SHIB") is False

    def test_not_multiplied_similar_names(self):
        """Test: Similar names without numeric prefix are not multiplied."""
        assert is_multiplied_token_pair("MOG", "MOGCOIN") is False
        assert is_multiplied_token_pair("PEPE", "PEPE2") is False

    def test_price_ratio_valid(self):
        """Test: Price ratio within 10x is valid."""
        assert check_price_ratio_valid(100.0, 95.0, max_ratio=10.0) is True
        assert check_price_ratio_valid(100.0, 50.0, max_ratio=10.0) is True
        assert check_price_ratio_valid(100.0, 15.0, max_ratio=10.0) is True

    def test_price_ratio_invalid(self):
        """Test: Price ratio > 10x is invalid (1000X tokens)."""
        assert check_price_ratio_valid(0.22, 0.00000022, max_ratio=10.0) is False  # MOG vs 1000000MOG
        assert check_price_ratio_valid(0.065, 0.000065, max_ratio=10.0) is False  # RATS vs 1000RATS
        assert check_price_ratio_valid(536.0, 0.00024, max_ratio=10.0) is False  # MASTOCK vs MA


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def factory(self):
        return TestDataFactory()

    def test_no_prices_returns_no_merge(self, factory):
        """Test: No prices -> NO MERGE."""
        source_a = factory.create_source("binance")
        source_b = factory.create_source("okx")
        chain_a = factory.create_chain("ethereum")
        chain_b = factory.create_chain("bsc")  # Different chains to avoid contract conflict

        token_a = factory.create_token("BTCUSDT", "BTC", source_a)
        toc_a = factory.create_token_on_chain(token_a, chain_a, "0xabc", uuid.uuid4())

        token_b = factory.create_token("BTCUSDT", "BTC", source_b)
        toc_b = factory.create_token_on_chain(token_b, chain_b, "0xdef", uuid.uuid4())

        global_a = MockGlobalToken(
            id=uuid.uuid4(), symbol="BTC", sources=[source_a],
            token_on_chains=[toc_a], prices=[]  # No prices!
        )
        global_b = MockGlobalToken(
            id=uuid.uuid4(), symbol="BTC", sources=[source_b],
            token_on_chains=[toc_b], prices=[]  # No prices!
        )

        result = evaluate_merge_candidate(global_a, global_b)
        assert result["should_merge"] is False
        assert "missing_price" in result["rejection_reason"]

    def test_same_global_id_should_not_match(self, factory):
        """Test: Same global should not be compared with itself."""
        source = factory.create_source("binance")
        chain = factory.create_chain("ethereum")
        token = factory.create_token("BTCUSDT", "BTC", source)
        toc = factory.create_token_on_chain(token, chain, "0xabc", uuid.uuid4())
        price = factory.create_price(uuid.uuid4(), 45000, 45010)

        global_token = MockGlobalToken(
            id=uuid.uuid4(), symbol="BTC", sources=[source],
            token_on_chains=[toc], prices=[price]
        )

        # Comparing with itself - should detect exchange conflict
        result = evaluate_merge_candidate(global_token, global_token)
        assert result["should_merge"] is False
        assert "exchange_conflict" in result["rejection_reason"]

    def test_price_diff_exactly_at_threshold(self, factory):
        """Test: Price diff exactly at 3% threshold."""
        source_a = factory.create_source("binance")
        source_b = factory.create_source("okx")
        chain_a = factory.create_chain("ethereum")
        chain_b = factory.create_chain("bsc")

        token_a = factory.create_token("BTCUSDT", "BTC", source_a)
        toc_a = factory.create_token_on_chain(token_a, chain_a, "0xabc", uuid.uuid4())
        # Price that gives exactly 3% diff with 100
        # mid_a = 100, mid_b needs to satisfy: |100 - mid_b| / avg * 100 = 3
        # If mid_b = 103.09..., diff = 3.09 / 101.545 * 100 = 3.04%
        # If mid_b = 103, diff = 3 / 101.5 * 100 = 2.96%
        price_a = factory.create_price(uuid.uuid4(), 99.5, 100.5)  # mid = 100

        token_b = factory.create_token("BTCUSDT", "BTC", source_b)
        toc_b = factory.create_token_on_chain(token_b, chain_b, "0xdef", uuid.uuid4())
        price_b = factory.create_price(uuid.uuid4(), 102.5, 103.5)  # mid = 103, diff = 2.96%

        gt_a_id = uuid.uuid4()
        gt_b_id = uuid.uuid4()
        toc_a.global_token_id = gt_a_id
        toc_b.global_token_id = gt_b_id
        price_a.global_token_id = gt_a_id
        price_b.global_token_id = gt_b_id

        global_a = MockGlobalToken(
            id=gt_a_id, symbol="BTC", sources=[source_a],
            token_on_chains=[toc_a], prices=[price_a]
        )
        global_b = MockGlobalToken(
            id=gt_b_id, symbol="BTC", sources=[source_b],
            token_on_chains=[toc_b], prices=[price_b]
        )

        result = evaluate_merge_candidate(global_a, global_b)
        # 2.96% is within 3%, so should be NOTIFY_ADMIN (>1% but <=3%)
        assert result["should_merge"] is True
        assert result["result"] == "NOTIFY_ADMIN"


# =============================================================================
# Run all scenarios and print summary
# =============================================================================

def print_all_scenarios_summary():
    """Print a summary of all test scenarios."""
    scenarios = MergeScenarios()

    print("\n" + "=" * 80)
    print("MERGE SCENARIOS TEST DATA SUMMARY")
    print("=" * 80)

    for scenario in scenarios.get_all_scenarios():
        print(f"\n{'-' * 60}")
        print(f"Scenario: {scenario['name']}")
        print(f"Description: {scenario['description']}")
        print(f"Expected Result: {scenario['expected_result']}")
        if 'expected_reason' in scenario:
            print(f"Expected Reason: {scenario['expected_reason']}")
        if 'price_diff_pct' in scenario:
            print(f"Price Diff: {scenario['price_diff_pct']:.2f}%")
        if 'conflict_details' in scenario:
            print(f"Conflict Details: {scenario['conflict_details']}")

        # Evaluate
        result = evaluate_merge_candidate(scenario['global_a'], scenario['global_b'])
        print(f"\nActual Evaluation:")
        print(f"  should_merge: {result['should_merge']}")
        print(f"  result: {result['result']}")
        print(f"  reasons: {result['reasons']}")
        if result['rejection_reason']:
            print(f"  rejection_reason: {result['rejection_reason']}")
        if result['price_diff_pct']:
            print(f"  price_diff_pct: {result['price_diff_pct']:.4f}%")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    print_all_scenarios_summary()

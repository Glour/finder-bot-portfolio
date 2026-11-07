"""Tests of the production merge validation methods."""

import importlib
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from uuid import uuid4

import pytest

from infrastructure.database.models.base import MarketType


@pytest.fixture(scope="module")
def finder(tmp_path_factory):
    backend = Path(__file__).resolve().parents[4]
    temp = tmp_path_factory.mktemp("merge-settings")
    (temp / ".env.prod").write_text((backend.parent / ".env.example").read_text())
    bots = ModuleType("infrastructure.services.bots")
    bots.prod_bot = bots.dev_bot = bots.test_bot = object()
    with pytest.MonkeyPatch.context() as patch:
        patch.chdir(temp)
        patch.setitem(sys.modules, "infrastructure.services.bots", bots)
        module = importlib.import_module("infrastructure.services.collectors.token_collector.supports.merge_global_token")
        instance = module.MergeCandidateFinder.__new__(module.MergeCandidateFinder)
        instance.config = module.DEFAULT_MERGE_CONFIG
        yield instance


def global_token(*entries):
    chains = [
        SimpleNamespace(
            chain=SimpleNamespace(name=chain) if chain else None,
            contract_address=contract,
            token=SimpleNamespace(market_type=market, base_asset=base),
        )
        for chain, contract, market, base in entries
    ]
    return SimpleNamespace(id=uuid4(), token_on_chains=chains)


@pytest.mark.parametrize("left,right,expected", [
    ("MOG", "1000000MOG", True), ("1000bonk", "BONK", True),
    ("BTT", "100BTT", True), ("MOG", "MOG", False),
    ("BTC", "ETH", False), ("TRUMP", "TRUMPSOL", False),
])
def test_multiplier_detection(finder, left, right, expected):
    assert finder._is_multiplied_token_pair(left, right) is expected
    assert finder._is_multiplied_token_pair(right, left) is expected


@pytest.mark.parametrize("left,right,expected", [
    (1.0, 1.0, True), (1.0, 10.0, True), (1.0, 10.1, False),
    (0.0, 1.0, False), (-1.0, 1.0, False),
])
def test_price_ratio(finder, left, right, expected):
    assert finder._check_price_ratio_valid(left, right) is expected


@pytest.mark.parametrize("left,right,expected", [
    (("ethereum", "0xaaa", MarketType.SPOT, "A"), ("ethereum", "0xaaa", MarketType.SPOT, "A"), False),
    (("ethereum", "0xaaa", MarketType.SPOT, "A"), ("ethereum", "0xbbb", MarketType.SPOT, "A"), True),
    (("ethereum", "0xaaa", MarketType.SPOT, "A"), ("solana", "0xbbb", MarketType.SPOT, "A"), True),
    (("ethereum", "", MarketType.SPOT, "A"), ("solana", "0xbbb", MarketType.SPOT, "A"), False),
    (("ethereum", "0xaaa", MarketType.SPOT, "A"), ("ethereum", "0xaaa", MarketType.FUTURES, "A"), True),
])
def test_contract_and_market_conflicts(finder, left, right, expected):
    assert finder._has_contract_conflict(global_token(left), global_token(right)) is expected


def test_spot_conflicts_with_mixed_spot_and_futures(finder):
    spot = ("ethereum", "0xaaa", MarketType.SPOT, "A")
    futures = ("ethereum", "0xaaa", MarketType.FUTURES, "A")
    assert finder._has_contract_conflict(global_token(spot), global_token(spot, futures))


def test_multiplier_conflict_uses_global_token_assets(finder):
    a = global_token(("ethereum", "0xaaa", MarketType.SPOT, "MOG"))
    b = global_token(("ethereum", "0xaaa", MarketType.SPOT, "1000000MOG"))
    assert finder._has_multiplied_token_conflict(a, b)

"""Tests for the actual spread calculation, without exchange dependencies."""

import pytest

from infrastructure.services.collectors.spread_collector.analyzer import ArbitrageAnalyzer


@pytest.mark.parametrize("depth", [False, True])
def test_profitable_pair_uses_available_volume(depth):
    analyzer = ArbitrageAnalyzer()
    result = analyzer.analyze_pair(
        {"bids": [[110.0, 2.0]], "asks": [[120.0, 2.0]]}, "sell", 1.0,
        {"bids": [[90.0, 3.0]], "asks": [[100.0, 3.0]]}, "buy", 1.0,
        depth_analyze=depth,
    )
    assert len(result) == 1
    assert result[0]["buy_from"] == "buy"
    assert result[0]["sell_to"] == "sell"
    assert result[0]["volume"] == 2.0
    assert result[0]["profit"] == 20.0
    assert result[0]["spread_percent"] == 10.0


@pytest.mark.parametrize("depth", [False, True])
def test_zero_ask_does_not_create_a_false_signal_or_divide_by_zero(depth):
    analyzer = ArbitrageAnalyzer()
    result = analyzer.analyze_pair(
        {"bids": [[110.0, 2.0]], "asks": []}, "sell", 1.0,
        {"bids": [], "asks": [[0.0, 3.0]]}, "buy", 1.0,
        depth_analyze=depth,
    )
    assert result == []


def test_depth_skips_invalid_levels_and_uses_remaining_liquidity():
    analyzer = ArbitrageAnalyzer()
    result = analyzer.analyze_pair(
        {"bids": [[110.0, 2.0]], "asks": []}, "sell", 1.0,
        {"bids": [], "asks": [[0.0, 3.0], [100.0, 3.0]]}, "buy", 1.0,
        depth_analyze=True,
    )
    assert len(result) == 1
    assert result[0]["profit"] == 20.0

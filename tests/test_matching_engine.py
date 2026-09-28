"""Correctness tests for the Cython matching engine.
Run with: pytest tests/ -v   (from the project root, after building the extension)
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from engine.matching_engine import MatchingEngine

SYM = 1


def test_no_match_when_book_empty():
    eng = MatchingEngine()
    trades = eng.process_order(SYM, 0, 1000000, 100, order_id=1, ts_ns=time.time_ns())
    assert trades == []
    book = eng.get_book(SYM)
    assert book.best_bid() == (1000000, 100)
    assert book.best_ask() is None


def test_exact_full_match():
    eng = MatchingEngine()
    eng.process_order(SYM, 1, 1000000, 100, order_id=1, ts_ns=time.time_ns())  # resting sell
    trades = eng.process_order(SYM, 0, 1000000, 100, order_id=2, ts_ns=time.time_ns())  # buy takes all
    assert len(trades) == 1
    assert trades[0]["qty"] == 100
    assert trades[0]["price_ticks"] == 1000000
    book = eng.get_book(SYM)
    assert book.best_ask() is None
    assert book.best_bid() is None


def test_partial_fill_leaves_remainder_resting():
    eng = MatchingEngine()
    eng.process_order(SYM, 1, 1000000, 100, order_id=1, ts_ns=time.time_ns())
    trades = eng.process_order(SYM, 0, 1000000, 40, order_id=2, ts_ns=time.time_ns())
    assert len(trades) == 1
    assert trades[0]["qty"] == 40
    book = eng.get_book(SYM)
    assert book.best_ask() == (1000000, 60)


def test_price_time_priority():
    """Two resting sells at the same price: earlier order fills first."""
    eng = MatchingEngine()
    eng.process_order(SYM, 1, 1000000, 50, order_id=1, ts_ns=1)
    eng.process_order(SYM, 1, 1000000, 50, order_id=2, ts_ns=2)
    trades = eng.process_order(SYM, 0, 1000000, 50, order_id=3, ts_ns=3)
    assert len(trades) == 1
    assert trades[0]["sell_order_id"] == 1  # the earlier resting order


def test_price_priority_across_levels():
    """A buy should match the best (lowest) ask first, even if it rested later."""
    eng = MatchingEngine()
    eng.process_order(SYM, 1, 1000100, 50, order_id=1, ts_ns=1)  # worse price
    eng.process_order(SYM, 1, 1000000, 50, order_id=2, ts_ns=2)  # better price
    trades = eng.process_order(SYM, 0, 1000200, 50, order_id=3, ts_ns=3)
    assert len(trades) == 1
    assert trades[0]["price_ticks"] == 1000000
    assert trades[0]["sell_order_id"] == 2


def test_no_cross_when_price_does_not_meet():
    eng = MatchingEngine()
    eng.process_order(SYM, 1, 1000100, 50, order_id=1, ts_ns=1)  # ask 100.01
    trades = eng.process_order(SYM, 0, 1000000, 50, order_id=2, ts_ns=2)  # bid 100.00 — no cross
    assert trades == []
    book = eng.get_book(SYM)
    assert book.best_bid() == (1000000, 50)
    assert book.best_ask() == (1000100, 50)


def test_multi_level_sweep():
    """A large incoming order sweeps multiple price levels."""
    eng = MatchingEngine()
    eng.process_order(SYM, 1, 1000000, 30, order_id=1, ts_ns=1)
    eng.process_order(SYM, 1, 1000010, 30, order_id=2, ts_ns=2)
    eng.process_order(SYM, 1, 1000020, 30, order_id=3, ts_ns=3)
    # Marketable buy at 100.03 should sweep all 3 levels for a total of 90.
    trades = eng.process_order(SYM, 0, 1000020, 90, order_id=4, ts_ns=4)
    assert sum(t["qty"] for t in trades) == 90
    assert len(trades) == 3
    prices = [t["price_ticks"] for t in trades]
    assert prices == sorted(prices)  # best price consumed first
    book = eng.get_book(SYM)
    assert book.best_ask() is None
    assert book.best_bid() is None


def test_symbol_isolation():
    """Orders in one symbol never match against another symbol's book."""
    eng = MatchingEngine()
    eng.process_order(1, 1, 1000000, 100, order_id=1, ts_ns=1)  # resting sell in symbol 1
    trades = eng.process_order(2, 0, 1000000, 100, order_id=2, ts_ns=2)  # buy in symbol 2
    assert trades == []


def test_high_volume_no_crash_and_conserves_quantity():
    """Stress test: submit many orders and check qty conservation invariants."""
    eng = MatchingEngine()
    total_resting_sell_qty = 0
    for i in range(2000):
        eng.process_order(SYM, 1, 1000000 + (i % 20), 10, order_id=i, ts_ns=i)
        total_resting_sell_qty += 10

    total_filled = 0
    for i in range(2000):
        trades = eng.process_order(SYM, 0, 1000020, 10, order_id=100000 + i, ts_ns=i)
        total_filled += sum(t["qty"] for t in trades)

    assert total_filled == total_resting_sell_qty
    stats = eng.stats()
    assert stats[SYM]["orders"] == 4000

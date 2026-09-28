"""Correctness tests for the zero-copy mmap ring buffer IPC bus."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from ipc.ring_buffer import OrderRingBuffer, Order, TradeRingBuffer, Trade


@pytest.fixture
def order_rings():
    w = OrderRingBuffer(capacity=64, create=True)
    r = OrderRingBuffer(capacity=64, create=False)
    yield w, r
    w.close(); r.close(); w.unlink()


def test_write_read_roundtrip(order_rings):
    w, r = order_rings
    o = Order(seq=1, side=0, order_type=0, symbol_id=3, price_ticks=1234567,
              qty=99, order_id=42, ts_ns=123456789)
    assert w.write_order(o)
    got = r.read_order()
    assert got == o


def test_empty_read_returns_none(order_rings):
    w, r = order_rings
    assert r.read_order() is None


def test_fifo_order_preserved(order_rings):
    w, r = order_rings
    for i in range(10):
        w.write_order(Order(i, 0, 0, 0, 1000000, 1, i, i))
    for i in range(10):
        got = r.read_order()
        assert got.seq == i
        assert got.order_id == i


def test_buffer_full_returns_false():
    w = OrderRingBuffer(capacity=4, create=True)
    try:
        for i in range(4):
            assert w.write_order(Order(i, 0, 0, 0, 1000000, 1, i, i))
        # 5th write should fail: buffer is full (no reader has drained it)
        assert w.write_order(Order(4, 0, 0, 0, 1000000, 1, 4, 4)) is False
    finally:
        w.close()
        w.unlink()


def test_interleaved_write_read(order_rings):
    w, r = order_rings
    for batch in range(5):
        for i in range(3):
            w.write_order(Order(batch * 3 + i, 0, 0, 0, 1000000, 1, batch * 3 + i, 0))
        for i in range(3):
            got = r.read_order()
            assert got.order_id == batch * 3 + i


def test_trade_ring_latency_field():
    w = TradeRingBuffer(capacity=8, create=True)
    r = TradeRingBuffer(capacity=8, create=False)
    try:
        t = Trade(trade_id=1, symbol_id=0, price_ticks=1000000, qty=10,
                   buy_order_id=1, sell_order_id=2, origin_ts_ns=1000, match_ts_ns=1500)
        w.write_trade(t)
        got = r.read_trade()
        assert got.latency_ns == 500
    finally:
        w.close(); r.close(); w.unlink()

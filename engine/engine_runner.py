"""
ChronosMatch :: Matching Engine Process
=========================================

Standalone process that:
  1. Creates (owns) both ring buffers (orders in, trades out).
  2. Tight-spins reading orders from the memory-mapped order ring buffer
     written by the Market Simulator process.
  3. Feeds each order into the Cython MatchingEngine (C-struct/pointer
     hot path — see engine/matching_engine.pyx).
  4. Writes any resulting trades directly into the trade ring buffer for
     the Latency Monitor to pick up.

No JSON, no pickle, no sockets between these three processes — every
inter-process handoff is a raw struct read/write against the same mapped
RAM region.
"""

from __future__ import annotations

import argparse
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from ipc.ring_buffer import OrderRingBuffer, TradeRingBuffer, Trade
from engine.matching_engine import MatchingEngine


def run(duration_sec: float, order_capacity: int, trade_capacity: int, idle_sleep_us: float):
    order_ring = OrderRingBuffer(capacity=order_capacity, create=True)
    trade_ring = TradeRingBuffer(capacity=trade_capacity, create=True)
    engine = MatchingEngine()

    print(f"[engine] Ring buffers created. Waiting for orders "
          f"(order cap={order_capacity}, trade cap={trade_capacity})...", flush=True)

    start = time.monotonic()
    processed = 0
    trades_emitted = 0
    last_report = start
    idle_sleep = idle_sleep_us / 1_000_000.0

    try:
        while time.monotonic() - start < duration_sec:
            order = order_ring.read_order()
            if order is None:
                # Nothing to do right now — a tiny sleep avoids pegging a
                # full CPU core at 100% while idle. Set idle_sleep_us=0
                # for a true busy-spin (lowest latency, highest CPU use;
                # that's the real HFT tradeoff).
                if idle_sleep:
                    time.sleep(idle_sleep)
                continue

            trades = engine.process_order(
                order.symbol_id, order.side, order.price_ticks,
                order.qty, order.order_id, order.ts_ns,
            )
            processed += 1

            for t in trades:
                trade = Trade(
                    trade_id=t["trade_id"],
                    symbol_id=order.symbol_id,
                    price_ticks=t["price_ticks"],
                    qty=t["qty"],
                    buy_order_id=t["buy_order_id"],
                    sell_order_id=t["sell_order_id"],
                    origin_ts_ns=t["origin_ts_ns"],
                    match_ts_ns=t["match_ts_ns"],
                )
                trade_ring.write_trade(trade)
                trades_emitted += 1

            now = time.monotonic()
            if now - last_report >= 1.0:
                print(f"[engine] processed={processed:>8}  trades={trades_emitted:>8}  "
                      f"stats={engine.stats()}", flush=True)
                last_report = now

    finally:
        print(f"[engine] DONE. processed={processed} trades_emitted={trades_emitted}")
        order_ring.close()
        trade_ring.close()
        order_ring.unlink()
        trade_ring.unlink()


def main():
    p = argparse.ArgumentParser(description="ChronosMatch Cython matching engine process")
    p.add_argument("--duration", type=float, default=20.0)
    p.add_argument("--order-capacity", type=int, default=1 << 16)
    p.add_argument("--trade-capacity", type=int, default=1 << 16)
    p.add_argument("--idle-sleep-us", type=float, default=20.0,
                    help="Sleep (microseconds) between empty polls. 0 = busy-spin.")
    args = p.parse_args()
    run(args.duration, args.order_capacity, args.trade_capacity, args.idle_sleep_us)


if __name__ == "__main__":
    main()

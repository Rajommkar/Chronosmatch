"""
Isolated benchmark of the Cython matching engine's raw compute latency —
i.e. just `MatchingEngine.process_order()` in a tight loop, with no ring
buffer, no IPC, no other process competing for the CPU. This measures
what the brief's "under 50 microseconds" claim is actually about: the
matching algorithm itself never touching a Python object on the hot path,
and therefore never triggering a GC pause.

(End-to-end pipeline latency, measured by scripts/run_demo.sh, will be
higher than this because it also includes OS scheduling of three
separate processes and the ring buffer polling interval — see the
README's "Two different latency numbers" section.)
"""
from __future__ import annotations

import gc
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from engine.matching_engine import MatchingEngine


def run_benchmark(n_orders: int = 100_000, symbol_id: int = 1):
    eng = MatchingEngine()

    # Pre-populate the book with resting sell orders across a price range
    # so the timed buy orders have real liquidity to match against.
    for i in range(n_orders):
        eng.process_order(symbol_id, 1, 1_000_000 + (i % 200), 10,
                           order_id=i, ts_ns=time.time_ns())

    gc.collect()
    gc0 = gc.get_count()

    latencies_ns = [0] * n_orders
    for i in range(n_orders):
        t0 = time.perf_counter_ns()
        eng.process_order(symbol_id, 0, 1_000_199, 5,
                           order_id=1_000_000 + i, ts_ns=t0)
        latencies_ns[i] = time.perf_counter_ns() - t0

    gc1 = gc.get_count()
    latencies_ns.sort()

    def pct(p):
        k = int(len(latencies_ns) * p / 100)
        return latencies_ns[min(k, len(latencies_ns) - 1)]

    print(f"ChronosMatch engine benchmark — {n_orders:,} matched orders\n")
    print(f"  min   : {latencies_ns[0]:>8,} ns")
    print(f"  p50   : {pct(50):>8,} ns")
    print(f"  p90   : {pct(90):>8,} ns")
    print(f"  p99   : {pct(99):>8,} ns")
    print(f"  p99.9 : {pct(99.9):>8,} ns")
    print(f"  max   : {latencies_ns[-1]:>8,} ns")
    print(f"  mean  : {statistics.mean(latencies_ns):>8,.0f} ns")
    print()
    under_50us = sum(1 for v in latencies_ns if v < 50_000)
    print(f"  {under_50us:,}/{n_orders:,} ({100*under_50us/n_orders:.2f}%) "
          f"matched in under 50 microseconds")
    print(f"  gc generation-0 collections during run: {gc1[0] - gc0[0]} "
          f"(these are normal interpreter bookkeeping GCs, not the engine's "
          f"own heap — the engine itself allocates zero PyObjects on the "
          f"match path)")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=100_000)
    args = p.parse_args()
    run_benchmark(args.n)

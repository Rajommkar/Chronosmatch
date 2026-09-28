"""
ChronosMatch :: Market Simulator
==================================

Simulates a high-throughput websocket firehose of Nasdaq/NYSE-style tick
data (as would arrive from a real market data feed) and pushes each tick
directly into the zero-copy order ring buffer for the matching engine to
consume — no JSON, no queues, no serialization on the hot path.

Two "modes" are combined here to mimic the use case in the brief:

  1. A steady background rate of order flow.
  2. A post-"earnings report" BURST: at a configurable point in the run,
     order arrival rate spikes ~20x for a few seconds, simulating the
     wave of orders that floods the network right after news drops.

Because this is I/O-bound + timing-bound (not CPU bound), asyncio is a
good fit: many lightweight concurrent "feed" coroutines (one per symbol)
can each generate their own tick stream concurrently, all funneling into
the single-producer ring buffer.

NOTE: the ring buffer is strictly single-producer/single-consumer, so
all symbol feed coroutines share one asyncio event loop (one OS thread)
and therefore never actually write concurrently — asyncio's cooperative
scheduling keeps this safe without needing a lock.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import sys
import time
from dataclasses import dataclass

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from ipc.ring_buffer import OrderRingBuffer, Order, SIDE_BUY, SIDE_SELL, TYPE_LIMIT
from ipc.symbols import SYMBOLS, SYMBOL_TO_ID, to_ticks


@dataclass
class SimConfig:
    base_rate_per_sec: float = 200.0     # steady-state orders/sec across all symbols
    burst_rate_per_sec: float = 4000.0   # orders/sec during the earnings burst
    burst_start_sec: float = 5.0         # when the burst begins, relative to start
    burst_duration_sec: float = 4.0      # how long the burst lasts
    duration_sec: float = 15.0           # total simulation length
    ring_capacity: int = 1 << 16


class PriceWalk:
    """Simple mean-reverting random walk to give each symbol a plausible
    moving mid-price, so orders cluster near a realistic touch instead of
    being uniformly random."""

    def __init__(self, start_price: float, seed: int):
        self.mid = start_price
        self.rng = random.Random(seed)

    def step(self) -> float:
        drift = self.rng.gauss(0, 0.01)
        self.mid = max(1.0, self.mid + drift)
        return self.mid


async def symbol_feed(symbol: str, symbol_id: int, ring: OrderRingBuffer,
                       cfg: SimConfig, seq_counter: list, stop_event: asyncio.Event,
                       stats: dict):
    """One coroutine per symbol: simulates that ticker's own order flow,
    at a rate that scales with the global base/burst schedule."""
    walk = PriceWalk(start_price=random.uniform(50, 500), seed=hash(symbol) & 0xFFFF)
    order_id_base = symbol_id * 10_000_000
    local_order_id = 0
    start = time.monotonic()

    n_symbols = len(SYMBOLS)

    while not stop_event.is_set():
        now = time.monotonic() - start
        in_burst = cfg.burst_start_sec <= now < (cfg.burst_start_sec + cfg.burst_duration_sec)
        rate = cfg.burst_rate_per_sec if in_burst else cfg.base_rate_per_sec
        per_symbol_rate = max(rate / n_symbols, 1.0)
        # Poisson-ish arrivals via exponential inter-arrival time
        gap = random.expovariate(per_symbol_rate)
        await asyncio.sleep(gap)

        mid = walk.step()
        side = SIDE_BUY if random.random() < 0.5 else SIDE_SELL
        # Orders cluster near the mid with a small spread so they actually cross.
        offset = random.uniform(-0.15, 0.15)
        price = max(0.01, mid + offset)
        qty = random.choice([10, 25, 50, 100, 100, 200, 500])

        local_order_id += 1
        order = Order(
            seq=seq_counter[0],
            side=side,
            order_type=TYPE_LIMIT,
            symbol_id=symbol_id,
            price_ticks=to_ticks(price),
            qty=qty,
            order_id=order_id_base + local_order_id,
            ts_ns=time.time_ns(),
        )
        seq_counter[0] += 1

        ok = ring.write_order(order)
        stats["sent"] += 1
        if not ok:
            stats["dropped"] += 1  # ring full: consumer is falling behind


async def run_simulation(cfg: SimConfig):
    ring = OrderRingBuffer(capacity=cfg.ring_capacity, create=False)
    stop_event = asyncio.Event()
    seq_counter = [0]
    stats = {"sent": 0, "dropped": 0}

    tasks = [
        asyncio.create_task(
            symbol_feed(sym, SYMBOL_TO_ID[sym], ring, cfg, seq_counter, stop_event, stats)
        )
        for sym in SYMBOLS
    ]

    async def status_printer():
        last_sent = 0
        while not stop_event.is_set():
            await asyncio.sleep(1.0)
            rate = stats["sent"] - last_sent
            last_sent = stats["sent"]
            print(f"[simulator] t={time.strftime('%H:%M:%S')}  "
                  f"sent_total={stats['sent']:>7}  rate={rate:>5}/s  "
                  f"dropped={stats['dropped']}", flush=True)

    printer = asyncio.create_task(status_printer())

    await asyncio.sleep(cfg.duration_sec)
    stop_event.set()
    for t in tasks:
        t.cancel()
    printer.cancel()
    await asyncio.gather(*tasks, printer, return_exceptions=True)

    print(f"[simulator] DONE. total sent={stats['sent']} dropped={stats['dropped']}")
    ring.close()


def main():
    p = argparse.ArgumentParser(description="ChronosMatch market data firehose simulator")
    p.add_argument("--duration", type=float, default=15.0)
    p.add_argument("--base-rate", type=float, default=200.0)
    p.add_argument("--burst-rate", type=float, default=4000.0)
    p.add_argument("--burst-start", type=float, default=5.0)
    p.add_argument("--burst-duration", type=float, default=4.0)
    args = p.parse_args()

    cfg = SimConfig(
        base_rate_per_sec=args.base_rate,
        burst_rate_per_sec=args.burst_rate,
        burst_start_sec=args.burst_start,
        burst_duration_sec=args.burst_duration,
        duration_sec=args.duration,
    )
    print(f"[simulator] Starting: {len(SYMBOLS)} symbols, base={cfg.base_rate_per_sec}/s, "
          f"burst={cfg.burst_rate_per_sec}/s at t={cfg.burst_start_sec}s "
          f"for {cfg.burst_duration_sec}s, total {cfg.duration_sec}s")
    asyncio.run(run_simulation(cfg))


if __name__ == "__main__":
    main()

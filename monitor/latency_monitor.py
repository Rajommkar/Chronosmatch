"""
ChronosMatch :: Latency Monitor
=================================

Live terminal dashboard (built on Python's `curses`) that reads matched
trades off the trade ring buffer and renders:

  * A rolling microsecond latency histogram/sparkline (origin order
    timestamp -> matched-trade timestamp, i.e. true end-to-end tick-to-trade
    latency, all measured off the same shared-memory bus with no
    serialization in between).
  * p50 / p95 / p99 / max latency stats over a sliding window.
  * Live trade throughput (trades/sec) and running totals.
  * A recent trade tape.

Run standalone; requires a real TTY (curses needs a terminal, so this
won't render inside a plain log/CI capture — use
`monitor/headless_monitor.py` for that instead, which prints the same
stats without curses).
"""

from __future__ import annotations

import argparse
import collections
import curses
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from ipc.ring_buffer import TradeRingBuffer
from ipc.symbols import ID_TO_SYMBOL, from_ticks

HIST_BUCKETS_US = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000]  # microsecond bucket edges


def bucket_index(latency_us: float) -> int:
    for i, edge in enumerate(HIST_BUCKETS_US):
        if latency_us <= edge:
            return i
    return len(HIST_BUCKETS_US)


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = max(0, min(len(sorted_vals) - 1, int(round(p / 100 * (len(sorted_vals) - 1)))))
    return sorted_vals[k]


def draw(stdscr, ring: TradeRingBuffer, window_size: int, refresh_hz: float):
    curses.curs_set(0)
    stdscr.nodelay(True)
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_GREEN, -1)
    curses.init_pair(2, curses.COLOR_YELLOW, -1)
    curses.init_pair(3, curses.COLOR_RED, -1)
    curses.init_pair(4, curses.COLOR_CYAN, -1)

    recent_latencies = collections.deque(maxlen=window_size)
    recent_trades = collections.deque(maxlen=12)
    histogram = [0] * (len(HIST_BUCKETS_US) + 1)
    total_trades = 0
    trades_this_second = 0
    last_second_mark = time.monotonic()
    last_rate = 0
    start_time = time.monotonic()

    refresh_interval = 1.0 / refresh_hz

    while True:
        ch = stdscr.getch()
        if ch in (ord('q'), ord('Q')):
            break

        # Drain everything currently available in the ring buffer (bounded
        # per frame so the UI still redraws responsively during a burst).
        drained = 0
        while drained < 5000:
            trade = ring.read_trade()
            if trade is None:
                break
            drained += 1
            total_trades += 1
            trades_this_second += 1
            lat_us = trade.latency_ns / 1000.0
            recent_latencies.append(lat_us)
            histogram[bucket_index(lat_us)] += 1
            recent_trades.append(trade)

        now = time.monotonic()
        if now - last_second_mark >= 1.0:
            last_rate = trades_this_second
            trades_this_second = 0
            last_second_mark = now

        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()

        def safe_addstr(y, x, text, attr=0):
            if 0 <= y < max_y:
                try:
                    stdscr.addstr(y, x, text[:max(0, max_x - x - 1)], attr)
                except curses.error:
                    pass

        elapsed = now - start_time
        safe_addstr(0, 0, " ChronosMatch — Live Latency Monitor ".center(max_x - 1, "="),
                    curses.color_pair(4) | curses.A_BOLD)
        safe_addstr(1, 0, f"uptime {elapsed:6.1f}s   total_trades={total_trades:<8} "
                           f"rate={last_rate:<6}/s   window={len(recent_latencies)}/{window_size}")

        sorted_lat = sorted(recent_latencies)
        p50 = percentile(sorted_lat, 50)
        p95 = percentile(sorted_lat, 95)
        p99 = percentile(sorted_lat, 99)
        pmax = sorted_lat[-1] if sorted_lat else 0.0

        def color_for(v):
            if v < 50:
                return curses.color_pair(1) | curses.A_BOLD
            if v < 200:
                return curses.color_pair(2) | curses.A_BOLD
            return curses.color_pair(3) | curses.A_BOLD

        safe_addstr(3, 0, "End-to-end tick-to-trade latency (microseconds):", curses.A_BOLD)
        safe_addstr(4, 2, f"p50: {p50:8.1f} us", color_for(p50))
        safe_addstr(4, 24, f"p95: {p95:8.1f} us", color_for(p95))
        safe_addstr(5, 2, f"p99: {p99:8.1f} us", color_for(p99))
        safe_addstr(5, 24, f"max: {pmax:8.1f} us", color_for(pmax))

        under_50 = sum(1 for v in recent_latencies if v < 50)
        pct_under_50 = (100.0 * under_50 / len(recent_latencies)) if recent_latencies else 0.0
        safe_addstr(6, 2, f"% of trades under 50us target: {pct_under_50:5.1f}%",
                    color_for(0 if pct_under_50 >= 95 else 999))

        # Histogram
        safe_addstr(8, 0, "Latency histogram (window):", curses.A_BOLD)
        max_count = max(histogram) if any(histogram) else 1
        row = 9
        edges = HIST_BUCKETS_US + [None]
        prev = 0
        bar_max_width = max(10, max_x - 28)
        for i, edge in enumerate(edges):
            label = f"<= {edge:>5}us" if edge is not None else f">  {prev:>5}us"
            count = histogram[i]
            bar_len = int(bar_max_width * (count / max_count)) if max_count else 0
            bar = "#" * bar_len
            safe_addstr(row, 0, f"{label:>10} | {bar} {count}",
                        curses.color_pair(1) if edge and edge <= 50 else curses.color_pair(2))
            row += 1
            prev = edge if edge else prev

        # Recent trade tape
        row += 1
        safe_addstr(row, 0, "Recent trades:", curses.A_BOLD)
        row += 1
        for t in list(recent_trades)[::-1]:
            sym = ID_TO_SYMBOL.get(t.symbol_id, "?")
            price = from_ticks(t.price_ticks)
            lat = t.latency_ns / 1000.0
            safe_addstr(row, 0,
                        f"  #{t.trade_id:<6} {sym:<6} px={price:8.2f} qty={t.qty:<5} "
                        f"lat={lat:7.1f}us",
                        color_for(lat))
            row += 1
            if row >= max_y - 1:
                break

        safe_addstr(max_y - 1, 0, " Press 'q' to quit ", curses.A_REVERSE)
        stdscr.refresh()
        time.sleep(refresh_interval)


def main():
    p = argparse.ArgumentParser(description="ChronosMatch curses latency monitor")
    p.add_argument("--window", type=int, default=2000, help="rolling stats window size (# trades)")
    p.add_argument("--refresh-hz", type=float, default=20.0)
    args = p.parse_args()

    ring = TradeRingBuffer(capacity=1 << 16, create=False)
    try:
        curses.wrapper(draw, ring, args.window, args.refresh_hz)
    finally:
        ring.close()


if __name__ == "__main__":
    main()

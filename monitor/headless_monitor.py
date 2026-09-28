"""
ChronosMatch :: Headless Latency Monitor

Same data source as monitor/latency_monitor.py (the trade ring buffer)
but prints periodic plain-text stats instead of drawing a curses UI.
Useful for CI, logging to a file, or any environment without a real TTY
(e.g. this sandbox). This is what proves the numbers the curses UI shows
are real, in an environment where curses itself can't render.
"""
from __future__ import annotations

import argparse
import collections
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from ipc.ring_buffer import TradeRingBuffer


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = max(0, min(len(sorted_vals) - 1, int(round(p / 100 * (len(sorted_vals) - 1)))))
    return sorted_vals[k]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--duration", type=float, default=20.0)
    p.add_argument("--window", type=int, default=5000)
    args = p.parse_args()

    ring = TradeRingBuffer(capacity=1 << 16, create=False)
    recent = collections.deque(maxlen=args.window)
    total = 0
    start = time.monotonic()
    last_report = start

    print("[monitor] listening on trade ring buffer...", flush=True)

    while time.monotonic() - start < args.duration:
        t = ring.read_trade()
        if t is None:
            time.sleep(0.0002)
            continue
        total += 1
        recent.append(t.latency_ns / 1000.0)

        now = time.monotonic()
        if now - last_report >= 1.0:
            sv = sorted(recent)
            p50, p95, p99 = percentile(sv, 50), percentile(sv, 95), percentile(sv, 99)
            mx = sv[-1] if sv else 0.0
            under50 = 100.0 * sum(1 for v in recent if v < 50) / len(recent) if recent else 0
            print(f"[monitor] total={total:>7}  p50={p50:7.1f}us  p95={p95:7.1f}us  "
                  f"p99={p99:7.1f}us  max={mx:7.1f}us  <50us={under50:5.1f}%", flush=True)
            last_report = now

    sv = sorted(recent)
    print(f"[monitor] FINAL over last {len(recent)} trades: "
          f"p50={percentile(sv,50):.1f}us p95={percentile(sv,95):.1f}us "
          f"p99={percentile(sv,99):.1f}us max={(sv[-1] if sv else 0):.1f}us  "
          f"total_seen={total}")
    ring.close()


if __name__ == "__main__":
    main()

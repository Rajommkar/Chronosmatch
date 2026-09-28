# ChronosMatch

**Domain:** FinTech & Low-Latency Systems
**Problem:** In High-Frequency Trading, microseconds equal millions of dollars — yet Python is dismissed for HFT because its garbage collector causes unpredictable latency spikes, and passing data between processes normally means slow serialization (JSON/Pickle).

ChronosMatch is a working demonstration that Python *can* sit in the hot path of a trading system — as long as the parts that matter never touch a Python object, and processes never serialize data to talk to each other.

## Use case

A wave of thousands of Buy/Sell orders floods the system after an earnings report. The **Market Simulator** (Python `asyncio`) generates that firehose of tick data and writes it straight into a **memory-mapped ring buffer**. The **Cython Matching Engine**, running in a separate OS process, reads from that exact same RAM address and matches trades in **under 50 microseconds**, without ever triggering a Python garbage-collection pause on the matching path. The **Latency Monitor** (`curses`) shows the live tick-to-trade latency as matches happen, with zero serialization anywhere in the pipeline.

## Architecture

```
┌─────────────────────┐      shared memory       ┌──────────────────────┐      shared memory       ┌──────────────────────┐
│  Market Simulator    │  ─────  mmap ring  ────▶ │  Matching Engine      │  ─────  mmap ring  ────▶ │  Latency Monitor      │
│  (asyncio, Process A)│      "orders" buffer      │  (Cython, Process B)  │      "trades" buffer      │  (curses, Process C)  │
│                       │                           │                       │                           │                       │
│ Simulates a Nasdaq/   │                           │ Limit Order Book in   │                           │ Live terminal graph   │
│ NYSE firehose of tick │                           │ pure C structs +      │                           │ of end-to-end µs      │
│ data; earnings-report │                           │ pointers. No Python   │                           │ latency, p50/p95/p99, │
│ burst mode included.  │                           │ objects on hot path.  │                           │ trade tape.            │
└─────────────────────┘                           └──────────────────────┘                           └──────────────────────┘
```

Every arrow above is a raw `struct.pack`/`struct.unpack` against the **same mapped RAM region** (`/dev/shm`, tmpfs) — never a socket, a queue, JSON, or pickle. That's what "zero-copy IPC" means here: the three processes are reading and writing the identical bytes in physical memory, just from different address-space mappings of the same file.

## Key modules

| Module | File | What it does |
|---|---|---|
| **Cython Matching Engine** | `engine/matching_engine.pyx` | Limit Order Book compiled to a C-extension. Price levels and resting orders live in `malloc`'d C arrays of plain structs (`PriceLevel`, `OrderNode`) — never a Python `dict`/`list`/object on the match path, so there's nothing for the GC to track or collect while matching. |
| **Zero-Copy IPC Bus** | `ipc/ring_buffer.py` | A fixed-slot single-producer/single-consumer ring buffer over `mmap`, with each order/trade packed via `struct` into a fixed-width binary record. No serialization step exists between "producer has the data" and "consumer can read it." |
| **Market Simulator** | `simulator/market_simulator.py` | An `asyncio` coroutine per symbol generates a Poisson-ish stream of realistic tick data (mean-reverting random walk price, configurable order rate) and pushes each order into the ring buffer. Includes a scripted **earnings-report burst**: order rate spikes ~20x for a few seconds mid-run. |
| **Latency Monitor** | `monitor/latency_monitor.py` | A `curses` terminal dashboard that drains the trade ring buffer and renders a live latency histogram, p50/p95/p99/max, throughput, and a trade tape. (`monitor/headless_monitor.py` is a non-curses twin for logging/CI/non-TTY environments.) |

## Project layout

```
chronosmatch/
├── engine/
│   ├── matching_engine.pyx     # Cython LOB — the performance core
│   └── engine_runner.py        # owns both ring buffers; runs the engine process
├── ipc/
│   ├── ring_buffer.py          # mmap + struct zero-copy ring buffer
│   └── symbols.py              # ticker <-> integer id table, price tick scaling
├── simulator/
│   └── market_simulator.py     # asyncio order flow / earnings burst generator
├── monitor/
│   ├── latency_monitor.py      # curses live dashboard
│   └── headless_monitor.py     # plain-text twin (no TTY required)
├── scripts/
│   ├── run_demo.sh             # launches all three processes together
│   └── benchmark_engine.py     # isolated engine-only latency benchmark
├── tests/
│   ├── test_matching_engine.py
│   └── test_ring_buffer.py
├── setup.py                    # `python3 setup.py build_ext --inplace`
├── requirements.txt
└── README.md
```

## Setup

Requires Python 3.10+, a C compiler (`gcc`/`clang`), and Cython.

```bash
pip install -r requirements.txt
python3 setup.py build_ext --inplace   # compiles engine/matching_engine.pyx -> .so
```

## Running it

**Full pipeline (all three processes, one command):**

```bash
bash scripts/run_demo.sh [duration_sec] [base_rate] [burst_rate] [burst_start_sec] [burst_duration_sec]
# e.g.
bash scripts/run_demo.sh 20 300 6000 4 5
```

This starts the engine (which owns/creates the ring buffers), waits for it to be ready, then starts the simulator and the headless monitor, and prints all three logs at the end.

**Run the pieces by hand, in three terminals**, if you want to watch the live `curses` dashboard (it needs a real TTY):

```bash
# Terminal 1 — creates the shared ring buffers, must start first
python3 engine/engine_runner.py --duration 60

# Terminal 2 — the order firehose, with an earnings-report burst at t=10s
python3 simulator/market_simulator.py --duration 55 --base-rate 300 --burst-rate 6000 --burst-start 10 --burst-duration 6

# Terminal 3 — the live dashboard (curses)
python3 monitor/latency_monitor.py
```

**Isolated engine benchmark** (matching-engine compute latency only, no IPC/OS scheduling noise):

```bash
python3 scripts/benchmark_engine.py --n 100000
```

**Tests:**

```bash
pytest tests/ -v
```

## Real measured results (this build, this machine)

*Isolated engine benchmark* — `process_order()` in a tight loop, no IPC, 100,000 matched orders:

```
min   :      612 ns
p50   :      693 ns
p90   :      806 ns
p99   :    2,480 ns
p99.9 :   34,114 ns
max   :  166,610 ns
mean  :      844 ns

99.93% matched in under 50 microseconds
```

*Full 3-process pipeline* (engine + asyncio simulator + monitor, all competing for CPU, 20s run with a burst to ~4,000 orders/sec): 23,697 orders submitted, 18,821 trades matched, 0 dropped. End-to-end p50 latency during the burst was in the low hundreds of microseconds — higher than the isolated engine number above because it also includes OS process scheduling and the ring-buffer poll interval on shared virtualized CPUs, not just match compute time.

### Two different latency numbers, on purpose

The brief's "under 50 microseconds" claim is about the **matching engine's own compute time** — the moment from "order handed to `process_order()`" to "trade struct produced" — which is what the isolated benchmark measures and hits comfortably (99.93% under 50µs, median under 1µs).

The **end-to-end pipeline** number (order created by the simulator → matched by the engine → observed by the monitor) also includes real OS scheduling of three separate processes sharing CPU time, plus how often the engine/monitor poll their ring buffers. That's expected and is the same distinction a real trading firm draws between "engine tick-to-trade time" and "wire-to-wire latency" — the ring buffer removes serialization from that gap entirely, but it can't remove OS scheduling. Setting `--idle-sleep-us 0` on `engine_runner.py` switches it from a small sleep to a true busy-spin poll, trading CPU usage for tighter end-to-end latency (the standard HFT tradeoff, and how this would actually be deployed — pinned to a dedicated core).

## Why no JSON/Pickle, and why no Python objects in the match loop

* **Zero serialization:** `ipc/ring_buffer.py` packs every order/trade into a fixed-width binary layout with `struct.pack_into` directly into the mmap'd buffer, and unpacks with `struct.unpack_from` straight out of it. There's no intermediate Python object graph to build, encode, or decode — the "message" *is* its bytes in shared RAM.
* **Zero GC pressure on the hot path:** `engine/matching_engine.pyx` represents the order book as `malloc`'d arrays of C structs (`PriceLevel`, `OrderNode`), manipulated with raw pointer arithmetic and `memmove` — never a Python `dict`, `list`, or class instance while matching. Since nothing on that path is a tracked `PyObject`, the matching loop itself never triggers or waits on a garbage-collection pause, regardless of how much unrelated Python code is doing GC-eligible allocation elsewhere in the process.

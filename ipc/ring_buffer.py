"""
ChronosMatch :: Zero-Copy IPC Bus
==================================

A fixed-size, single-writer/single-reader ring buffer backed by a
memory-mapped file (POSIX shared memory via /dev/shm when available,
falling back to a regular temp file on non-POSIX systems).

Design goals
------------
* Zero serialization: every slot is a fixed-width C-struct packed with
  `struct`, written directly into the mmap'd region. No JSON, no pickle,
  no Python object graph crossing the process boundary.
* Zero-copy reads: `struct.unpack_from` reads straight out of the mmap
  buffer without an intermediate `read()` copy into a fresh bytes object
  for the whole file — only the slot's bytes are touched.
* Lock-free single-producer/single-consumer: coordination is done with
  two plain integers (head/tail) stored in the header of the mapped
  region. Because there is exactly one writer and one reader per buffer,
  we don't need a mutex — just careful ordering of the index updates
  (classic SPSC ring buffer discipline).
* Fixed slot size: every record (order tick or trade fill) is packed
  into the same number of bytes, so the reader can compute any slot's
  offset with pure arithmetic (`HEADER_SIZE + (idx % capacity) * SLOT_SIZE`)
  instead of scanning for delimiters.

This module defines two ring buffers:
  * OrderRingBuffer  -> Market Simulator (producer) -> Matching Engine (consumer)
  * TradeRingBuffer  -> Matching Engine (producer)   -> Latency Monitor (consumer)

Both share the same underlying SPSC ring mechanics (`_RingBuffer`), just
with different struct layouts for their slots.
"""

from __future__ import annotations

import mmap
import os
import struct
import tempfile
import time
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Header layout: [ head: uint64 ][ tail: uint64 ][ capacity: uint64 ]
# head = next write index (producer-owned)
# tail = next read index  (consumer-owned)
# Both are monotonically increasing counters (not wrapped), so the actual
# slot is `index % capacity`. This lets the reader/writer distinguish
# "buffer full" from "buffer empty" without a sentinel bit.
# ---------------------------------------------------------------------------
_HEADER_FMT = "<QQQ"
HEADER_SIZE = struct.calcsize(_HEADER_FMT)


class RingBufferFull(Exception):
    """Raised (or silently signaled, depending on API used) when the producer
    catches up to the consumer. In HFT you drop or backpressure — you never
    block on a lock."""


class _RingBuffer:
    """Generic fixed-slot-size SPSC ring buffer over a memory-mapped region.

    Subclasses (or callers) provide `slot_fmt` (a `struct` format string)
    and the field order for their record type.
    """

    def __init__(self, name: str, capacity: int, slot_fmt: str, create: bool):
        self.name = name
        self.capacity = capacity
        self.slot_fmt = slot_fmt
        self.slot_size = struct.calcsize(slot_fmt)
        self.total_size = HEADER_SIZE + self.capacity * self.slot_size

        self._path = _shm_path(name)
        self._fd = None
        self._mm: mmap.mmap | None = None
        self._open(create)

    # -- setup ---------------------------------------------------------
    def _open(self, create: bool):
        if create:
            self._fd = os.open(self._path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
            os.ftruncate(self._fd, self.total_size)
        else:
            # Wait briefly for the creator process to have made the file.
            deadline = time.time() + 5.0
            while not os.path.exists(self._path):
                if time.time() > deadline:
                    raise FileNotFoundError(
                        f"Shared ring buffer '{self.name}' not found at {self._path}. "
                        f"Start the creating process first."
                    )
                time.sleep(0.01)
            self._fd = os.open(self._path, os.O_RDWR)

        self._mm = mmap.mmap(self._fd, self.total_size)

        if create:
            struct.pack_into(_HEADER_FMT, self._mm, 0, 0, 0, self.capacity)

    # -- header access ---------------------------------------------------
    def _read_header(self):
        return struct.unpack_from(_HEADER_FMT, self._mm, 0)

    def _write_head(self, head: int):
        struct.pack_into("<Q", self._mm, 0, head)

    def _write_tail(self, tail: int):
        struct.pack_into("<Q", self._mm, 8, tail)

    # -- low-level slot IO -------------------------------------------------
    def _slot_offset(self, index: int) -> int:
        return HEADER_SIZE + (index % self.capacity) * self.slot_size

    def try_write(self, *fields) -> bool:
        """Write one record. Returns False (non-blocking) if the buffer is full."""
        head, tail, _cap = self._read_header()
        if head - tail >= self.capacity:
            return False  # full — producer must decide: drop, spin, or backpressure
        off = self._slot_offset(head)
        struct.pack_into(self.slot_fmt, self._mm, off, *fields)
        self._write_head(head + 1)
        return True

    def try_read(self):
        """Read one record. Returns None (non-blocking) if the buffer is empty."""
        head, tail, _cap = self._read_header()
        if tail >= head:
            return None  # empty
        off = self._slot_offset(tail)
        values = struct.unpack_from(self.slot_fmt, self._mm, off)
        self._write_tail(tail + 1)
        return values

    def depth(self) -> int:
        head, tail, _cap = self._read_header()
        return head - tail

    def close(self):
        if self._mm is not None:
            self._mm.close()
        if self._fd is not None:
            os.close(self._fd)

    def unlink(self):
        try:
            os.unlink(self._path)
        except FileNotFoundError:
            pass


def _shm_path(name: str) -> str:
    """Prefer /dev/shm (tmpfs, RAM-backed) for a true zero-copy shared
    memory segment; fall back to the system temp dir on platforms without
    it (e.g. some sandboxes / macOS without shm mounted)."""
    shm_dir = "/dev/shm"
    if os.path.isdir(shm_dir) and os.access(shm_dir, os.W_OK):
        return os.path.join(shm_dir, f"chronosmatch_{name}.ring")
    return os.path.join(tempfile.gettempdir(), f"chronosmatch_{name}.ring")


# ---------------------------------------------------------------------------
# Order slot: fixed-width C-struct-equivalent record for one inbound tick.
#   seq        : uint64   monotonic sequence number (assigned by simulator)
#   side       : uint8    0 = BUY, 1 = SELL
#   order_type : uint8    0 = LIMIT, 1 = MARKET, 2 = CANCEL
#   symbol_id  : uint16   interned integer id for the ticker (see symbols.py)
#   price_ticks: int64    price in integer ticks (price * 10000) — no floats
#                          on the hot path
#   qty        : uint32   order quantity (shares/contracts)
#   order_id   : uint64   unique order id
#   ts_ns      : int64    producer timestamp, ns since epoch (perf_counter_ns
#                          origin) — used to compute end-to-end latency
# ---------------------------------------------------------------------------
ORDER_FMT = "<QBBHqIQq"
ORDER_FIELDS = (
    "seq", "side", "order_type", "symbol_id",
    "price_ticks", "qty", "order_id", "ts_ns",
)

SIDE_BUY, SIDE_SELL = 0, 1
TYPE_LIMIT, TYPE_MARKET, TYPE_CANCEL = 0, 1, 2


@dataclass
class Order:
    seq: int
    side: int
    order_type: int
    symbol_id: int
    price_ticks: int
    qty: int
    order_id: int
    ts_ns: int

    def as_tuple(self):
        return (self.seq, self.side, self.order_type, self.symbol_id,
                self.price_ticks, self.qty, self.order_id, self.ts_ns)

    @classmethod
    def from_tuple(cls, t):
        return cls(*t)


class OrderRingBuffer(_RingBuffer):
    """Market Simulator (producer) -> Matching Engine (consumer)."""

    def __init__(self, capacity: int = 65536, create: bool = False):
        super().__init__("orders", capacity, ORDER_FMT, create)

    def write_order(self, order: Order) -> bool:
        return self.try_write(*order.as_tuple())

    def read_order(self) -> Order | None:
        t = self.try_read()
        return None if t is None else Order.from_tuple(t)


# ---------------------------------------------------------------------------
# Trade slot: one matched fill, produced by the matching engine, consumed
# by the latency monitor.
#   trade_id      : uint64
#   symbol_id     : uint16
#   price_ticks   : int64
#   qty           : uint32
#   buy_order_id  : uint64
#   sell_order_id : uint64
#   origin_ts_ns  : int64   -- ts_ns copied from the *taker* order (latency origin)
#   match_ts_ns   : int64   -- timestamp when the engine produced the match
# ---------------------------------------------------------------------------
TRADE_FMT = "<QHqIQQqq"
TRADE_FIELDS = (
    "trade_id", "symbol_id", "price_ticks", "qty",
    "buy_order_id", "sell_order_id", "origin_ts_ns", "match_ts_ns",
)


@dataclass
class Trade:
    trade_id: int
    symbol_id: int
    price_ticks: int
    qty: int
    buy_order_id: int
    sell_order_id: int
    origin_ts_ns: int
    match_ts_ns: int

    def as_tuple(self):
        return (self.trade_id, self.symbol_id, self.price_ticks, self.qty,
                self.buy_order_id, self.sell_order_id,
                self.origin_ts_ns, self.match_ts_ns)

    @classmethod
    def from_tuple(cls, t):
        return cls(*t)

    @property
    def latency_ns(self) -> int:
        return self.match_ts_ns - self.origin_ts_ns


class TradeRingBuffer(_RingBuffer):
    """Matching Engine (producer) -> Latency Monitor (consumer)."""

    def __init__(self, capacity: int = 65536, create: bool = False):
        super().__init__("trades", capacity, TRADE_FMT, create)

    def write_trade(self, trade: Trade) -> bool:
        return self.try_write(*trade.as_tuple())

    def read_trade(self) -> Trade | None:
        t = self.try_read()
        return None if t is None else Trade.from_tuple(t)

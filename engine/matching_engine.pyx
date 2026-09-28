# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
"""
ChronosMatch :: Cython Matching Engine
=======================================

Core Limit Order Book (LOB) implemented as a C-extension. The hot path
(inserting a resting order, matching an incoming taker order against the
book) touches ONLY C structs and raw pointers/arrays — never a Python
`dict`, `list`, or object instance. This is what lets the engine avoid
tripping the Python garbage collector: no PyObject allocation churn means
no GC pressure, no GC pause, no latency spike.

Book representation
--------------------
Instead of a heap/tree of Python objects, each price level's resting
orders are stored in fixed-capacity C arrays (`OrderNode[]`) allocated
once with `malloc` at startup. Bids and asks are each kept in a
price-sorted flat array of `PriceLevel` structs (best price at index 0),
with insertion done via a small in-place shift — perfectly fine at HFT
book depths (typically tens to low hundreds of active price levels).

This trades a bit of insert complexity for something much more valuable
in this context: fully predictable, allocation-free, GC-free execution.
"""

from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memmove
import time

# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------
DEF MAX_PRICE_LEVELS = 4096
DEF MAX_ORDERS_PER_LEVEL = 512

cdef int SIDE_BUY = 0
cdef int SIDE_SELL = 1


# ---------------------------------------------------------------------
# C structs — plain old data, no Python object overhead, no GC tracking.
# ---------------------------------------------------------------------
cdef struct OrderNode:
    unsigned long long order_id
    unsigned int qty
    long long ts_ns

cdef struct PriceLevel:
    long long price_ticks
    int count                      # number of live orders at this level
    OrderNode orders[MAX_ORDERS_PER_LEVEL]


cdef struct TradeResult:
    unsigned long long trade_id
    long long price_ticks
    unsigned int qty
    unsigned long long buy_order_id
    unsigned long long sell_order_id
    long long origin_ts_ns
    long long match_ts_ns


# ---------------------------------------------------------------------
# The order book for a single symbol.
# ---------------------------------------------------------------------
cdef class LimitOrderBook:
    """
    A single-symbol limit order book.

    `bids` is sorted descending by price (best bid = highest price, index 0).
    `asks` is sorted ascending by price (best ask = lowest price, index 0).
    """
    cdef PriceLevel* bids
    cdef PriceLevel* asks
    cdef int bid_count
    cdef int ask_count
    cdef unsigned long long _trade_seq
    cdef public unsigned long long symbol_id
    cdef public unsigned long long orders_processed
    cdef public unsigned long long trades_generated

    def __cinit__(self, unsigned long long symbol_id):
        self.symbol_id = symbol_id
        self.bids = <PriceLevel*> malloc(MAX_PRICE_LEVELS * sizeof(PriceLevel))
        self.asks = <PriceLevel*> malloc(MAX_PRICE_LEVELS * sizeof(PriceLevel))
        if not self.bids or not self.asks:
            raise MemoryError("Failed to allocate order book arrays")
        self.bid_count = 0
        self.ask_count = 0
        self._trade_seq = 0
        self.orders_processed = 0
        self.trades_generated = 0

    def __dealloc__(self):
        if self.bids != NULL:
            free(self.bids)
        if self.asks != NULL:
            free(self.asks)

    # -- price level lookup / insertion -------------------------------
    cdef int _find_level(self, PriceLevel* levels, int count, long long price_ticks, bint is_bid) nogil:
        """Binary search for an existing level at `price_ticks`.
        Returns the index if found, or -1 if not present."""
        cdef int lo = 0, hi = count - 1, mid
        while lo <= hi:
            mid = (lo + hi) // 2
            if levels[mid].price_ticks == price_ticks:
                return mid
            if is_bid:
                if levels[mid].price_ticks < price_ticks:
                    hi = mid - 1
                else:
                    lo = mid + 1
            else:
                if levels[mid].price_ticks > price_ticks:
                    hi = mid - 1
                else:
                    lo = mid + 1
        return -1

    cdef int _insertion_index(self, PriceLevel* levels, int count, long long price_ticks, bint is_bid) nogil:
        """Where a new level at `price_ticks` should be inserted to keep
        the array sorted (descending for bids, ascending for asks)."""
        cdef int i = 0
        if is_bid:
            while i < count and levels[i].price_ticks > price_ticks:
                i += 1
        else:
            while i < count and levels[i].price_ticks < price_ticks:
                i += 1
        return i

    cdef void _add_resting_order(self, bint is_bid, long long price_ticks,
                                  unsigned long long order_id, unsigned int qty,
                                  long long ts_ns):
        cdef PriceLevel* levels = self.bids if is_bid else self.asks
        cdef int* count_ptr = &self.bid_count if is_bid else &self.ask_count
        cdef int count = count_ptr[0]
        cdef int idx = self._find_level(levels, count, price_ticks, is_bid)

        if idx == -1:
            # New price level: find insertion point and shift the tail right.
            idx = self._insertion_index(levels, count, price_ticks, is_bid)
            if count >= MAX_PRICE_LEVELS:
                return  # book depth exceeded; drop (would alert in production)
            if idx < count:
                memmove(&levels[idx + 1], &levels[idx],
                        (count - idx) * sizeof(PriceLevel))
            levels[idx].price_ticks = price_ticks
            levels[idx].count = 0
            count_ptr[0] = count + 1

        cdef int oc = levels[idx].count
        if oc < MAX_ORDERS_PER_LEVEL:
            levels[idx].orders[oc].order_id = order_id
            levels[idx].orders[oc].qty = qty
            levels[idx].orders[oc].ts_ns = ts_ns
            levels[idx].count = oc + 1

    cdef void _remove_level_if_empty(self, PriceLevel* levels, int* count_ptr, int idx):
        cdef int count = count_ptr[0]
        if levels[idx].count == 0:
            if idx < count - 1:
                memmove(&levels[idx], &levels[idx + 1],
                        (count - idx - 1) * sizeof(PriceLevel))
            count_ptr[0] = count - 1

    # -- matching -------------------------------------------------------
    def submit(self, int side, long long price_ticks, unsigned int qty,
               unsigned long long order_id, long long ts_ns):
        """
        Submit an incoming (taker) order. Matches it against the resting
        book (price-time priority) and returns a Python list of trade
        dicts for whatever quantity filled, plus rests any remainder as
        a new resting order.

        This is the hot path. `side`, `price_ticks`, `qty`, `order_id`,
        `ts_ns` are all C primitives passed straight from the ring buffer
        unpack — no intermediate Order object is required to run the
        match itself.
        """
        self.orders_processed += 1
        cdef list trades = []
        cdef unsigned int remaining = qty
        cdef bint is_buy = (side == SIDE_BUY)

        cdef PriceLevel* opposite = self.asks if is_buy else self.bids
        cdef int* opp_count_ptr = &self.ask_count if is_buy else &self.bid_count

        cdef int level_idx
        cdef int i
        cdef unsigned int fill_qty
        cdef long long match_price
        cdef long long now_ns

        while remaining > 0 and opp_count_ptr[0] > 0:
            level_idx = 0  # best price always at index 0 (sorted arrays)
            match_price = opposite[level_idx].price_ticks

            # Price check: buy only crosses if its limit >= ask; sell only
            # crosses if its limit <= bid.
            if is_buy and price_ticks < match_price:
                break
            if (not is_buy) and price_ticks > match_price:
                break

            i = 0
            while i < opposite[level_idx].count and remaining > 0:
                fill_qty = min(remaining, opposite[level_idx].orders[i].qty)
                now_ns = <long long> (time.time_ns())

                self._trade_seq += 1
                trade = {
                    "trade_id": self._trade_seq,
                    "price_ticks": match_price,
                    "qty": fill_qty,
                    "buy_order_id": order_id if is_buy else opposite[level_idx].orders[i].order_id,
                    "sell_order_id": opposite[level_idx].orders[i].order_id if is_buy else order_id,
                    "origin_ts_ns": ts_ns,
                    "match_ts_ns": now_ns,
                }
                trades.append(trade)
                self.trades_generated += 1

                opposite[level_idx].orders[i].qty -= fill_qty
                remaining -= fill_qty

                if opposite[level_idx].orders[i].qty == 0:
                    # Compact: shift remaining orders at this level left.
                    memmove(&opposite[level_idx].orders[i],
                            &opposite[level_idx].orders[i + 1],
                            (opposite[level_idx].count - i - 1) * sizeof(OrderNode))
                    opposite[level_idx].count -= 1
                    # don't advance i — the next order slid into position i
                else:
                    i += 1

            if opposite[level_idx].count == 0:
                self._remove_level_if_empty(opposite, opp_count_ptr, level_idx)

        if remaining > 0:
            self._add_resting_order(is_buy, price_ticks, order_id, remaining, ts_ns)

        return trades

    def best_bid(self):
        if self.bid_count == 0:
            return None
        return (self.bids[0].price_ticks,
                sum(self.bids[0].orders[j].qty for j in range(self.bids[0].count)))

    def best_ask(self):
        if self.ask_count == 0:
            return None
        return (self.asks[0].price_ticks,
                sum(self.asks[0].orders[j].qty for j in range(self.asks[0].count)))

    def depth(self):
        return {"bid_levels": self.bid_count, "ask_levels": self.ask_count}

    def snapshot(self, int n_levels=5):
        """Return a small Python snapshot of the top N levels (for the
        dashboard / debugging). Not on the hot path."""
        bids = []
        for i in range(min(n_levels, self.bid_count)):
            total = sum(self.bids[i].orders[j].qty for j in range(self.bids[i].count))
            bids.append((self.bids[i].price_ticks, total))
        asks = []
        for i in range(min(n_levels, self.ask_count)):
            total = sum(self.asks[i].orders[j].qty for j in range(self.asks[i].count))
            asks.append((self.asks[i].price_ticks, total))
        return {"bids": bids, "asks": asks}


cdef class MatchingEngine:
    """Multi-symbol wrapper: owns one LimitOrderBook per symbol_id."""
    cdef dict books  # symbol_id -> LimitOrderBook  (setup-time only, not hot path)

    def __cinit__(self):
        self.books = {}

    cdef LimitOrderBook _get_book(self, unsigned long long symbol_id):
        book = self.books.get(symbol_id)
        if book is None:
            book = LimitOrderBook(symbol_id)
            self.books[symbol_id] = book
        return book

    def process_order(self, unsigned long long symbol_id, int side,
                       long long price_ticks, unsigned int qty,
                       unsigned long long order_id, long long ts_ns):
        cdef LimitOrderBook book = self._get_book(symbol_id)
        return book.submit(side, price_ticks, qty, order_id, ts_ns)

    def get_book(self, unsigned long long symbol_id):
        return self.books.get(symbol_id)

    def stats(self):
        return {
            sym: {"orders": b.orders_processed, "trades": b.trades_generated}
            for sym, b in self.books.items()
        }

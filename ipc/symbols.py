"""Interned symbol table. The ring buffer wire format uses a uint16
symbol_id instead of a variable-length ticker string, so this module is
the single source of truth mapping between the two."""

SYMBOLS = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META", "AMD"]

SYMBOL_TO_ID = {sym: i for i, sym in enumerate(SYMBOLS)}
ID_TO_SYMBOL = {i: sym for sym, i in SYMBOL_TO_ID.items()}

PRICE_SCALE = 10_000  # price_ticks = round(price * PRICE_SCALE)


def to_ticks(price: float) -> int:
    return int(round(price * PRICE_SCALE))


def from_ticks(ticks: int) -> float:
    return ticks / PRICE_SCALE

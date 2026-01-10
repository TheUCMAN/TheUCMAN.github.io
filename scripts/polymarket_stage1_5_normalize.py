# polymarket_stage1_5_normalize.py

import json
from pathlib import Path
from datetime import datetime, UTC

RAW_DIR = Path("data/raw/polymarket")
OUT_DIR = Path("data/normalized/polymarket")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _as_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _unwrap_book(obj):
    """
    Polymarket HAR payloads can appear as:
      - {"token_id": ..., "bids": [...], "asks": [...]}
      - {"data": {...}}
      - {"book": {...}}
    This returns the dict that actually contains bids/asks.
    """
    if not isinstance(obj, dict):
        return None
    if "bids" in obj or "asks" in obj or "token_id" in obj:
        return obj
    if "data" in obj and isinstance(obj["data"], dict):
        return obj["data"]
    if "book" in obj and isinstance(obj["book"], dict):
        return obj["book"]
    return obj  # fallback

def _best_level(levels, side):
    """
    levels: list of [price, size] pairs, sometimes as strings.
    Returns (best_price, best_size).
    """
    if not isinstance(levels, list) or not levels:
        return (None, None)

    parsed = []
    for lv in levels:
        if not isinstance(lv, (list, tuple)) or len(lv) < 2:
            continue
        p = _as_float(lv[0])
        s = _as_float(lv[1])
        if p is None or s is None:
            continue
        parsed.append((p, s))

    if not parsed:
        return (None, None)

    # bids: max price, asks: min price
    if side == "bid":
        p, s = max(parsed, key=lambda t: t[0])
    else:
        p, s = min(parsed, key=lambda t: t[0])
    return (p, s)

# -----------------------------
# Load latest raw file
# -----------------------------
raw_files = sorted(RAW_DIR.glob("books_raw_*.json"))
if not raw_files:
    raise FileNotFoundError("No Polymarket raw books found (data/raw/polymarket/books_raw_*.json)")

raw_path = raw_files[-1]
print(f">>> Normalizing Polymarket books: {raw_path.name}")

with open(raw_path, "r", encoding="utf-8") as f:
    raw_books = json.load(f)

# -----------------------------
# Normalize
# -----------------------------
normalized = []
skipped = 0

for obj in raw_books:
    book = _unwrap_book(obj)
    if not isinstance(book, dict):
        skipped += 1
        continue

    token_id = book.get("token_id") or book.get("tokenId") or book.get("token")
    bids = book.get("bids", [])
    asks = book.get("asks", [])

    if token_id is None:
        skipped += 1
        continue

    best_bid, bid_size = _best_level(bids, "bid")
    best_ask, ask_size = _best_level(asks, "ask")

    mid = None
    spread = None
    if best_bid is not None and best_ask is not None:
        mid = round((best_bid + best_ask) / 2, 6)
        spread = round(best_ask - best_bid, 6)

    # Simple liquidity proxy: top-of-book sizes (not perfect, but stable)
    liquidity = 0.0
    if bid_size is not None:
        liquidity += bid_size
    if ask_size is not None:
        liquidity += ask_size

    normalized.append({
        "source": "polymarket",
        "token_id": str(token_id),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid,
        "spread": spread,
        "top_bid_size": bid_size,
        "top_ask_size": ask_size,
        "liquidity_proxy": round(liquidity, 6),
        "raw_snapshot": raw_path.name,
    })

# -----------------------------
# Write output
# -----------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUT_DIR / f"books_normalized_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(normalized, f, indent=2)

print("\nSTAGE P1.5 COMPLETE — POLYMARKET NORMALIZED")
print("------------------------------------------")
print(f"Raw books read: {len(raw_books)}")
print(f"Normalized rows: {len(normalized)}")
print(f"Skipped: {skipped}")
print(f"Wrote: {out_path.resolve()}")

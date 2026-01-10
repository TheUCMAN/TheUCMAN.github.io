import json
from pathlib import Path
from datetime import datetime, UTC

# -----------------------------
# CONFIG
# -----------------------------
RAW_DIR = Path("data/raw/polymarket").resolve()
OUT_DIR = Path("data/normalized/polymarket")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# LOAD LATEST RAW FILE
# -----------------------------
raw_files = sorted(RAW_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime)
if not raw_files:
    raise FileNotFoundError("No Polymarket raw books found in data/raw/polymarket")

raw_path = raw_files[-1]
print(f">>> Normalizing Polymarket books: {raw_path.name}")

with open(raw_path, "r", encoding="utf-8") as f:
    raw_books = json.load(f)

# -----------------------------
# NORMALIZATION
# -----------------------------
normalized = []
skipped = 0

def parse_side(side):
    """
    side = [[price, size], ...]
    Polymarket often encodes price/size as strings
    """
    if not side:
        return None
    try:
        price, size = side[0]
        return float(price), float(size)
    except Exception:
        return None

for book in raw_books:
    # Handle wrapped payloads
    if isinstance(book, dict) and "data" in book:
        book = book["data"]

    if not isinstance(book, dict):
        skipped += 1
        continue

    token_id = book.get("token_id")
    bids = book.get("bids")
    asks = book.get("asks")

    if not token_id or not bids or not asks:
        skipped += 1
        continue

    best_bid = parse_side(bids)
    best_ask = parse_side(asks)

    if not best_bid or not best_ask:
        skipped += 1
        continue

    bid_price, bid_size = best_bid
    ask_price, ask_size = best_ask

    mid_price = round((bid_price + ask_price) / 2, 4)
    spread = round(ask_price - bid_price, 4)
    liquidity = bid_size + ask_size

    normalized.append({
        "token_id": token_id,
        "best_bid": bid_price,
        "best_ask": ask_price,
        "mid_price": mid_price,
        "spread": spread,
        "liquidity": liquidity,
        "source": "polymarket"
    })

# -----------------------------
# WRITE OUTPUT
# -----------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUT_DIR / f"books_normalized_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(normalized, f, indent=2)

# -----------------------------
# SUMMARY
# -----------------------------
print("\nSTAGE P1.5 COMPLETE — POLYMARKET NORMALIZED")
print("------------------------------------------")
print(f"Raw books read: {len(raw_books)}")
print(f"Normalized rows: {len(normalized)}")
print(f"Skipped: {skipped}")
print(f"Wrote: {out_path.resolve()}")

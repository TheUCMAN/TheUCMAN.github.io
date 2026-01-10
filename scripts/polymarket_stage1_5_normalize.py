import json
from pathlib import Path
from datetime import datetime, UTC

RAW_DIR = Path("data/raw/polymarket").resolve()
OUT_DIR = Path("data/normalized/polymarket")
OUT_DIR.mkdir(parents=True, exist_ok=True)

raw_files = sorted(RAW_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime)
if not raw_files:
    raise FileNotFoundError("No Polymarket raw books found")

raw_path = raw_files[-1]
print(f">>> Normalizing Polymarket books: {raw_path.name}")

raw_books = json.load(open(raw_path, "r", encoding="utf-8"))

normalized = []
skipped = {
    "no_asset": 0,
    "no_books": 0,
    "parse_fail": 0
}

def parse_level(level):
    """
    Supports:
    - ["0.54", "1200"]
    - {"price": "0.54", "size": "1200"}
    """
    try:
        if isinstance(level, list):
            return float(level[0]), float(level[1])
        if isinstance(level, dict):
            return float(level["price"]), float(level["size"])
    except Exception:
        return None
    return None

for book in raw_books:
    if not isinstance(book, dict):
        skipped["parse_fail"] += 1
        continue

    asset_id = book.get("asset_id")
    bids = book.get("bids")
    asks = book.get("asks")

    if not asset_id:
        skipped["no_asset"] += 1
        continue

    if not bids or not asks:
        skipped["no_books"] += 1
        continue

    bid = parse_level(bids[0])
    ask = parse_level(asks[0])

    if not bid or not ask:
        skipped["parse_fail"] += 1
        continue

    bid_price, bid_size = bid
    ask_price, ask_size = ask

    mid_price = round((bid_price + ask_price) / 2, 5)
    spread = round(ask_price - bid_price, 5)
    liquidity = bid_size + ask_size

    normalized.append({
        "market_id": asset_id,
        "best_bid": bid_price,
        "best_ask": ask_price,
        "mid_price": mid_price,
        "spread": spread,
        "liquidity": liquidity,
        "source": "polymarket"
    })

timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUT_DIR / f"books_normalized_{timestamp}.json"

json.dump(normalized, open(out_path, "w", encoding="utf-8"), indent=2)

print("\nSTAGE P1.5 COMPLETE — POLYMARKET NORMALIZED")
print("------------------------------------------")
print(f"Raw books read: {len(raw_books)}")
print(f"Normalized rows: {len(normalized)}")
print(f"Skipped (no asset_id): {skipped['no_asset']}")
print(f"Skipped (no liquidity): {skipped['no_books']}")
print(f"Skipped (parse fail): {skipped['parse_fail']}")
print(f"Wrote: {out_path.resolve()}")

import json
from pathlib import Path
from datetime import datetime, UTC

# ---------------------------------
# CONFIG
# ---------------------------------
BOOKS_DIR = Path("data/normalized/polymarket")
EVENTS_DIR = Path("data/normalized/polymarket")
OUTPUT_DIR = Path("data/computed/polymarket")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------
# LOAD LATEST FILES
# ---------------------------------
books_file = sorted(BOOKS_DIR.glob("books_normalized_*.json"))[-1]
events_file = sorted(EVENTS_DIR.glob("gamma_events_*.json"))[-1]

print(f">>> Joining Polymarket books + events")
print(f"Books:  {books_file.name}")
print(f"Events: {events_file.name}")

books = json.load(open(books_file, "r", encoding="utf-8"))
events = json.load(open(events_file, "r", encoding="utf-8"))

# ---------------------------------
# BUILD TOKEN → MARKET MAP
# ---------------------------------
token_map = {}

for market in events:
    for outcome in market.get("outcomes", []):
        token_id = outcome.get("token_id")
        if not token_id:
            continue

        token_map[token_id] = {
            "market_id": market.get("market_id"),
            "title": market.get("title"),
            "category": market.get("category"),
            "slug": market.get("slug"),
            "outcome": outcome.get("label"),
            "side": outcome.get("side")
        }

# ---------------------------------
# JOIN BOOKS
# ---------------------------------
rows = []
skipped = 0

for book in books:
    token_id = book.get("asset_id")
    if token_id not in token_map:
        skipped += 1
        continue

    meta = token_map[token_id]

    rows.append({
        "market_id": meta["market_id"],
        "title": meta["title"],
        "category": meta["category"],
        "outcome": meta["outcome"],
        "side": meta["side"],
        "asset_id": token_id,
        "best_bid": book.get("best_bid"),
        "best_ask": book.get("best_ask"),
        "mid_price": book.get("mid_price"),
        "liquidity": book.get("liquidity"),
        "last_trade_price": book.get("last_trade_price"),
        "timestamp": book.get("timestamp")
    })

# ---------------------------------
# WRITE OUTPUT
# ---------------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUTPUT_DIR / f"polymarket_joined_{timestamp}.json"

json.dump(rows, open(out_path, "w", encoding="utf-8"), indent=2)

# ---------------------------------
# SUMMARY
# ---------------------------------
print("\nSTAGE P2B COMPLETE — POLYMARKET JOINED SIGNALS")
print("---------------------------------------------")
print(f"Books processed: {len(books)}")
print(f"Joined rows:     {len(rows)}")
print(f"Skipped books:   {skipped}")
print(f"Wrote: {out_path.resolve()}")

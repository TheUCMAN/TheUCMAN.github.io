# scripts/polymarket_stage2C_final_join.py

import json
from pathlib import Path
from datetime import datetime, UTC

BOOKS_DIR = Path("data/normalized/polymarket")
OUTCOMES_DIR = Path("data/normalized/polymarket")
OUT_DIR = Path("data/computed/polymarket")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Load latest files
books_file = sorted(BOOKS_DIR.glob("books_normalized_*.json"))[-1]
outcomes_file = sorted(OUTCOMES_DIR.glob("gamma_outcomes_*.json"))[-1]

print(">>> STAGE P2C FINAL — JOIN BOOKS → OUTCOMES")
print(f"Books:    {books_file.name}")
print(f"Outcomes: {outcomes_file.name}")

books = json.load(open(books_file, "r", encoding="utf-8"))
outcomes = json.load(open(outcomes_file, "r", encoding="utf-8"))

joined = []
skipped = 0

for book in books:
    token = book.get("asset_id")
    if not token:
        skipped += 1
        continue

    meta = outcomes.get(token)
    if not meta:
        skipped += 1
        continue

    joined.append({
        "token": token,
        "event": meta.get("event_title"),
        "market": meta.get("market_question"),
        "outcome": meta.get("outcome_name"),
        "side": meta.get("side"),

        # Pricing
        "best_bid": book.get("best_bid"),
        "best_ask": book.get("best_ask"),
        "last_trade": book.get("last_trade_price"),
        "liquidity": book.get("liquidity"),

        # Raw depth (optional, powerful later)
        "bids": book.get("bids"),
        "asks": book.get("asks"),
    })

ts = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUT_DIR / f"polymarket_joined_markets_{ts}.json"

json.dump(joined, open(out_path, "w", encoding="utf-8"), indent=2)

print("\nSTAGE P2C FINAL COMPLETE")
print("------------------------")
print(f"Books processed: {len(books)}")
print(f"Joined rows:     {len(joined)}")
print(f"Skipped books:   {skipped}")
print(f"Wrote: {out_path.resolve()}")

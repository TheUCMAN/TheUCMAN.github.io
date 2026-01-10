# scripts/polymarket_stage3B_join_books_to_clob.py

import json
from pathlib import Path
from datetime import datetime, UTC

# -----------------------------
# PATHS
# -----------------------------
BOOKS_DIR = Path("data/normalized/polymarket")
CLOB_DIR = Path("data/normalized/polymarket")
OUTPUT_DIR = Path("data/computed/polymarket")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# LOAD LATEST FILES
# -----------------------------
books_file = sorted(BOOKS_DIR.glob("books_normalized_*.json"))[-1]
clob_file = sorted(CLOB_DIR.glob("clob_market_outcomes_*.json"))[-1]

print(">>> STAGE P3B — JOIN BOOKS → CLOB OUTCOMES")
print(f"Books: {books_file.name}")
print(f"CLOB:  {clob_file.name}")

with open(books_file, "r", encoding="utf-8") as f:
    books = json.load(f)

with open(clob_file, "r", encoding="utf-8") as f:
    markets = json.load(f)

# -----------------------------
# BUILD TOKEN INDEX
# -----------------------------
token_index = {}

for m in markets:
    market_id = m.get("marketId")
    for o in m.get("outcomes", []):
        token = o.get("clobTokenId")
        if token:
            token_index[token] = {
                "marketId": market_id,
                "outcome": o.get("outcome")
            }

# -----------------------------
# JOIN
# -----------------------------
joined = []
skipped = 0

for b in books:
    token = b.get("asset_id")
    if token not in token_index:
        skipped += 1
        continue

    meta = token_index[token]

    joined.append({
        "marketId": meta["marketId"],
        "outcome": meta["outcome"],
        "clobTokenId": token,
        "best_bid": b.get("best_bid"),
        "best_ask": b.get("best_ask"),
        "mid_price": b.get("mid_price"),
        "bid_liquidity": b.get("bid_liquidity"),
        "ask_liquidity": b.get("ask_liquidity"),
        "last_trade_price": b.get("last_trade_price"),
        "timestamp": b.get("timestamp")
    })

# -----------------------------
# WRITE OUTPUT
# -----------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUTPUT_DIR / f"polymarket_live_markets_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(joined, f, indent=2)

# -----------------------------
# SUMMARY
# -----------------------------
print("\nSTAGE P3B COMPLETE — LIVE POLYMARKET MARKETS")
print("--------------------------------------------")
print(f"Books processed: {len(books)}")
print(f"Joined rows:     {len(joined)}")
print(f"Skipped books:   {skipped}")
print(f"Wrote: {out_path.resolve()}")

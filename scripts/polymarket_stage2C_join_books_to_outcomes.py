import json
from pathlib import Path
from datetime import datetime, UTC

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
BOOKS_DIR = Path("data/normalized/polymarket")
EVENTS_DIR = Path("data/normalized/polymarket")
OUTPUT_DIR = Path("data/computed/polymarket")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------
# LOAD LATEST FILES
# -------------------------------------------------
books_file = sorted(BOOKS_DIR.glob("books_normalized_*.json"))[-1]
events_file = sorted(EVENTS_DIR.glob("gamma_events_*.json"))[-1]

print(">>> STAGE P2C — JOIN BOOKS TO OUTCOMES")
print(f"Books file:  {books_file.name}")
print(f"Events file: {events_file.name}")

books = json.load(open(books_file, "r", encoding="utf-8"))
events = json.load(open(events_file, "r", encoding="utf-8"))

# -------------------------------------------------
# BUILD clobTokenId → outcome metadata map
# -------------------------------------------------
outcome_map = {}
outcomes_seen = 0

for event in events:
    event_title = event.get("title")
    category = event.get("category")
    slug = event.get("slug")
    market_id = event.get("market_id")

    for market in event.get("markets", []) if "markets" in event else []:
        for outcome in market.get("outcomes", []):
            clob_token = outcome.get("clobTokenId")
            if not clob_token:
                continue

            outcome_map[clob_token] = {
                "market_id": market_id,
                "event_title": event_title,
                "slug": slug,
                "category": category,
                "market_question": market.get("question"),
                "outcome_label": outcome.get("name"),
                "outcome_side": outcome.get("side"),
            }
            outcomes_seen += 1

# -------------------------------------------------
# JOIN BOOKS → OUTCOMES
# -------------------------------------------------
joined = []
skipped = 0

for book in books:
    asset_id = book.get("asset_id")
    if asset_id not in outcome_map:
        skipped += 1
        continue

    meta = outcome_map[asset_id]

    joined.append({
        "market_id": meta["market_id"],
        "event_title": meta["event_title"],
        "market_question": meta["market_question"],
        "outcome": meta["outcome_label"],
        "side": meta["outcome_side"],
        "category": meta["category"],
        "slug": meta["slug"],
        "asset_id": asset_id,
        "best_bid": book.get("best_bid"),
        "best_ask": book.get("best_ask"),
        "mid_price": book.get("mid_price"),
        "liquidity": book.get("liquidity"),
        "last_trade_price": book.get("last_trade_price"),
        "timestamp": book.get("timestamp"),
        "source": "polymarket"
    })

# -------------------------------------------------
# WRITE OUTPUT
# -------------------------------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUTPUT_DIR / f"polymarket_joined_outcomes_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(joined, f, indent=2)

# -------------------------------------------------
# SUMMARY
# -------------------------------------------------
print("\nSTAGE P2C COMPLETE — POLYMARKET JOINED OUTCOMES")
print("------------------------------------------------")
print(f"Books processed:      {len(books)}")
print(f"Outcome mappings:     {len(outcome_map)}")
print(f"Joined rows produced: {len(joined)}")
print(f"Skipped books:        {skipped}")
print(f"Wrote: {out_path.resolve()}")

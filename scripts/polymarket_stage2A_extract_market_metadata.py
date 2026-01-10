import json
from pathlib import Path
from datetime import datetime, UTC

RAW_DIR = Path("data/raw/polymarket").resolve()
OUT_DIR = Path("data/normalized/polymarket")
OUT_DIR.mkdir(parents=True, exist_ok=True)

raw_files = sorted(RAW_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime)
if not raw_files:
    raise FileNotFoundError("No Polymarket raw files found")

raw_path = raw_files[-1]
print(f">>> Extracting Polymarket market metadata from: {raw_path.name}")

raw_books = json.load(open(raw_path, "r", encoding="utf-8"))

seen = {}
extracted = []

for book in raw_books:
    if not isinstance(book, dict):
        continue

    asset_id = book.get("asset_id")
    market = book.get("market")

    if not asset_id or not isinstance(market, dict):
        continue

    if asset_id in seen:
        continue  # avoid duplicates

    title = market.get("title") or market.get("name")
    question = market.get("question") or market.get("description")
    slug = market.get("slug")

    extracted.append({
        "polymarket_market_id": asset_id,
        "title": title,
        "question": question,
        "slug": slug
    })

    seen[asset_id] = True

timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUT_DIR / f"polymarket_markets_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(extracted, f, indent=2)

print("\nSTAGE P2A-1 COMPLETE — POLYMARKET METADATA")
print("-----------------------------------------")
print(f"Unique markets extracted: {len(extracted)}")
print(f"Wrote: {out_path.resolve()}")

import json
from collections import Counter

PATH = "data/raw/polymarket/books_raw_2026-01-10T01-49-41Z.json"

with open(PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

asset_ids = [b.get("asset_id") for b in data if b.get("asset_id")]

print("TOTAL BOOKS:", len(data))
print("BOOKS WITH ASSET_ID:", len(asset_ids))
print("UNIQUE ASSET IDS:", len(set(asset_ids)))
print("\nTOP 10 ASSET IDS:")
for aid, cnt in Counter(asset_ids).most_common(10):
    print(aid, cnt)

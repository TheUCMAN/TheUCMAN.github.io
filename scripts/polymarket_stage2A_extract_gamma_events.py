import json
from pathlib import Path
from datetime import datetime, UTC

# ---------------------------------
# CONFIG
# ---------------------------------
HAR_DIR = Path(".")
OUTPUT_DIR = Path("data/normalized/polymarket")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_HOST = "gamma-api.polymarket.com"
TARGET_PATH = "/events/pagination"

# ---------------------------------
# LOAD LATEST HAR
# ---------------------------------
har_files = sorted(HAR_DIR.glob("*.har"), key=lambda p: p.stat().st_mtime)
if not har_files:
    raise FileNotFoundError("No HAR files found in repo root")

har_path = har_files[-1]
print(f">>> Parsing Gamma events from HAR: {har_path.name}")

with open(har_path, "r", encoding="utf-8") as f:
    har = json.load(f)

entries = har.get("log", {}).get("entries", [])

# ---------------------------------
# EXTRACT EVENTS
# ---------------------------------
markets = {}
matched_requests = 0

for entry in entries:
    request = entry.get("request", {})
    response = entry.get("response", {})
    url = request.get("url", "")

    if TARGET_HOST not in url:
        continue
    if TARGET_PATH not in url:
        continue

    content = response.get("content", {})
    text = content.get("text")
    if not text:
        continue

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        continue

    matched_requests += 1

    data = payload.get("data", [])
    for event in data:
        market_id = event.get("id")
        if not market_id:
            continue

        market = {
            "market_id": market_id,
            "ticker": event.get("ticker"),
            "slug": event.get("slug"),
            "title": event.get("title"),
            "category": event.get("category"),
            "subcategory": event.get("subcategory"),
            "end_time": event.get("endTime"),
            "outcomes": []
        }

        for outcome in event.get("outcomes", []):
            market["outcomes"].append({
                "label": outcome.get("name"),
                "token_id": outcome.get("tokenId"),
                "side": outcome.get("side")
            })

        markets[market_id] = market

# ---------------------------------
# WRITE OUTPUT
# ---------------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUTPUT_DIR / f"gamma_events_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(list(markets.values()), f, indent=2)

# ---------------------------------
# SUMMARY
# ---------------------------------
print("\nSTAGE P2A COMPLETE — GAMMA EVENT EXTRACTION")
print("-------------------------------------------")
print(f"HAR file: {har_path.name}")
print(f"Gamma requests matched: {matched_requests}")
print(f"Unique markets extracted: {len(markets)}")
print(f"Wrote: {out_path.resolve()}")

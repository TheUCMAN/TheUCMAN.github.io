# scripts/polymarket_stage3A_extract_clob_markets.py

import json
from pathlib import Path
from datetime import datetime, UTC
import re

# -----------------------------
# CONFIG
# -----------------------------
HAR_DIR = Path(".")
OUTPUT_DIR = Path("data/normalized/polymarket")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_HOST = "clob.polymarket.com"
TARGET_PATH_PREFIX = "/rewards/markets/"

# -----------------------------
# LOAD LATEST HAR
# -----------------------------
har_files = sorted(HAR_DIR.glob("*.har"), key=lambda p: p.stat().st_mtime)
if not har_files:
    raise FileNotFoundError("No HAR files found in repo root")

har_path = har_files[-1]
print(f">>> Parsing CLOB markets from HAR: {har_path.name}")

with open(har_path, "r", encoding="utf-8") as f:
    har = json.load(f)

entries = har.get("log", {}).get("entries", [])

# -----------------------------
# EXTRACT MARKET OUTCOMES
# -----------------------------
markets = {}
matched_requests = 0

for entry in entries:
    req = entry.get("request", {})
    res = entry.get("response", {})
    url = req.get("url", "")

    if TARGET_HOST not in url:
        continue
    if TARGET_PATH_PREFIX not in url:
        continue

    matched_requests += 1

    # Extract marketId from URL
    m = re.search(r"/rewards/markets/([0-9a-fx]+)", url)
    if not m:
        continue

    market_id = m.group(1)

    content = res.get("content", {})
    text = content.get("text")
    if not text:
        continue

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        continue

    outcomes = []

    # Polymarket returns outcomes in different shapes
    for key in ["outcomes", "tokens", "marketTokens"]:
        if key in payload and isinstance(payload[key], list):
            for o in payload[key]:
                token = o.get("clobTokenId") or o.get("tokenId")
                label = o.get("label") or o.get("name")
                if token:
                    outcomes.append({
                        "outcome": label,
                        "clobTokenId": token
                    })

    markets[market_id] = {
        "marketId": market_id,
        "outcomes": outcomes,
        "raw_keys": list(payload.keys())
    }

# -----------------------------
# WRITE OUTPUT
# -----------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUTPUT_DIR / f"clob_market_outcomes_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(list(markets.values()), f, indent=2)

# -----------------------------
# SUMMARY
# -----------------------------
print("\nSTAGE P3A COMPLETE — CLOB MARKET OUTCOMES")
print("----------------------------------------")
print(f"HAR file:              {har_path.name}")
print(f"Market requests hit:   {matched_requests}")
print(f"Unique markets parsed: {len(markets)}")
print(f"Wrote: {out_path.resolve()}")

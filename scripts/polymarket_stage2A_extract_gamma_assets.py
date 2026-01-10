# polymarket_stage2A_extract_gamma_assets.py

import json
from pathlib import Path
from datetime import datetime, UTC

# -----------------------------
# CONFIG
# -----------------------------
HAR_DIR = Path(".")
OUTPUT_DIR = Path("data/normalized/polymarket")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_HOST = "gamma-api.polymarket.com"
TARGET_PATH_KEYWORD = "/markets/"

# -----------------------------
# LOAD LATEST HAR
# -----------------------------
har_files = sorted(HAR_DIR.glob("*.har"), key=lambda p: p.stat().st_mtime)
if not har_files:
    raise FileNotFoundError("No .har files found")

har_path = har_files[-1]
print(f">>> Parsing Gamma assets from HAR: {har_path.name}")

with open(har_path, "r", encoding="utf-8") as f:
    har = json.load(f)

entries = har.get("log", {}).get("entries", [])

# -----------------------------
# EXTRACT ASSET MAPPINGS
# -----------------------------
asset_index = {}
matched_requests = 0

for entry in entries:
    req = entry.get("request", {})
    res = entry.get("response", {})
    url = req.get("url", "")

    if TARGET_HOST not in url:
        continue
    if TARGET_PATH_KEYWORD not in url:
        continue

    content = res.get("content", {})
    text = content.get("text")
    if not text:
        continue

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        continue

    matched_requests += 1

    market_id = payload.get("id") or payload.get("marketId")

    for asset in payload.get("assets", []):
        clob_token = asset.get("clobTokenId")
        asset_id = asset.get("assetId")

        if not clob_token or not asset_id:
            continue

        asset_index[str(clob_token)] = {
            "asset_id": asset_id,
            "market_id": market_id
        }

# -----------------------------
# WRITE OUTPUT
# -----------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUTPUT_DIR / f"gamma_assets_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(asset_index, f, indent=2)

# -----------------------------
# SUMMARY
# -----------------------------
print("\nSTAGE P2A-4 COMPLETE — GAMMA ASSET EXTRACTION")
print("---------------------------------------------")
print(f"HAR file: {har_path.name}")
print(f"Gamma market requests matched: {matched_requests}")
print(f"Asset mappings extracted: {len(asset_index)}")
print(f"Wrote: {out_path.resolve()}")

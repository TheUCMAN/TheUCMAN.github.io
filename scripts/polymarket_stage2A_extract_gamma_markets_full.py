import json
from pathlib import Path
from datetime import datetime, UTC

# -----------------------------
# CONFIG
# -----------------------------
HAR_DIR = Path(".")
OUTPUT_DIR = Path("data/normalized/polymarket")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_HOST = "clob.polymarket.com"
TARGET_PATH_FRAGMENT = "/rewards/markets/"

# -----------------------------
# LOAD LATEST HAR
# -----------------------------
har_files = sorted(HAR_DIR.glob("*.har"), key=lambda p: p.stat().st_mtime)
if not har_files:
    raise FileNotFoundError("No .har files found in repo root")

har_path = har_files[-1]
print(f">>> Parsing FULL Gamma markets from HAR: {har_path.name}")

with open(har_path, "r", encoding="utf-8") as f:
    har = json.load(f)

entries = har.get("log", {}).get("entries", [])

# -----------------------------
# EXTRACT FULL MARKET PAYLOADS
# -----------------------------
markets = {}
matched_requests = 0

for entry in entries:
    request = entry.get("request", {})
    response = entry.get("response", {})
    url = request.get("url", "")

    if TARGET_HOST not in url:
        continue
    if TARGET_PATH_FRAGMENT not in url:
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

    # payload is a SINGLE market object
    condition_id = payload.get("condition_id") or payload.get("id")
    if not condition_id:
        continue

    markets[str(condition_id)] = payload

# -----------------------------
# WRITE OUTPUT
# -----------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUTPUT_DIR / f"gamma_markets_full_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(list(markets.values()), f, indent=2)

# -----------------------------
# SUMMARY
# -----------------------------
print("\nSTAGE P2A-2 COMPLETE — FULL GAMMA MARKETS")
print("----------------------------------------")
print(f"HAR file: {har_path.name}")
print(f"Gamma market requests matched: {matched_requests}")
print(f"Unique markets extracted:     {len(markets)}")
print(f"Wrote: {out_path.resolve()}")

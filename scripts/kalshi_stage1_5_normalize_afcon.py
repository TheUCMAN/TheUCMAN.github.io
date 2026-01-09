import json
from pathlib import Path
from datetime import datetime, timezone

print(">>> RUNNING AFCON STAGE 1.5 NORMALIZE <<<")

# ==========================================================
# PATHS
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = REPO_ROOT / "data" / "raw" / "kalshi" / "afcon"
OUT_DIR = REPO_ROOT / "data" / "normalized" / "afcon"

OUT_DIR.mkdir(parents=True, exist_ok=True)

raw_files = sorted(RAW_DIR.glob("events_raw_*.json"))
if not raw_files:
    raise SystemExit("No raw AFCON files found")

INPUT_FILE = raw_files[-1]

timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
OUT_FILE = OUT_DIR / f"markets_flat_{timestamp}.json"

# ==========================================================
# LOAD RAW
# ==========================================================

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    raw = json.load(f)

events = raw.get("events", [])
print(f"Loaded {len(events)} AFCON events")

# ==========================================================
# NORMALIZE
# ==========================================================

flat_markets = []

for ev in events:
    event_title = ev.get("title") or ev.get("name") or "UNKNOWN_EVENT"

    markets = ev.get("markets") or []
    if not isinstance(markets, list):
        continue

    for m in markets:
        record = {
            "league": "AFCON",
            "event": event_title,
            "market_title": m.get("title") or m.get("name") or "UNKNOWN_MARKET",

            # --- normalized pricing fields (safe)
            "yes_price": m.get("last_price"),
            "yes_bid": m.get("yes_bid"),
            "yes_ask": m.get("yes_ask"),
            "no_price": None,  # often absent; filled later if available

            # --- liquidity
            "volume": m.get("volume", 0),
            "open_interest": m.get("open_interest", 0),

            # --- metadata
            "status": m.get("status"),
            "close_time": m.get("close_date") or m.get("expiration_date"),

            # --- keep raw for schema drift debugging
            "raw_market": m
        }

        flat_markets.append(record)

# ==========================================================
# WRITE OUTPUT
# ==========================================================

with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(flat_markets, f, indent=2)

print("Stage 1.5 complete")
print(f"Events processed: {len(events)}")
print(f"Markets flattened: {len(flat_markets)}")
print(f"Wrote: {OUT_FILE}")

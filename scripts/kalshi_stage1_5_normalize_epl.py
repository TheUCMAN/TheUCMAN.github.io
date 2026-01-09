import json
from pathlib import Path
from datetime import datetime

# ==========================================================
# PATHS
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = REPO_ROOT / "data" / "raw" / "kalshi" / "epl"
OUT_DIR = REPO_ROOT / "data" / "normalized" / "epl"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Use latest.json for now (we’ll version next)
RAW_FILE = RAW_DIR / "latest.json"

timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
OUT_FILE = OUT_DIR / f"markets_flat_{timestamp}.json"

# ==========================================================
# LOAD RAW
# ==========================================================

if not RAW_FILE.exists():
    raise SystemExit("Missing raw EPL latest.json")

with open(RAW_FILE, "r", encoding="utf-8") as f:
    raw = json.load(f)

events = raw.get("events", [])
rows = []

# ==========================================================
# FLATTEN MARKETS (NO INTERPRETATION)
# ==========================================================

for ev in events:
    event_id = ev.get("ticker") or ev.get("id")
    event_title = ev.get("title")
    event_time = ev.get("target_datetime")

    for m in ev.get("markets", []):
        row = {
            "event_id": event_id,
            "event_title": event_title,
            "event_time": event_time,

            "market_id": m.get("ticker") or m.get("id"),
            "market_title": m.get("title"),
            "market_type_guess": None,   # intentionally blank

            "yes_price": m.get("yes_price"),
            "no_price": m.get("no_price"),
            "volume": m.get("volume"),
            "open_interest": m.get("open_interest"),

            # Keep full raw market for future adapters
            "raw_market": m,
        }

        rows.append(row)

# ==========================================================
# OUTPUT
# ==========================================================

with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(rows, f, indent=2)

print(f"Stage 1.5 complete")
print(f"Events processed: {len(events)}")
print(f"Markets flattened: {len(rows)}")
print(f"Wrote: {OUT_FILE}")

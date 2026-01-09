import requests
import json
from pathlib import Path
from datetime import datetime, timezone

print(">>> RUNNING AFCON STAGE 1 PULL <<<")

# ==========================================================
# PATHS
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "data" / "raw" / "kalshi" / "afcon"
OUT_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
OUT_FILE = OUT_DIR / f"events_raw_{timestamp}.json"

# ==========================================================
# KALSHI API
# ==========================================================

BASE_URL = "https://api.kalshi.com/trade-api/v2/events"

params = {
    "limit": 100,
    "category": "Sports"
}

resp = requests.get(BASE_URL, params=params, timeout=30)
resp.raise_for_status()

data = resp.json()

# ==========================================================
# FILTER AFCON EVENTS
# ==========================================================

events = data.get("events", [])

afcon_events = []
for ev in events:
    title = (ev.get("title") or "").lower()
    series = (ev.get("series_ticker") or "").lower()

    if "afcon" in title or "africa cup" in title or "afcon" in series:
        afcon_events.append(ev)

print(f"Total events fetched: {len(events)}")
print(f"AFCON events matched: {len(afcon_events)}")

# ==========================================================
# WRITE OUTPUT
# ==========================================================

out = {
    "source": "kalshi",
    "league": "AFCON",
    "fetched_at": timestamp,
    "events": afcon_events
}

with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(out, f, indent=2)

print(f"Wrote raw AFCON snapshot: {OUT_FILE}")

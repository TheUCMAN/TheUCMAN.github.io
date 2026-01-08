import requests
import json
from datetime import datetime, timezone
from pathlib import Path

# ==========================================================
# CONFIG — EPL ONLY
# ==========================================================

EVENT_TICKERS = [
    "KXEPLGAME-26JAN07BOUTOT",
    "KXEPLGAME-26JAN07BRESUN",
    "KXEPLGAME-26JAN07BURMUN",
    "KXEPLGAME-26JAN07CRYAVL",
    "KXEPLGAME-26JAN07EVEWOL",
    "KXEPLGAME-26JAN07FULCFC",
    "KXEPLGAME-26JAN07MCIBRI",
    "KXEPLGAME-26JAN07NEWLEE",
]

BASE_URL = "https://api.elections.kalshi.com/v1/events/"

HEADERS = {
    "User-Agent": "arbstack-bot/1.0",
    "Accept": "application/json",
}

PARAMS = {
    "single_event_per_series": "false",
    "tickers": ",".join(EVENT_TICKERS),
    "page_size": 100,
    "page_number": 1,
}

# ==========================================================
# PATHS — REPO RELATIVE (cloud-safe)
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = REPO_ROOT / "data" / "raw" / "kalshi" / "epl"
RAW_DIR.mkdir(parents=True, exist_ok=True)

LATEST_PTR = RAW_DIR / "latest.json"

# ==========================================================
# RUN
# ==========================================================

print("Pulling EPL events from Kalshi...")

resp = requests.get(
    BASE_URL,
    headers=HEADERS,
    params=PARAMS,
    timeout=20,
)

print("HTTP status:", resp.status_code)

if resp.status_code != 200:
    print(resp.text)
    raise SystemExit("Kalshi EPL request failed")

data = resp.json()

events = data.get("events", [])
print("Number of EPL events:", len(events))

# ==========================================================
# SAVE SNAPSHOT + LATEST POINTER
# ==========================================================

ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")

snapshot_file = RAW_DIR / f"events_raw_{ts}.json"

with open(snapshot_file, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

with open(LATEST_PTR, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

print("Saved snapshot:", snapshot_file)
print("Updated latest.json")

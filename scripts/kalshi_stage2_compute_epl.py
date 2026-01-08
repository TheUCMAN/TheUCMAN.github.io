import json
from pathlib import Path

# ==========================================================
# PATHS
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_LATEST = REPO_ROOT / "data" / "raw" / "kalshi" / "epl" / "latest.json"

# ==========================================================
# HELPERS
# ==========================================================

def parse_teams(title: str):
    if " vs " not in title:
        return None, None
    home, away = title.split(" vs ", 1)
    return home.strip(), away.strip()

def extract_prices(event, home, away):
    prices = {}
    volume = 0

    for m in event.get("markets", []):
        title = m.get("title", "").lower()
        price = m.get("yes_price")
        vol = m.get("volume", 0)

        if price is None:
            continue

        if price > 1:
            price = price / 100

        volume += vol

        if home.lower() in title:
            prices["home"] = price
        elif away.lower() in title:
            prices["away"] = price
        elif "draw" in title or "tie" in title:
            prices["tie"] = price

    return prices, volume

# ==========================================================
# LOAD DATA
# ==========================================================

if not RAW_LATEST.exists():
    raise SystemExit("Missing EPL latest.json")

data = json.load(open(RAW_LATEST, "r", encoding="utf-8"))
events = data.get("events", [])

print(f"Loaded {len(events)} EPL events\n")

# ==========================================================
# DIAGNOSTIC LOOP
# ==========================================================

for ev in events:
    match = ev.get("title", "")
    home, away = parse_teams(match)

    if not home or not away:
        print("SKIP (bad title):", match)
        continue

    prices, volume = extract_prices(ev, home, away)

    # 🔍 THE KEY LINE
    print(match)
    print("  prices:", prices)
    print("  volume:", volume)
    print("-" * 50)

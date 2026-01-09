# bookmaker_stage1_5_normalize_365_epl.py

import json
from pathlib import Path
from datetime import datetime, UTC

RAW_DIR = Path("data/raw/bookmakers/365/epl")
OUT_DIR = Path("data/normalized/bookmakers/365/epl")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BOOKMAKER_ID = 103  # BetMGM
TARGET_MARKET = "Full Time Result"
TARGET_COMP = "Premier League"

def implied_prob(decimal_odds):
    try:
        return round(1 / float(decimal_odds), 4)
    except Exception:
        return None

# -----------------------------
# Load latest raw snapshot
# -----------------------------
raw_files = sorted(RAW_DIR.glob("games_raw_*.json"))
if not raw_files:
    raise FileNotFoundError("No raw 365Scores EPL files found")

raw_path = raw_files[-1]
print(f">>> Normalizing bookmaker file: {raw_path.name}")

with open(raw_path, "r", encoding="utf-8") as f:
    games = json.load(f)

# -----------------------------
# Normalize & Deduplicate
# -----------------------------
normalized = {}

for g in games:
    if g.get("competitionDisplayName") != TARGET_COMP:
        continue

    markets = g.get("markets", [])
    for m in markets:
        if m.get("lineType", {}).get("name") != TARGET_MARKET:
            continue

        odds = m.get("odds", [])
        for o in odds:
            if o.get("bookmakerId") != BOOKMAKER_ID:
                continue

            home = g.get("homeCompetitor", {}).get("name")
            away = g.get("awayCompetitor", {}).get("name")
            start = g.get("startTime")

            if not home or not away:
                continue

            canonical_match = f"EPL|{home.upper()}|{away.upper()}|{start[:10]}"

            prices = {}
            for opt in o.get("prices", []):
                key = opt.get("name", "").upper()
                if key in ("1", "X", "2"):
                    prices[key] = opt.get("decimal")

            if not all(k in prices for k in ("1", "X", "2")):
                continue

            p_home = implied_prob(prices["1"])
            p_draw = implied_prob(prices["X"])
            p_away = implied_prob(prices["2"])

            if None in (p_home, p_draw, p_away):
                continue

            overround = round(p_home + p_draw + p_away, 4)

            normalized[canonical_match] = {
                "canonical_match": canonical_match,
                "home_team": home,
                "away_team": away,
                "start_time": start,
                "bookmaker": "BetMGM",
                "market": "FULL_TIME_RESULT",
                "odds": {
                    "home": prices["1"],
                    "draw": prices["X"],
                    "away": prices["2"]
                },
                "implied_prob": {
                    "home": p_home,
                    "draw": p_draw,
                    "away": p_away
                },
                "overround": overround,
                "source": "365scores",
                "snapshot": raw_path.name
            }

# -----------------------------
# Write Output
# -----------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUT_DIR / f"bookmaker_365_epl_normalized_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(list(normalized.values()), f, indent=2)

print("\nSTAGE B1.5 COMPLETE — 365Scores NORMALIZED")
print("----------------------------------------")
print(f"Raw games read: {len(games)}")
print(f"Unique EPL matches normalized: {len(normalized)}")
print(f"Wrote: {out_path.resolve()}")

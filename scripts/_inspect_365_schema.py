import json
from pathlib import Path
from pprint import pprint

RAW_DIR = Path("data/raw/bookmakers/365/epl")
raw_files = sorted(RAW_DIR.glob("games_raw_*.json"))
raw_path = raw_files[-1]

print("Inspecting:", raw_path.name)

with open(raw_path, "r", encoding="utf-8") as f:
    games = json.load(f)

print("\nTOTAL GAMES:", len(games))

# Inspect the first game that actually has markets
found = False
for idx, g in enumerate(games):
    markets = g.get("markets", [])
    if not markets:
        continue

    print(f"\n=== FIRST GAME WITH MARKETS (index {idx}) ===")
    print("Keys:", g.keys())

    print("Competition:", g.get("competitionDisplayName"))
    print("HomeCompetitor:", g.get("homeCompetitor"))
    print("AwayCompetitor:", g.get("awayCompetitor"))
    print("StartTime:", g.get("startTime"))

    print("Markets count:", len(markets))

    # print first 2 markets and first odds object inside each
    for mi, m in enumerate(markets[:2]):
        print(f"\n--- MARKET {mi+1} ---")
        print("Market keys:", m.keys())
        print("lineType:", m.get("lineType"))

        odds = m.get("odds", [])
        print("odds count:", len(odds))

        if odds:
            o = odds[0]
            print("First odds keys:", o.keys())
            print("bookmakerId:", o.get("bookmakerId"))
            print("First odds object preview:", o)

    found = True
    break

if not found:
    print("\nNo games with markets found in this file.")


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

# Inspect first 2 games deeply
for i, g in enumerate(games[:2]):
    print(f"\n=== GAME {i+1} ===")
    print("Keys:", g.keys())

    print("Competition:", g.get("competitionDisplayName"))
    print("Home:", g.get("homeCompetitor", {}).get("name"))
    print("Away:", g.get("awayCompetitor", {}).get("name"))

    markets = g.get("markets", [])
    print("Markets count:", len(markets))

    for m in markets[:3]:
        print("\n Market keys:", m.keys())
        print(" lineType:", m.get("lineType"))
        print(" odds type:", type(m.get("odds")))

        odds = m.get("odds", [])
        for o in odds[:1]:
            print("  Odds keys:", o.keys())
            print("  bookmakerId:", o.get("bookmakerId"))
            pprint(o)
    break

# bookmaker_stage1_parse_365_har.py

import json
from pathlib import Path
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
HAR_DIR = Path(".")  # current directory or change if needed
OUTPUT_DIR = Path("data/raw/bookmakers/365/epl")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_ENDPOINT_KEYWORD = "/web/games/allscores"
TARGET_SPORT_ID = 1  # Soccer
TARGET_COMPETITION_NAME = "Premier League"

# -----------------------------
# LOAD LATEST HAR
# -----------------------------
har_files = sorted(HAR_DIR.glob("*.har"), key=lambda p: p.stat().st_mtime)
if not har_files:
    raise FileNotFoundError("No .har files found in directory")

har_path = har_files[-1]
print(f">>> Parsing HAR file: {har_path.name}")

with open(har_path, "r", encoding="utf-8") as f:
    har = json.load(f)

entries = har.get("log", {}).get("entries", [])

# -----------------------------
# EXTRACT RELEVANT RESPONSES
# -----------------------------
raw_games = []
total_entries = 0
matched_entries = 0

for entry in entries:
    request = entry.get("request", {})
    response = entry.get("response", {})
    url = request.get("url", "")

    if TARGET_ENDPOINT_KEYWORD not in url:
        continue

    total_entries += 1

    content = response.get("content", {})
    text = content.get("text")

    if not text:
        continue

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        continue

    games = payload.get("games", [])
    if not games:
        continue

    for game in games:
        if game.get("sportId") != TARGET_SPORT_ID:
            continue

        comp_name = game.get("competitionDisplayName", "")
        if TARGET_COMPETITION_NAME not in comp_name:
            continue

        raw_games.append(game)
        matched_entries += 1

# -----------------------------
# WRITE OUTPUT
# -----------------------------
timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUTPUT_DIR / f"games_raw_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(raw_games, f, indent=2)

# -----------------------------
# SUMMARY
# -----------------------------
print("\nSTAGE B1 COMPLETE — 365 HAR PARSE")
print("--------------------------------")
print(f"HAR file: {har_path.name}")
print(f"Matching endpoint hits: {total_entries}")
print(f"EPL games extracted: {len(raw_games)}")
print(f"Wrote: {out_path.resolve()}")

import json
from pathlib import Path
from collections import Counter

# ==========================================================
# PATHS
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
NORM_DIR = REPO_ROOT / "data" / "normalized" / "epl"

# Pick latest normalized file automatically
files = sorted(NORM_DIR.glob("markets_flat_*.json"))
if not files:
    raise SystemExit("No normalized EPL market files found")

INPUT_FILE = files[-1]

# ==========================================================
# TAXONOMY RULES (v1)
# ==========================================================

RULES = [
    ("MATCH_WINNER", ["wins", "win the match"]),
    ("POINT_SPREAD", ["spread", "handicap", "+", "-"]),
    ("TOTAL_GOALS", ["total goals", "over", "under"]),
    ("BOTH_TEAMS_SCORE", ["both teams score"]),
    ("FIRST_EVENT", ["first", "scores first"]),
    ("PLAYER_PROP", ["player"]),
    ("TEAM_PROP", ["team", "scores", "clean sheet"]),
    ("BINARY_EVENT", ["will", "yes", "no"]),
    ("CONDITIONAL_EVENT", ["if", "given", "provided"]),
]

# ==========================================================
# LOAD DATA
# ==========================================================

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    markets = json.load(f)

results = []
counter = Counter()

# ==========================================================
# CLASSIFY
# ==========================================================

for m in markets:
    title = (m.get("market_title") or "").lower()
    taxonomy = "UNKNOWN"

    for tag, keywords in RULES:
        if any(k in title for k in keywords):
            taxonomy = tag
            break

    m["taxonomy"] = taxonomy
    results.append(m)
    counter[taxonomy] += 1

# ==========================================================
# OUTPUT SUMMARY
# ==========================================================

print(f"Input file: {INPUT_FILE.name}")
print(f"Markets analyzed: {len(markets)}\n")

print("MARKET TAXONOMY SUMMARY")
print("-" * 40)
for k, v in counter.items():
    print(f"{k:20} {v}")

# Optional: write annotated output (safe)
OUT_FILE = INPUT_FILE.with_name(INPUT_FILE.stem + "_taxonomy.json")
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2)

print("\nWrote taxonomy file:")
print(OUT_FILE)

import json
from pathlib import Path
from collections import defaultdict

# ==========================================================
# PATHS
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
NORM_DIR = REPO_ROOT / "data" / "normalized" / "epl"

files = sorted(NORM_DIR.glob("*_taxonomy.json"))
if not files:
    raise SystemExit("No taxonomy files found")

INPUT_FILE = files[-1]

# ==========================================================
# LOAD
# ==========================================================

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    markets = json.load(f)

unknown_titles = [
    m["market_title"]
    for m in markets
    if m.get("taxonomy") == "UNKNOWN" and m.get("market_title")
]

print(f"UNKNOWN markets found: {len(unknown_titles)}\n")

# ==========================================================
# SIMPLE LANGUAGE CLUSTERING
# ==========================================================

clusters = defaultdict(list)

KEYWORDS = [
    "first",
    "last",
    "either",
    "both",
    "score",
    "goal",
    "half",
    "minute",
    "extra time",
    "penalty",
]

for title in unknown_titles:
    lowered = title.lower()
    matched = False

    for kw in KEYWORDS:
        if kw in lowered:
            clusters[kw].append(title)
            matched = True

    if not matched:
        clusters["misc"].append(title)

# ==========================================================
# OUTPUT
# ==========================================================

print("UNKNOWN MARKET CLUSTERS")
print("-" * 50)

for k, v in clusters.items():
    print(f"\n[{k.upper()}] ({len(v)})")
    for t in v[:5]:
        print("  -", t)
    if len(v) > 5:
        print("  ...")

import json
import math
from pathlib import Path
from statistics import mean

# ==========================================================
# PATHS
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
NORM_DIR = REPO_ROOT / "data" / "normalized" / "epl"

files = sorted(NORM_DIR.glob("*_taxonomy.json"))
if not files:
    raise SystemExit("No taxonomy files found")

INPUT_FILE = files[-1]

OUT_DIR = REPO_ROOT / "data" / "computed" / "epl"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "total_goals_arb.json"

# ==========================================================
# LOAD
# ==========================================================

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    markets = json.load(f)

tg_markets = [
    m for m in markets
    if m.get("taxonomy") == "TOTAL_GOALS" and m.get("yes_price") is not None
]

if not tg_markets:
    print("No TOTAL_GOALS markets available")
    exit(0)

# ==========================================================
# COMPUTE BASELINE
# ==========================================================

probs = [
    m["yes_price"] / 100 if m["yes_price"] > 1 else m["yes_price"]
    for m in tg_markets
]

baseline = mean(probs)

# ==========================================================
# SCORE
# ==========================================================

results = []

for m in tg_markets:
    p = m["yes_price"] / 100 if m["yes_price"] > 1 else m["yes_price"]
    volume = m.get("volume", 0) or 0

    deviation = abs(p - baseline)
    liquidity_weight = math.log(1 + volume)

    arb_score = round(deviation * liquidity_weight, 5)

    results.append({
        "event": m["event_title"],
        "market": m["market_title"],
        "probability": round(p, 4),
        "volume": volume,
        "deviation": round(deviation, 4),
        "arb_score": arb_score,
    })

# ==========================================================
# OUTPUT
# ==========================================================

results.sort(key=lambda x: x["arb_score"], reverse=True)

with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2)

print(f"TOTAL_GOALS arb engine complete")
print(f"Markets scored: {len(results)}")
print(f"Wrote: {OUT_FILE}")

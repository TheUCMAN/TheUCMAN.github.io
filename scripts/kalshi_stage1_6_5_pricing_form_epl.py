import json
from pathlib import Path

# ==========================================================
# PATHS
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
NORM_DIR = REPO_ROOT / "data" / "normalized" / "epl"

files = sorted(NORM_DIR.glob("*_taxonomy.json"))
if not files:
    raise SystemExit("No taxonomy files found")

INPUT_FILE = files[-1]

print(f"Inspecting file: {INPUT_FILE.name}\n")

# ==========================================================
# LOAD
# ==========================================================

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    markets = json.load(f)

found = False

for m in markets:
    if m.get("taxonomy") == "TOTAL_GOALS":
        found = True
        print("EVENT:", m.get("event_title"))
        print("MARKET:", m.get("market_title"))

        # show price-related keys in normalized row
        price_keys = [k for k in m.keys() if "price" in k or "line" in k]
        print("NORMALIZED PRICE FIELDS:", price_keys)

        # show raw market structure
        raw = m.get("raw_market", {})
        print("RAW MARKET KEYS:", list(raw.keys()))

        # dig one level deeper if contracts exist
        if "contracts" in raw and raw["contracts"]:
            print("RAW CONTRACT SAMPLE KEYS:",
                  list(raw["contracts"][0].keys()))

        print("-" * 60)

if not found:
    print("No TOTAL_GOALS markets found in taxonomy file.")

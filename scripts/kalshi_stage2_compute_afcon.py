import json
import csv
from pathlib import Path

# ==========================================================
# PATHS
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]

RAW_LATEST = REPO_ROOT / "data" / "raw" / "kalshi" / "afcon" / "latest.json"
OUT_DIR = REPO_ROOT / "data" / "computed" / "afcon"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "latest.csv"
OUT_JSON = OUT_DIR / "latest.json"

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
        t = m.get("title", "").lower()
        price = m.get("yes_price")
        vol = m.get("volume", 0)

        if price is None:
            continue

        if price > 1:
            price = price / 100

        volume += vol

        if home.lower() in t:
            prices["home"] = price
        elif away.lower() in t:
            prices["away"] = price
        elif "draw" in t or "tie" in t:
            prices["tie"] = price

    return prices, volume

# ==========================================================
# LOAD DATA
# ==========================================================

if not RAW_LATEST.exists():
    raise SystemExit("Missing AFCON latest.json from Stage 1")

data = json.load(open(RAW_LATEST, "r", encoding="utf-8"))
events = data.get("events", [])

rows = []

# ==========================================================
# COMPUTE
# ==========================================================

for ev in events:
    match = ev.get("title", "")
    home, away = parse_teams(match)
    if not home or not away:
        continue

    prices, volume = extract_prices(ev, home, away)

    # AFCON: require all 3 prices (this is intentional)
    if not all(k in prices for k in ("home", "away", "tie")):
        continue

    vals = list(prices.values())
    pct_spread = max(vals) - min(vals)
    arb_score = round(pct_spread * (1 + volume / 10_000), 4)

    rows.append({
        "competition": "AFCON",
        "match": match,
        "home_team": home,
        "away_team": away,
        "home_pct": prices["home"],
        "away_pct": prices["away"],
        "tie_pct": prices["tie"],
        "volume": volume,
        "pct_spread": round(pct_spread, 4),
        "arb_score": arb_score,
    })

# ==========================================================
# RANK + OUTPUT
# ==========================================================

rows.sort(key=lambda r: r["arb_score"], reverse=True)

for i, r in enumerate(rows, 1):
    r["rank"] = i

if not rows:
    print("No valid AFCON rows produced")
    exit(0)

with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(rows, f, indent=2)

print("Wrote:")
print(" -", OUT_CSV)
print(" -", OUT_JSON)
print(f"Produced {len(rows)} AFCON rows")

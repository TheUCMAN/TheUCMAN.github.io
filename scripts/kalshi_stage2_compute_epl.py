import json
import csv
from pathlib import Path

# ==========================================================
# PATHS
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_LATEST = REPO_ROOT / "data" / "raw" / "kalshi" / "epl" / "latest.json"
OUT_DIR = REPO_ROOT / "data" / "computed" / "epl"
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
    raise SystemExit("Missing latest.json from Stage 1")

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

    # Require at least TWO prices (2-way market is valid)
    if len(prices) < 2:
        continue

    price_vals = list(prices.values())
    pct_spread = max(price_vals) - min(price_vals)

    arb_score = round(pct_spread * (1 + volume / 10_000), 4)

    rows.append({
        "competition": "EPL",
        "match": match,
        "home_team": home,
        "away_team": away,
        "home_pct": prices.get("home"),
        "away_pct": prices.get("away"),
        "tie_pct": prices.get("tie"),
        "volume": volume,
        "pct_spread": round(pct_spread, 4),
        "arb_score": arb_score,
    })

# ==========================================================
# RANK + OUTPUT
# ==========================================================

rows.sort(key=lambda r: r["arb_score"], reverse=True)

for i, r in enumerate(rows, 1):

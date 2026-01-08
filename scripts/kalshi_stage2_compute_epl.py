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
# NORMALIZATION HELPERS
# ==========================================================

def normalize(text: str) -> str:
    return (
        text.lower()
        .replace("fc", "")
        .replace("afc", "")
        .replace("united", "utd")
        .replace("manchester", "man")
        .replace("hotspur", "")
        .replace("city", "")
        .replace(".", "")
        .strip()
    )

def parse_teams(title: str):
    if " vs " not in title:
        return None, None
    home, away = title.split(" vs ", 1)
    return home.strip(), away.strip()

# ==========================================================
# PRICE EXTRACTION (KALSHI EPL LOGIC)
# ==========================================================

def extract_prices(event, home, away):
    prices = {}
    volume = 0

    home_n = normalize(home)
    away_n = normalize(away)

    for m in event.get("markets", []):
        title = normalize(m.get("title", ""))
        price = m.get("yes_price")
        vol = m.get("volume", 0)

        if price is None:
            continue

        if price > 1:
            price = price / 100

        volume += vol

        # Match win markets
        if "win" in title or "moneyline" in title:
            if home_n and home_n in title:
                prices["home"] = price
            elif away_n and away_n in title:
                prices["away"] = price

        # Optional draw
        if "draw" in title or "tie" in title:
            prices["tie"] = price

    return prices, volume

# ==========================================================
# LOAD DATA
# ==========================================================

if not RAW_LATEST.exists():
    raise SystemExit("Missing EPL latest.json from Stage 1")

with open(RAW_LATEST, "r", encoding="utf-8") as f:
    data = json.load(f)

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

    # Require at least two sides (2-way or 3-way)
    if len(prices) < 2:
        continue

    vals = list(prices.values())
    pct_spread = max(vals) - min(vals)

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
# OUTPUT
# ==========================================================

rows.sort(key=lambda r: r["arb_score"], reverse=True)

for i, r in enumerate(rows, 1):
    r["rank"] = i

if not rows:
    print("No valid EPL rows produced")
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
print(f"Produced {len(rows)} EPL rows")

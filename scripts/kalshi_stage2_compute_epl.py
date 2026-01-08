import json
import csv
from pathlib import Path
from datetime import datetime, timezone

# ==========================================================
# PATHS — REPO RELATIVE
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]

RAW_LATEST = REPO_ROOT / "data" / "raw" / "kalshi" / "epl" / "latest.json"
OUT_DIR = REPO_ROOT / "data" / "computed" / "epl"
OUT_DIR.mkdir(parents=True, exist_ok=True)

LATEST_CSV = OUT_DIR / "latest.csv"
LATEST_JSON = OUT_DIR / "latest.json"

# ==========================================================
# HELPERS
# ==========================================================

def parse_teams(title: str):
    if " vs " not in title:
        return None, None
    home, away = title.split(" vs ", 1)
    return home.strip(), away.strip()

def extract_probs(event, home_team, away_team):
    probs = {}
    volume = 0

    for m in event.get("markets", []):
        title = m.get("title", "").lower()
        price = m.get("yes_price")
        vol = m.get("volume", 0)

        if price is None:
            continue

        # normalize 0–100 → 0–1 if needed
        if price > 1:
            price = price / 100

        volume += vol

        if home_team.lower() in title:
            probs["home"] = price
        elif away_team.lower() in title:
            probs["away"] = price
        elif "draw" in title or "tie" in title:
            probs["tie"] = price

    return probs.get("home"), probs.get("away"), probs.get("tie"), volume


# ==========================================================
# LOAD RAW DATA
# ==========================================================

if not RAW_LATEST.exists():
    raise SystemExit("Missing latest.json from Stage 1")

with open(RAW_LATEST, "r", encoding="utf-8") as f:
    data = json.load(f)

events = data.get("events", [])
rows = []

# ==========================================================
# COMPUTE
# ==========================================================

for ev in events:
    title = ev.get("title", "")
    home, away = parse_teams(title)
    if not home or not away:
        continue

    home_p, away_p, tie_p, volume = extract_probs(ev, home, away)
    if None in (home_p, away_p, tie_p):
        continue

    pct_spread = max(home_p, away_p, tie_p) - min(home_p, away_p, tie_p)
    arb_score = round(pct_spread * (1 + volume / 10_000), 3)

    rows.append({
        "competition": "EPL",
        "match": title,
        "home_team": home,
        "away_team": away,
        "home_pct": home_p,
        "away_pct": away_p,
        "tie_pct": tie_p,
        "volume": volume,
        "pct_spread": pct_spread,
        "arb_score": arb_score,
    })

# ==========================================================
# RANK
# ==========================================================

rows.sort(key=lambda r: r["arb_score"], reverse=True)
for i, r in enumerate(rows, start=1):
    r["rank"] = i

# ==========================================================
# WRITE OUTPUTS
# ==========================================================

if rows:
    fields = list(rows[0].keys())

    with open(LATEST_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    with open(LATEST_JSON, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)

    print("Wrote:")
    print(" -", LATEST_CSV)
    print(" -", LATEST_JSON)
else:
    print("No valid EPL rows produced")

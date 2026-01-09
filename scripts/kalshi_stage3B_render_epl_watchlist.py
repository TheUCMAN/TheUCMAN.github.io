import json
from pathlib import Path
from datetime import datetime, timezone
import math

# ==========================================================
# PATHS
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
IN_DIR = REPO_ROOT / "data" / "computed" / "epl"
OUT_DIR = REPO_ROOT / "docs" / "epl"
OUT_DIR.mkdir(parents=True, exist_ok=True)

files = sorted(IN_DIR.glob("resolution_signal_*.json"))
if not files:
    raise SystemExit("No resolution signal files found")

INPUT_FILE = files[-1]
OUT_FILE = OUT_DIR / "epl_market_watchlist.html"

# ==========================================================
# LOAD
# ==========================================================

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# ==========================================================
# SIGNAL LANGUAGE + WATCHLIST LOGIC
# ==========================================================

def market_state(b):
    if b["conviction"] > 0.2 and b["volume"] > 1000:
        return "🟢 Conviction"
    if b["volume"] > 1000 and b["conviction"] < 0.05:
        return "🟡 Crowded & Uncertain"
    return "🔴 Thin / Ignore"

def watch_status(b):
    if b["volume"] > 1000 and b["conviction"] < 0.05:
        return "👀 Watch Closely"
    if b["volume"] < 500:
        return "⏳ Waiting"
    return "✅ Act"

def explanation(b):
    if b["volume"] > 1000 and b["conviction"] < 0.05:
        return "High attention, no clear belief yet"
    if b["conviction"] > 0.2:
        return "Market belief forming"
    return "Low signal strength"

# ==========================================================
# DAILY SUMMARY
# ==========================================================

if all(watch_status(r["best_signal"]) == "👀 Watch Closely" for r in data):
    daily_summary = (
        "Today’s EPL markets are crowded but undecided. "
        "These matches are worth watching — not acting on yet."
    )
else:
    daily_summary = "Some matches are transitioning from watch to action."

# ==========================================================
# BUILD TABLE
# ==========================================================

rows_html = ""

for r in data:
    b = r["best_signal"]
    rows_html += f"""
    <tr>
      <td>{watch_status(b)}</td>
      <td>{market_state(b)}</td>
      <td>{r['event']}</td>
      <td>{b['mid_yes_prob']}</td>
      <td>{b['volume']}</td>
      <td>{b['open_interest']}</td>
      <td>{b['conviction']}</td>
      <td>{explanation(b)}</td>
    </tr>
    """

html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>EPL – Market Watchlist & Conviction</title>
  <style>
    body {{ font-family: Arial, sans-serif; padding: 20px; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; }}
    th {{ background: #f4f4f4; }}
    tr:hover {{ background: #fafafa; }}
    .banner {{ background: #eef3ff; padding: 12px; margin-bottom: 15px; border-left: 4px solid #4a6cf7; }}
    .note {{ color: #555; margin-top: 15px; }}
  </style>
</head>
<body>

<h2>EPL – Market Watchlist & Conviction Scan</h2>

<div class="banner">
<b>Daily Read:</b> {daily_summary}
</div>

<p><b>How to read this:</b><br>
Markets move through phases. High volume without conviction often appears
before lineups, injuries, or news. Arbstack highlights when to <i>watch</i>
as carefully as when to act.
</p>

<table>
  <thead>
    <tr>
      <th>Watch Status</th>
      <th>Market State</th>
      <th>Match</th>
      <th>YES Mid Prob</th>
      <th>Volume</th>
      <th>Open Interest</th>
      <th>Conviction</th>
      <th>Interpretation</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
</table>

<p class="note">
Signals are derived from Kalshi YES/NO resolution markets.
This page emphasizes discipline, not constant action.
</p>

</body>
</html>
"""

with open(OUT_FILE, "w", encoding="utf-8") as f:
    f.write(html)

print("Stage 3B+ complete")
print(f"Wrote: {OUT_FILE}")

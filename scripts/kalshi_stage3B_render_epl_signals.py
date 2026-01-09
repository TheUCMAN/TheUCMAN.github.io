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
OUT_FILE = OUT_DIR / "epl_market_mood.html"

# ==========================================================
# LOAD
# ==========================================================

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# ==========================================================
# MARKET STATE LOGIC
# ==========================================================

def market_state(row):
    conviction = row["conviction"]
    volume = row["volume"]

    if conviction > 0.2 and volume > 1000:
        return "🟢 Conviction"
    if volume > 1000 and conviction < 0.05:
        return "🟡 Crowded & Uncertain"
    return "🔴 Thin / Ignore"

def explanation(row):
    if row["conviction"] < 0.05 and row["volume"] > 1000:
        return "High attention, no clear belief"
    if row["conviction"] > 0.2:
        return "Market leaning strongly"
    return "Low signal strength"

# ==========================================================
# DAILY SUMMARY
# ==========================================================

states = [market_state(r["best_signal"]) for r in data]

if all(s == "🟡 Crowded & Uncertain" for s in states):
    daily_summary = "Today’s EPL markets show high attention but low conviction. The market is watching, not committing."
else:
    daily_summary = "Some matches show meaningful conviction signals today."

# ==========================================================
# BUILD TABLE
# ==========================================================

rows_html = ""

for r in data:
    b = r["best_signal"]
    rows_html += f"""
    <tr>
      <td>{market_state(b)}</td>
      <td>{r['event']}</td>
      <td>{b['mid_yes_prob']}</td>
      <td>{b['volume']}</td>
      <td>{b['open_interest']}</td>
      <td>{b['conviction']}</td>
      <td>{b['signal_score']}</td>
      <td>{explanation(b)}</td>
    </tr>
    """

html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>EPL – Market Mood & Conviction Scan</title>
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

<h2>EPL – Market Mood & Conviction Scan</h2>

<div class="banner">
<b>Daily Read:</b> {daily_summary}
</div>

<p><b>How to read this:</b><br>
This page shows how confident the market is about match outcomes.
High volume without conviction means uncertainty — not opportunity.
Arbstack surfaces when <i>not</i> to act as clearly as when to act.
</p>

<table>
  <thead>
    <tr>
      <th>Market State</th>
      <th>Match</th>
      <th>YES Mid Prob</th>
      <th>Volume</th>
      <th>Open Interest</th>
      <th>Conviction</th>
      <th>Signal Score</th>
      <th>Interpretation</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
</table>

<p class="note">
Signals are derived from Kalshi resolution (YES/NO) markets. This is not betting advice.
</p>

</body>
</html>
"""

with open(OUT_FILE, "w", encoding="utf-8") as f:
    f.write(html)

print("Stage 3B complete")
print(f"Wrote: {OUT_FILE}")

import json
from pathlib import Path
from datetime import datetime

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
OUT_FILE = OUT_DIR / "epl_resolution_signals.html"

# ==========================================================
# LOAD DATA
# ==========================================================

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

# ==========================================================
# HELPERS
# ==========================================================

def interpret(row):
    s = row["signal_score"]
    vol = row["volume"]
    tight = row["tightness"]

    if s > 0.8 and vol > 1000:
        return "Strong conviction, well-supported"
    if s > 0.5:
        return "Moderate conviction"
    if tight < 0.4:
        return "Wide spread, low quality"
    return "Weak / noisy signal"

# ==========================================================
# BUILD HTML
# ==========================================================

rows_html = ""

for r in data:
    b = r["best_signal"]
    rows_html += f"""
    <tr>
      <td>{r['rank']}</td>
      <td>{r['event']}</td>
      <td>{b['mid_yes_prob']}</td>
      <td>{b['conviction']}</td>
      <td>{b['signal_score']}</td>
      <td>{b['volume']}</td>
      <td>{b['open_interest']}</td>
      <td>{b['tightness']}</td>
      <td>{interpret(b)}</td>
    </tr>
    """

html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>EPL – Kalshi Resolution Signals</title>
  <style>
    body {{ font-family: Arial, sans-serif; padding: 20px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; }}
    th {{ background: #f4f4f4; }}
    tr:hover {{ background: #fafafa; }}
    caption {{ margin-bottom: 10px; font-weight: bold; }}
  </style>
</head>
<body>

<h2>EPL – Kalshi Resolution Signals</h2>
<p>Generated at: {generated_at}</p>

<table>
  <thead>
    <tr>
      <th>Rank</th>
      <th>Match</th>
      <th>YES Mid Prob</th>
      <th>Conviction</th>
      <th>Signal Score</th>
      <th>Volume</th>
      <th>Open Interest</th>
      <th>Tightness</th>
      <th>Interpretation</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
</table>

<p style="margin-top:20px; color:#666;">
This page shows market conviction signals, not betting advice.
Signals reflect Kalshi resolution markets (YES/NO), not bookmaker odds.
</p>

</body>
</html>
"""

with open(OUT_FILE, "w", encoding="utf-8") as f:
    f.write(html)

print("Stage 3A complete")
print(f"Wrote: {OUT_FILE}")

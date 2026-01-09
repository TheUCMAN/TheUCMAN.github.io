"""
Stage 2C — EPL Time-Series Delta Engine (Schema-safe)
Compares the two most recent resolution_signal files
"""

import json
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = BASE_DIR / "data" / "computed" / "epl"
OUTPUT_DIR = BASE_DIR / "data" / "computed" / "epl"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print(">>> RUNNING STAGE 2C: TIME-SERIES DELTA ENGINE <<<")

# --------------------------------------------------
# Load last two resolution files
# --------------------------------------------------
files = sorted(INPUT_DIR.glob("resolution_signal_*.json"))
if len(files) < 2:
    raise RuntimeError("Need at least two resolution_signal files")

prev_file, curr_file = files[-2], files[-1]

with open(prev_file, "r", encoding="utf-8") as f:
    prev_raw = json.load(f)

with open(curr_file, "r", encoding="utf-8") as f:
    curr_raw = json.load(f)

print(f"Comparing:\n - {prev_file.name}\n - {curr_file.name}")

# --------------------------------------------------
# Normalize schema (list vs dict)
# --------------------------------------------------
def extract_matches(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "matches" in data:
        return data["matches"]
    raise ValueError("Unknown resolution_signal schema")

prev_matches = extract_matches(prev_raw)
curr_matches = extract_matches(curr_raw)

def get_match_key(m):
    for key in ("match", "event", "event_title", "title"):
        if key in m and m[key]:
            return m[key]
    raise KeyError(f"No match identifier found in record: {m.keys()}")

prev_map = {get_match_key(m): m for m in prev_matches}
curr_map = {get_match_key(m): m for m in curr_matches}

# --------------------------------------------------
# Delta computation
# --------------------------------------------------
deltas = []

for match, curr in curr_map.items():
    prev = prev_map.get(match)
    if not prev:
        continue

    delta_volume = curr["volume"] - prev["volume"]
    delta_oi = curr["open_interest"] - prev["open_interest"]
    delta_conviction = curr["conviction"] - prev["conviction"]

    velocity = delta_conviction / max(abs(delta_volume), 1)

    # Phase logic
    if curr["conviction"] > 0.1 and delta_conviction > 0:
        phase = "EDGE FORMING"
    elif delta_volume > 0 and delta_conviction == 0:
        phase = "CROWDED"
    elif delta_volume > 0:
        phase = "WARMING"
    else:
        phase = "WATCH"

    deltas.append({
        "match": match,
        "volume_prev": prev["volume"],
        "volume_curr": curr["volume"],
        "delta_volume": delta_volume,
        "open_interest_prev": prev["open_interest"],
        "open_interest_curr": curr["open_interest"],
        "delta_open_interest": delta_oi,
        "conviction_prev": prev["conviction"],
        "conviction_curr": curr["conviction"],
        "delta_conviction": delta_conviction,
        "velocity": round(velocity, 6),
        "phase": phase
    })

# --------------------------------------------------
# Write output
# --------------------------------------------------
timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
output_path = OUTPUT_DIR / f"epl_timeseries_delta_{timestamp}.json"

with open(output_path, "w", encoding="utf-8") as f:
    json.dump({
        "generated_at": timestamp,
        "previous_file": prev_file.name,
        "current_file": curr_file.name,
        "matches": deltas
    }, f, indent=2)

print("Stage 2C complete")
print(f"Wrote: {output_path}")
print(f"Matches analyzed: {len(deltas)}")

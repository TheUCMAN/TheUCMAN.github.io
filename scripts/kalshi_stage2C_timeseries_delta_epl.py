"""
Stage 2C — EPL Time-Series Delta Engine (Clean & Schema-Safe)
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
# Helpers
# --------------------------------------------------
def extract_matches(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "matches" in data:
        return data["matches"]
    raise ValueError("Unknown resolution schema")

def get_match_key(m):
    for k in ("match", "event", "event_title", "title"):
        if k in m and m[k]:
            return m[k]
    return "UNKNOWN_MATCH"

def get_number(m, keys):
    for k in keys:
        if k in m and isinstance(m[k], (int, float)):
            return m[k]
    return 0.0

# --------------------------------------------------
# Normalize
# --------------------------------------------------
prev_matches = extract_matches(prev_raw)
curr_matches = extract_matches(curr_raw)

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

    prev_volume = get_number(prev, ["volume", "total_volume", "recent_volume"])
    curr_volume = get_number(curr, ["volume", "total_volume", "recent_volume"])

    prev_oi = get_number(prev, ["open_interest", "oi"])
    curr_oi = get_number(curr, ["open_interest", "oi"])

    prev_conv = get_number(prev, ["conviction", "confidence", "signal_strength"])
    curr_conv = get_number(curr, ["conviction", "confidence", "signal_strength"])

    delta_volume = curr_volume - prev_volume
    delta_oi = curr_oi - prev_oi
    delta_conv = curr_conv - prev_conv

    velocity = delta_conv / max(abs(delta_volume), 1)

    if curr_conv > 0.1 and delta_conv > 0:
        phase = "EDGE FORMING"
    elif delta_volume > 0 and delta_conv == 0:
        phase = "CROWDED"
    elif delta_volume > 0:
        phase = "WARMING"
    else:
        phase = "WATCH"

    deltas.append({
        "match": match,
        "volume_prev": prev_volume,
        "volume_curr": curr_volume,
        "delta_volume": delta_volume,
        "open_interest_prev": prev_oi,
        "open_interest_curr": curr_oi,
        "delta_open_interest": delta_oi,
        "conviction_prev": prev_conv,
        "conviction_curr": curr_conv,
        "delta_conviction": delta_conv,
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

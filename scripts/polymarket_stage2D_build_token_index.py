import json
from pathlib import Path
from datetime import datetime, UTC

# -----------------------------
# CONFIG
# -----------------------------
EVENTS_DIR = Path("data/normalized/polymarket")
OUTPUT_DIR = Path("data/normalized/polymarket")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# LOAD LATEST GAMMA EVENTS FILE
# -----------------------------
event_files = sorted(EVENTS_DIR.glob("gamma_events_*.json"))
if not event_files:
    raise FileNotFoundError("No gamma_events_*.json found")

event_path = event_files[-1]
print(f">>> Building token index from: {event_path.name}")

with open(event_path, "r", encoding="utf-8") as f:
    events = json.load(f)

# -----------------------------
# BUILD TOKEN → MARKET INDEX
# -----------------------------
token_index = {}
markets_seen = 0
tokens_seen = 0

for market in events:
    condition_id = market.get("condition_id")
    question = market.get("question")
    market_slug = market.get("market_slug")

    tokens = market.get("tokens", [])
    if not condition_id or not tokens:
        continue

    markets_seen += 1

    for t in tokens:
        token_id = t.get("token_id")
        outcome = t.get("outcome")

        if not token_id:
            continue

        token_index[token_id] = {
            "condition_id": condition_id,
            "outcome": outcome,
            "question": question,
            "market_slug": market_slug
        }
        tokens_seen += 1

# -----------------------------
# WRITE OUTPUT
# -----------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUTPUT_DIR / f"polymarket_token_index_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(token_index, f, indent=2)

# -----------------------------
# SUMMARY
# -----------------------------
print("\nSTAGE P2D COMPLETE — TOKEN INDEX BUILT")
print("--------------------------------------")
print(f"Markets processed: {markets_seen}")
print(f"Tokens indexed:    {tokens_seen}")
print(f"Wrote: {out_path.resolve()}")

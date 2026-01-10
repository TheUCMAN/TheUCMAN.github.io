# scripts/polymarket_stage2A_extract_gamma_outcomes.py

import json
from pathlib import Path
from datetime import datetime, UTC

HAR_DIR = Path(".")
OUT_DIR = Path("data/normalized/polymarket")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_HOST = "gamma-api.polymarket.com"
TARGET_PATH = "/events/pagination"

har_files = sorted(HAR_DIR.glob("*.har"), key=lambda p: p.stat().st_mtime)
if not har_files:
    raise FileNotFoundError("No HAR files found")

har_path = har_files[-1]
print(f">>> Parsing Gamma outcomes from HAR: {har_path.name}")

har = json.load(open(har_path, "r", encoding="utf-8"))
entries = har["log"]["entries"]

outcomes_index = {}
matched_requests = 0

for entry in entries:
    url = entry["request"]["url"]
    if TARGET_HOST not in url or TARGET_PATH not in url:
        continue

    text = entry["response"]["content"].get("text")
    if not text:
        continue

    try:
        payload = json.loads(text)
    except Exception:
        continue

    matched_requests += 1

    for event in payload.get("data", []):
        event_id = event.get("id")
        event_title = event.get("title")

        for market in event.get("markets", []):
            market_id = market.get("id")
            market_question = market.get("question")

            for outcome in market.get("outcomes", []):

                # CASE 1: outcome is dict
                if isinstance(outcome, dict):
                    token = outcome.get("clobTokenId")
                    if not token:
                        continue

                    outcomes_index[token] = {
                        "event_id": event_id,
                        "event_title": event_title,
                        "market_id": market_id,
                        "market_question": market_question,
                        "outcome_id": outcome.get("id"),
                        "outcome_name": outcome.get("name"),
                        "side": outcome.get("side"),
                    }

                # CASE 2: outcome is already clobTokenId (string)
                elif isinstance(outcome, str):
                    outcomes_index[outcome] = {
                        "event_id": event_id,
                        "event_title": event_title,
                        "market_id": market_id,
                        "market_question": market_question,
                        "outcome_id": None,
                        "outcome_name": None,
                        "side": None,
                    }

ts = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUT_DIR / f"gamma_outcomes_{ts}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(outcomes_index, f, indent=2)

print("\nSTAGE P2A-3 COMPLETE — GAMMA OUTCOMES")
print("------------------------------------")
print(f"Gamma requests matched: {matched_requests}")
print(f"Outcome tokens indexed: {len(outcomes_index)}")
print(f"Wrote: {out_path.resolve()}")

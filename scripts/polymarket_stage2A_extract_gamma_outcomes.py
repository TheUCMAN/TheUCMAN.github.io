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

outcomes = {}
matched = 0

for e in entries:
    url = e["request"]["url"]
    if TARGET_HOST not in url or TARGET_PATH not in url:
        continue

    text = e["response"]["content"].get("text")
    if not text:
        continue

    try:
        payload = json.loads(text)
    except Exception:
        continue

    matched += 1

    for event in payload.get("data", []):
        event_id = event.get("id")
        event_title = event.get("title")

        for market in event.get("markets", []):
            market_id = market.get("id")
            market_question = market.get("question")

            for outcome in market.get("outcomes", []):
                token = outcome.get("clobTokenId")
                if not token:
                    continue

                outcomes[token] = {
                    "event_id": event_id,
                    "event_title": event_title,
                    "market_id": market_id,
                    "market_question": market_question,
                    "outcome_id": outcome.get("id"),
                    "outcome_name": outcome.get("name"),
                    "side": outcome.get("side"),
                }

ts = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUT_DIR / f"gamma_outcomes_{ts}.json"

json.dump(outcomes, open(out_path, "w", encoding="utf-8"), indent=2)

print("\nSTAGE P2A-3 COMPLETE — GAMMA OUTCOMES")
print("------------------------------------")
print(f"Gamma pages matched: {matched}")
print(f"Outcome tokens indexed: {len(outcomes)}")
print(f"Wrote: {out_path.resolve()}")

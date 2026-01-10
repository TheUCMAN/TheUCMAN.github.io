# scripts/polymarket_stage0W_parse_ws_messages.py
import json
from pathlib import Path
from collections import Counter, defaultdict

WS_DIR = Path("data/raw/polymarket/ws")

def latest_ws_file():
    files = sorted(WS_DIR.glob("*.jsonl"), key=lambda p: p.stat().st_mtime)
    if not files:
        raise FileNotFoundError("No WS jsonl files found in data/raw/polymarket/ws")
    return files[-1]

def main():
    ws_file = latest_ws_file()
    print(f">>> Parsing WS file: {ws_file.name}\n")

    type_counter = Counter()
    key_counter = Counter()
    sample_by_type = defaultdict(list)

    total = 0

    with ws_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1

            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                type_counter["__parse_error__"] += 1
                continue

            payload = msg.get("payload", {})
            msg_type = payload.get("type") or payload.get("event") or "unknown"

            type_counter[msg_type] += 1

            if isinstance(payload, dict):
                for k in payload.keys():
                    key_counter[k] += 1

            if len(sample_by_type[msg_type]) < 2:
                sample_by_type[msg_type].append(payload)

    print("SUMMARY")
    print("-------")
    print(f"Total WS messages: {total}\n")

    print("Message types:")
    for k, v in type_counter.most_common():
        print(f" - {k}: {v}")

    print("\nTop payload keys:")
    for k, v in key_counter.most_common(15):
        print(f" - {k}: {v}")

    print("\nSamples:")
    for t, samples in sample_by_type.items():
        print(f"\n=== TYPE: {t} ===")
        for s in samples:
            print(json.dumps(s, indent=2)[:800])

if __name__ == "__main__":
    main()

import json
from pathlib import Path
from datetime import datetime

WS_DIR = Path("data/raw/polymarket/ws")
OUT_DIR = Path("data/normalized/polymarket")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# pick latest ws file
ws_files = sorted(WS_DIR.glob("ws_asset_messages_*.jsonl"))
if not ws_files:
    raise FileNotFoundError("No WS asset files found")

ws_path = ws_files[-1]
print(f">>> Normalizing WS books: {ws_path.name}")

rows = []

with open(ws_path, "r", encoding="utf-8") as f:
    for line in f:
        try:
            msg = json.loads(line)
        except Exception:
            continue

        if msg.get("event_type") != "book":
            continue

        bids = msg.get("bids", [])
        asks = msg.get("asks", [])

        if not bids or not asks:
            continue

        best_bid = max(float(b["price"]) for b in bids)
        best_ask = min(float(a["price"]) for a in asks)

        bid_vol = sum(float(b["size"]) for b in bids)
        ask_vol = sum(float(a["size"]) for a in asks)

        mid = round((best_bid + best_ask) / 2, 4)
        spread = round(best_ask - best_bid, 4)

        rows.append({
            "timestamp": int(msg.get("timestamp")),
            "market": msg.get("market"),
            "asset_id": msg.get("asset_id"),
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": mid,
            "spread": spread,
            "bid_volume": round(bid_vol, 2),
            "ask_volume": round(ask_vol, 2),
            "imbalance": round((bid_vol - ask_vol) / (bid_vol + ask_vol), 4)
        })

ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUT_DIR / f"polymarket_ws_books_{ts}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(rows, f, indent=2)

print("\nSTAGE P0W-3 COMPLETE — WS BOOKS NORMALIZED")
print("------------------------------------------")
print(f"Rows written: {len(rows)}")
print(f"Wrote: {out_path.resolve()}")

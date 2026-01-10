import json
import time
from datetime import datetime, UTC
from pathlib import Path
import websocket

# -------------------------
# CONFIG
# -------------------------
ASSET_FILE = "data/raw/polymarket/books_raw_2026-01-10T01-49-41Z.json"
TOP_N_ASSETS = 5
RUN_SECONDS = 120

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

OUT_DIR = Path("data/raw/polymarket/ws")
OUT_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
OUT_PATH = OUT_DIR / f"ws_asset_messages_{timestamp}.jsonl"

# -------------------------
# LOAD TOP ASSETS
# -------------------------
with open(ASSET_FILE, "r", encoding="utf-8") as f:
    books = json.load(f)

from collections import Counter
asset_ids = [b["asset_id"] for b in books if b.get("asset_id")]
top_assets = [a for a, _ in Counter(asset_ids).most_common(TOP_N_ASSETS)]

print("Subscribing to asset_ids:")
for a in top_assets:
    print(" -", a)

# -------------------------
# WS HANDLERS
# -------------------------
def on_open(ws):
    print("WS CONNECTED")
    msg = {
        "type": "subscribe",
        "asset_ids": top_assets
    }
    ws.send(json.dumps(msg))
    print("Subscribed to assets")

def on_message(ws, message):
    with open(OUT_PATH, "a", encoding="utf-8") as f:
        f.write(message + "\n")

def on_error(ws, error):
    print("WS ERROR:", error)

def on_close(ws, *_):
    print("WS CLOSED")

# -------------------------
# RUN
# -------------------------
print(">>> STAGE P0W-2 — POLYMARKET ASSET WS CAPTURE")
print("Connecting to:", WS_URL)
print("Writing to:   ", OUT_PATH.resolve())
print("Run duration:", RUN_SECONDS, "seconds\n")

ws = websocket.WebSocketApp(
    WS_URL,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

import threading
t = threading.Thread(target=ws.run_forever)
t.start()

time.sleep(RUN_SECONDS)
ws.close()

print("\nSTAGE P0W-2 COMPLETE")
print("Saved to:", OUT_PATH.resolve())

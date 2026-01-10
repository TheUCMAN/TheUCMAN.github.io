# polymarket_stage0W_capture_ws_assets_v3.py

import json
import time
from pathlib import Path
from datetime import datetime, UTC
import websocket

# -----------------------------
# CONFIG
# -----------------------------
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

ASSET_IDS = [
    "11044322011069405496481552041346880611463175311601935567806748052760845735406"
]

RUN_SECONDS = 60
OUTPUT_DIR = Path("data/raw/polymarket/ws")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

out_file = OUTPUT_DIR / f"ws_asset_books_{datetime.now(UTC).strftime('%Y-%m-%dT%H-%M-%SZ')}.jsonl"

# -----------------------------
# WS CALLBACKS
# -----------------------------
def on_open(ws):
    print("WS CONNECTED")

    sub_msg = {
        "type": "subscribe",
        "channel": "book",
        "asset_ids": ASSET_IDS
    }

    ws.send(json.dumps(sub_msg))
    print("Subscribed with payload:")
    print(json.dumps(sub_msg, indent=2))


def on_message(ws, message):
    with open(out_file, "a", encoding="utf-8") as f:
        f.write(message + "\n")


def on_error(ws, error):
    print("WS ERROR:", error)


def on_close(ws, *_):
    print("WS CLOSED")


# -----------------------------
# RUN
# -----------------------------
print(">>> STAGE P0W-2(v3) — POLYMARKET BOOK WS CAPTURE")
print("Connecting to:", WS_URL)
print("Writing to:", out_file.resolve())
print("Run duration:", RUN_SECONDS, "seconds")

ws = websocket.WebSocketApp(
    WS_URL,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
)

import threading
t = threading.Thread(target=ws.run_forever)
t.daemon = True
t.start()

time.sleep(RUN_SECONDS)
ws.close()

print("\nSTAGE COMPLETE")
print("Saved to:", out_file.resolve())

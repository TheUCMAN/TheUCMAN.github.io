import json
import time
from pathlib import Path
from datetime import datetime, UTC

import websocket

# -----------------------------
# CONFIG
# -----------------------------
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RUN_SECONDS = 120

OUT_DIR = Path("data/raw/polymarket/ws")
OUT_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
OUT_FILE = OUT_DIR / f"ws_all_messages_{timestamp}.jsonl"

# -----------------------------
# WS CALLBACKS
# -----------------------------
def on_open(ws):
    print("WS CONNECTED")
    subscribe_msg = {
        "type": "subscribe",
        "channel": "market"
    }
    ws.send(json.dumps(subscribe_msg))
    print("Subscribed to ALL markets")

def on_message(ws, message):
    with open(OUT_FILE, "a", encoding="utf-8") as f:
        f.write(message + "\n")

def on_error(ws, error):
    print("WS ERROR:", error)

def on_close(ws, code, msg):
    print("WS CLOSED")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print(">>> STAGE P0W-A — POLYMARKET GLOBAL WS CAPTURE")
    print(f"Connecting to: {WS_URL}")
    print(f"Writing to:    {OUT_FILE.resolve()}")
    print(f"Run duration:  {RUN_SECONDS}s\n")

    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # run websocket in background
    import threading
    t = threading.Thread(target=ws.run_forever, daemon=True)
    t.start()

    try:
        time.sleep(RUN_SECONDS)
    except KeyboardInterrupt:
        print("Interrupted by user")

    ws.close()
    print("\nSTAGE P0W-A COMPLETE")
    print(f"Saved to: {OUT_FILE.resolve()}")

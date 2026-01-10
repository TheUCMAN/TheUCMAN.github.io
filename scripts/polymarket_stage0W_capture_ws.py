# polymarket_stage0W_capture_ws.py

import json
import time
import websocket
from pathlib import Path
from datetime import datetime, UTC

# -----------------------------
# CONFIG
# -----------------------------
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

OUTPUT_DIR = Path("data/raw/polymarket/ws")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RUN_SECONDS = 120  # capture duration (2 min default)

# -----------------------------
# OUTPUT FILE
# -----------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUTPUT_DIR / f"ws_messages_{timestamp}.jsonl"

print(">>> STAGE P0W — POLYMARKET CLOB WS CAPTURE")
print(f"Connecting to: {WS_URL}")
print(f"Writing to:    {out_path.resolve()}")
print(f"Run duration:  {RUN_SECONDS}s\n")

# -----------------------------
# WS HANDLERS
# -----------------------------
def on_open(ws):
    print("WS CONNECTED")

    # Subscribe message (Polymarket allows empty / default subscription)
    subscribe_msg = {
        "type": "subscribe",
        "channel": "market"
    }
    ws.send(json.dumps(subscribe_msg))
    print("Subscribed to market channel")


def on_message(ws, message):
    try:
        data = json.loads(message)
    except Exception:
        data = {"raw": message}

    record = {
        "ts": datetime.now(UTC).isoformat(),
        "payload": data
    }

    with open(out_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def on_error(ws, error):
    print("WS ERROR:", error)


def on_close(ws, *_):
    print("WS CLOSED")


# -----------------------------
# RUN WS CLIENT
# -----------------------------
ws = websocket.WebSocketApp(
    WS_URL,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
)

# Run WS in blocking mode with timeout
start = time.time()

def run():
    ws.run_forever()

import threading
t = threading.Thread(target=run)
t.start()

while time.time() - start < RUN_SECONDS:
    time.sleep(1)

ws.close()
t.join()

print("\nSTAGE P0W COMPLETE — WS CAPTURE FINISHED")
print(f"Saved to: {out_path.resolve()}")

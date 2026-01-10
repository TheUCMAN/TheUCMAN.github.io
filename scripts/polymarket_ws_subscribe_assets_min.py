# Minimal Polymarket WS subscriber (correct payload)
import json, time
import websocket
from datetime import datetime

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
ASSET_IDS = [
    "11044322011069405496481552041346880611463175311601935567806748052760845735406",
    "33963119312723998132193953391600393627750977397076277580336249660068651969709",
]
RUN_SECONDS = 120

out = f"data/raw/polymarket/ws/ws_books_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.jsonl"

def on_open(ws):
    payload = {
        "type": "subscribe",
        "channel": "book",
        "asset_ids": ASSET_IDS
    }
    ws.send(json.dumps(payload))
    print("SUBSCRIBED:", payload)

def on_message(ws, msg):
    with open(out, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def on_error(ws, err):
    print("WS ERROR:", err)

def on_close(ws):
    print("WS CLOSED")

ws = websocket.WebSocketApp(
    WS_URL,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

# run briefly, then exit
import threading
t = threading.Thread(target=ws.run_forever, daemon=True)
t.start()
time.sleep(600)
ws.close()
print("SAVED TO:", out)

print("WS listening… press Ctrl+C to stop")


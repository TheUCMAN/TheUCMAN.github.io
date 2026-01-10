import json
import time
from datetime import datetime, UTC
import websocket

ASSET_IDS = [
    "11044322011069405496481552041346880611463175311601935567806748052760845735406",
    "33963119312723998132193953391600393627750977397076277580336249660068651969709"
]

OUT = f"data/raw/polymarket/ws/ws_books_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.jsonl"

def on_open(ws):
    sub = {
        "type": "subscribe",
        "channel": "price_change",   # IMPORTANT
        "asset_ids": ASSET_IDS
    }
    ws.send(json.dumps(sub))
    print("SUBSCRIBED:", sub)

def on_message(ws, message):
    with open(OUT, "a", encoding="utf-8") as f:
        f.write(message + "\n")

def on_error(ws, error):
    print("WS ERROR:", error)

def on_close(ws, status, msg):
    print("WS CLOSED:", status, msg)

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    ws.run_forever(ping_interval=20, ping_timeout=10)

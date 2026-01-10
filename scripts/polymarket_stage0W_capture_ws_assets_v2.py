# scripts/polymarket_stage0W_capture_ws_assets_v2.py
import json
import time
import threading
from pathlib import Path
from datetime import datetime, UTC
from collections import Counter

import websocket  # pip install websocket-client

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

RUN_SECONDS = 30
PING_EVERY_SECONDS = 15
MAX_RECONNECTS = 1
TOP_N_ASSETS = 5

RAW_DIR = Path("data/raw/polymarket")
WS_OUT_DIR = Path("data/raw/polymarket/ws")


def latest_file(glob_pat: str, base: Path) -> Path | None:
    files = sorted(base.glob(glob_pat), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def load_top_asset_ids_from_latest_books(top_n: int) -> list[str]:
    f = latest_file("books_raw_*.json", RAW_DIR)
    if not f:
        raise FileNotFoundError("No books_raw_*.json found in data/raw/polymarket")

    data = json.loads(f.read_text(encoding="utf-8"))
    asset_ids = [str(b.get("asset_id")) for b in data if b.get("asset_id")]

    if not asset_ids:
        raise RuntimeError("No asset_id fields found in latest books_raw file")

    c = Counter(asset_ids)
    top = [aid for aid, _ in c.most_common(top_n)]
    return top


def utc_ts() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")


class WSRecorder:
    def __init__(self, asset_ids: list[str], out_path: Path):
        self.asset_ids = asset_ids
        self.out_path = out_path
        self.ws = None
        self._stop = threading.Event()
        self._has_written_any = threading.Event()
        self._lock = threading.Lock()
        self._reconnects = 0

    def log_line(self, obj: dict):
        line = json.dumps(obj, ensure_ascii=False)
        # Write immediately so even one message produces a visible file
        with self._lock:
            with self.out_path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        self._has_written_any.set()

    def on_open(self, ws):
        print("WS CONNECTED")

        sub_msg = {
            "type": "subscribe",
            "channel": "market",
            "asset_ids": self.asset_ids,
        }
        ws.send(json.dumps(sub_msg))
        print("Subscribed to assets")

        # Start heartbeat thread
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def on_message(self, ws, message: str):
        try:
            payload = json.loads(message)
        except Exception:
            payload = {"raw": message}

        self.log_line(
            {
                "ts": utc_ts(),
                "event": "message",
                "payload": payload,
            }
        )

    def on_error(self, ws, error):
        print(f"WS ERROR: {error}")
        self.log_line({"ts": utc_ts(), "event": "error", "error": str(error)})

    def on_close(self, ws, status_code, msg):
        print("WS CLOSED")
        self.log_line(
            {"ts": utc_ts(), "event": "close", "status_code": status_code, "msg": msg}
        )

        # Auto-reconnect once (only if not stopping)
        if not self._stop.is_set() and self._reconnects < MAX_RECONNECTS:
            self._reconnects += 1
            print(f"Reconnecting... ({self._reconnects}/{MAX_RECONNECTS})")
            time.sleep(2)
            self._connect()

    def _heartbeat_loop(self):
        # Polymarket can drop idle connections; ping helps.
        while not self._stop.is_set():
            time.sleep(PING_EVERY_SECONDS)
            try:
                if self.ws:
                    self.ws.send(json.dumps({"type": "ping", "ts": utc_ts()}))
                    self.log_line({"ts": utc_ts(), "event": "ping_sent"})
            except Exception as e:
                self.log_line({"ts": utc_ts(), "event": "ping_error", "error": str(e)})

    def _connect(self):
        self.ws = websocket.WebSocketApp(
            WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        # run_forever blocks; we run it in a thread
        threading.Thread(target=self.ws.run_forever, daemon=True).start()

    def run(self, run_seconds: int):
        self._connect()

        start = time.time()
        while time.time() - start < run_seconds:
            time.sleep(0.25)

        # Stop & close cleanly
        self._stop.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

        print("\nSTAGE P0W-2(v2) COMPLETE")
        print(f"Saved to: {self.out_path.resolve()}")
        if self.out_path.exists():
            print(f"File size: {self.out_path.stat().st_size} bytes")
        if not self._has_written_any.is_set():
            print("NOTE: No messages were received during the capture window.")


if __name__ == "__main__":
    WS_OUT_DIR.mkdir(parents=True, exist_ok=True)

    asset_ids = load_top_asset_ids_from_latest_books(TOP_N_ASSETS)

    print("Subscribing to TOP asset_ids:")
    for a in asset_ids:
        print(" -", a)

    out_path = WS_OUT_DIR / f"ws_asset_messages_{utc_ts()}.jsonl"

    print(f">>> STAGE P0W-2(v2) — POLYMARKET ASSET WS CAPTURE")
    print(f"Connecting to: {WS_URL}")
    print(f"Writing to:    {out_path.resolve()}")
    print(f"Run duration:  {RUN_SECONDS} seconds\n")

    rec = WSRecorder(asset_ids=asset_ids, out_path=out_path)
    rec.run(RUN_SECONDS)

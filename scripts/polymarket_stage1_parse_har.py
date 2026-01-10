# polymarket_stage1_parse_har.py

import json
from pathlib import Path
from datetime import datetime, UTC

# -----------------------------
# CONFIG
# -----------------------------
HAR_DIR = Path(".")  # repo root
OUTPUT_DIR = Path("data/raw/polymarket")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_HOST = "clob.polymarket.com"
TARGET_PATH_KEYWORD = "/books"

# -----------------------------
# LOAD LATEST HAR
# -----------------------------
har_files = sorted(HAR_DIR.glob("*.har"), key=lambda p: p.stat().st_mtime)
if not har_files:
    raise FileNotFoundError("No .har files found in repo root")

har_path = har_files[-1]
print(f">>> Parsing Polymarket HAR: {har_path.name}")

with open(har_path, "r", encoding="utf-8") as f:
    har = json.load(f)

entries = har.get("log", {}).get("entries", [])

# -----------------------------
# EXTRACT BOOK SNAPSHOTS
# -----------------------------
books = []
matched_requests = 0

for entry in entries:
    request = entry.get("request", {})
    response = entry.get("response", {})
    url = request.get("url", "")

    if TARGET_HOST not in url:
        continue
    if TARGET_PATH_KEYWORD not in url:
        continue

    content = response.get("content", {})
    text = content.get("text")
    if not text:
        continue

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        continue

    matched_requests += 1

    # payload may be a dict or list depending on endpoint
    if isinstance(payload, dict):
        books.append(payload)
    elif isinstance(payload, list):
        books.extend(payload)

# -----------------------------
# WRITE OUTPUT
# -----------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUTPUT_DIR / f"books_raw_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(books, f, indent=2)

# -----------------------------
# SUMMARY
# -----------------------------
print("\nSTAGE P1 COMPLETE — POLYMARKET HAR PARSE")
print("--------------------------------------")
print(f"HAR file: {har_path.name}")
print(f"Matching book requests: {matched_requests}")
print(f"Book snapshots extracted: {len(books)}")
print(f"Wrote: {out_path.resolve()}")

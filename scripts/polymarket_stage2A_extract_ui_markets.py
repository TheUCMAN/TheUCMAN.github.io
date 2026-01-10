import json
from pathlib import Path
from datetime import datetime, UTC
from collections import Counter

# -----------------------------
# CONFIG
# -----------------------------
HAR_DIR = Path(".")
OUTPUT_DIR = Path("data/normalized/polymarket")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

UI_PATH_KEY = "/_next/data/"
SPORT_KEYWORDS = ["soccer", "football", "premier", "epl"]

# -----------------------------
# LOAD LATEST HAR
# -----------------------------
har_files = sorted(HAR_DIR.glob("*.har"), key=lambda p: p.stat().st_mtime)
if not har_files:
    raise FileNotFoundError("No .har files found")

har_path = har_files[-1]
print(f">>> Parsing Polymarket UI HAR: {har_path.name}")

with open(har_path, "r", encoding="utf-8") as f:
    har = json.load(f)

entries = har.get("log", {}).get("entries", [])

# -----------------------------
# EXTRACT UI MARKETS
# -----------------------------
markets = []
paths_seen = Counter()

for entry in entries:
    request = entry.get("request", {})
    response = entry.get("response", {})
    url = request.get("url", "")

    if UI_PATH_KEY not in url:
        continue

    paths_seen[url.split(UI_PATH_KEY)[-1].split("?")[0]] += 1

    text = response.get("content", {}).get("text")
    if not text:
        continue

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        continue

    page = payload.get("pageProps", {})
    data = page.get("data")

    if not isinstance(data, list):
        continue

    for item in data:
        question = item.get("question") or item.get("title")
        slug = item.get("slug")
        category = item.get("category") or ""

        if not question or not slug:
            continue

        markets.append({
            "slug": slug,
            "question": question,
            "category": category,
            "raw": item
        })

# -----------------------------
# WRITE OUTPUT
# -----------------------------
timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
out_path = OUTPUT_DIR / f"polymarket_ui_markets_{timestamp}.json"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(markets, f, indent=2)

# -----------------------------
# SUMMARY
# -----------------------------
print("\nSTAGE P2A COMPLETE — UI MARKET EXTRACTION")
print("-----------------------------------------")
print(f"UI endpoints hit: {sum(paths_seen.values())}")
print(f"Unique UI paths: {len(paths_seen)}")
print(f"Markets extracted: {len(markets)}")
print(f"Wrote: {out_path.resolve()}")

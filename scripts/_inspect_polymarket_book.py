import json
from pathlib import Path

RAW_DIR = Path("data/raw/polymarket").resolve()
raw_files = sorted(RAW_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime)

if not raw_files:
    raise FileNotFoundError("No raw Polymarket files found")

raw_path = raw_files[-1]
print("Inspecting file:", raw_path.name)

data = json.load(open(raw_path, "r", encoding="utf-8"))

print("\nTOTAL BOOK OBJECTS:", len(data))

# find first non-empty book
for idx, book in enumerate(data):
    if not isinstance(book, dict):
        continue

    # unwrap common wrapper
    if "data" in book and isinstance(book["data"], dict):
        book = book["data"]

    print(f"\n=== SAMPLE BOOK (index {idx}) ===")
    print("TOP-LEVEL KEYS:")
    for k in book.keys():
        print(" -", k)

    # dig one level deeper
    for k, v in book.items():
        if isinstance(v, dict):
            print(f"\nNESTED DICT KEYS under '{k}':")
            for kk in v.keys():
                print("  -", kk)

    break

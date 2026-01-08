import json
from pathlib import Path

p = Path("data/raw/kalshi/epl/latest.json")

print("File exists:", p.exists())

data = json.load(open(p, "r", encoding="utf-8"))

print("Top-level keys:", data.keys())

events = data.get("events", [])
print("Number of events:", len(events))

if not events:
    raise SystemExit("No events found")

ev = events[0]

print("\nFIRST EVENT KEYS:")
print(ev.keys())

print("\nEVENT TITLE:")
print(ev.get("title"))

print("\nCONTRACTS FIELD TYPE:")
print(type(ev.get("contracts")))

print("\nCONTRACTS SAMPLE:")
for c in ev.get("contracts", [])[:5]:
    print(c)

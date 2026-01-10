import json
from pathlib import Path
from collections import Counter

HAR_FILES = sorted(Path(".").glob("polymarket*.har"))
if not HAR_FILES:
    raise FileNotFoundError("No HAR files found")

har_path = HAR_FILES[-1]
print("Inspecting HAR:", har_path.name)

har = json.load(open(har_path, "r", encoding="utf-8"))
entries = har.get("log", {}).get("entries", [])

hosts = Counter()
paths = Counter()

for e in entries:
    url = e.get("request", {}).get("url", "")
    if "polymarket" not in url:
        continue

    hosts[url.split("/")[2]] += 1
    path = "/" + "/".join(url.split("/")[3:]).split("?")[0]
    paths[path] += 1

print("\nTOP POLYMARKET HOSTS:")
for h, c in hosts.most_common(10):
    print(f"{h}: {c}")

print("\nTOP POLYMARKET PATHS:")
for p, c in paths.most_common(20):
    print(f"{p}: {c}")

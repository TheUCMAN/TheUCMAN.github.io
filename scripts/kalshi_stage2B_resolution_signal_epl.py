import json
import math
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

# ==========================================================
# PATHS
# ==========================================================
REPO_ROOT = Path(__file__).resolve().parents[1]
NORM_DIR = REPO_ROOT / "data" / "normalized" / "epl"

files = sorted(NORM_DIR.glob("*_taxonomy.json"))
if not files:
    raise SystemExit("No taxonomy files found. Run Stage 1.6 first.")

INPUT_FILE = files[-1]

OUT_DIR = REPO_ROOT / "data" / "computed" / "epl"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
OUT_FILE = OUT_DIR / f"resolution_signal_{ts}.json"

# ==========================================================
# HELPERS
# ==========================================================
def cents_to_prob(x):
    """Kalshi prices are often in cents (0-100) but sometimes already 0-1."""
    if x is None:
        return None
    try:
        x = float(x)
    except Exception:
        return None
    if x > 1:
        return x / 100.0
    return x

def safe_num(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def mid_from_bid_ask(bid, ask, last=None):
    bid_p = cents_to_prob(bid)
    ask_p = cents_to_prob(ask)
    last_p = cents_to_prob(last)

    if bid_p is not None and ask_p is not None and ask_p >= bid_p:
        return (bid_p + ask_p) / 2.0, (ask_p - bid_p)

    # Fallbacks (some markets may have only last_price)
    if last_p is not None:
        return last_p, None

    if bid_p is not None:
        return bid_p, None
    if ask_p is not None:
        return ask_p, None

    return None, None

def log1p(x):
    return math.log(1.0 + max(0.0, x))

# ==========================================================
# LOAD
# ==========================================================
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    markets = json.load(f)

# ==========================================================
# FILTER: resolution-style markets (Winner?)
# ==========================================================
# We learned from Stage 1.6.5: these have yes_price/no_price + yes_bid/yes_ask in raw_market,
# and titles look like "<Team A> vs <Team B> Winner?"
resolution = []

for m in markets:
    title = (m.get("market_title") or "").lower()
    raw = m.get("raw_market", {}) or {}

    # Identify by phrasing + presence of bid/ask fields
    if " winner?" in title and ("yes_bid" in raw or "yes_ask" in raw or "last_price" in raw):
        resolution.append(m)

if not resolution:
    print("No resolution-style 'Winner?' markets found.")
    exit(0)

# ==========================================================
# GROUP BY EVENT (match)
# ==========================================================
by_event = defaultdict(list)
for m in resolution:
    by_event[m.get("event_title")].append(m)

results = []

for event_title, ms in by_event.items():
    # Many duplicates exist; treat each as a separate venue/market instance.
    # We’ll compute best representative signal by picking the one with highest dollar_volume/volume.
    scored_markets = []
    for m in ms:
        raw = m.get("raw_market", {}) or {}

        yes_bid = raw.get("yes_bid") or raw.get("yes_bid_dollars")
        yes_ask = raw.get("yes_ask") or raw.get("yes_ask_dollars")
        last = raw.get("last_price") or raw.get("last_price_dollars")

        mid, spread = mid_from_bid_ask(yes_bid, yes_ask, last)

        volume = safe_num(raw.get("volume"), 0.0)
        open_interest = safe_num(raw.get("open_interest"), 0.0)
        liquidity = safe_num(raw.get("liquidity"), 0.0)
        dollar_volume = safe_num(raw.get("dollar_volume"), 0.0)

        # Conviction = distance from 0.5
        conviction = abs((mid if mid is not None else 0.5) - 0.5) if mid is not None else 0.0

        # Market quality: tighter spreads are better. If spread missing, don’t penalize too hard.
        # Convert spread into a "tightness" score.
        if spread is None:
            tightness = 0.5
        else:
            # spread near 0 => tightness near 1; spread near 0.20 => much worse
            tightness = max(0.0, 1.0 - (spread / 0.20))

        # Liquidity weight
        liq_weight = log1p(volume) + log1p(open_interest) + log1p(dollar_volume) + log1p(liquidity)

        # Final signal score (v1):
        # - higher conviction
        # - higher liquidity
        # - tighter spread
        signal_score = round(conviction * (1 + liq_weight) * (0.5 + tightness), 6)

        scored_markets.append({
            "event": event_title,
            "market_title": m.get("market_title"),
            "mid_yes_prob": None if mid is None else round(mid, 4),
            "yes_spread": None if spread is None else round(spread, 4),
            "volume": int(volume),
            "open_interest": int(open_interest),
            "liquidity": liquidity,
            "dollar_volume": dollar_volume,
            "conviction": round(conviction, 4),
            "tightness": round(tightness, 4),
            "signal_score": signal_score,
        })

    # Pick "best" market instance to represent the match
    scored_markets.sort(key=lambda x: (x["signal_score"], x["dollar_volume"], x["volume"]), reverse=True)
    best = scored_markets[0]

    # Keep top 3 instances for transparency
    top_instances = scored_markets[:3]

    results.append({
        "event": event_title,
        "best_signal": best,
        "top_instances": top_instances,
    })

# Rank matches by best signal
results.sort(key=lambda x: x["best_signal"]["signal_score"], reverse=True)

# Add rank
for i, r in enumerate(results, 1):
    r["rank"] = i

# Output
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2)

print("Stage 2B complete: Resolution Signal Engine")
print(f"Matches scored: {len(results)}")
print(f"Wrote: {OUT_FILE}")

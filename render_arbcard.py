import argparse
import json
import math
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from jinja2 import Environment, FileSystemLoader, select_autoescape

# Playwright: pip install playwright && playwright install chromium
from playwright.sync_api import sync_playwright


def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "arbcard"


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def pct_to_meter(delta_str: str) -> float:
    """
    Convert "+6.0%" style string to a 0..100 meter.
    Heuristic: 0% => 10, 10% => 80, 20% => 100 (clamped).
    """
    m = re.search(r"(-?\d+(\.\d+)?)", delta_str)
    if not m:
        return 40.0
    v = float(m.group(1))
    # If delta is a percent, treat 0..20 as the main band
    meter = 10 + (v / 10.0) * 70  # 0->10, 10->80
    return clamp(meter, 0, 100)


def label_from_score(score: float) -> Dict[str, str]:
    if score >= 80:
        return {"score_class": "good", "score_label": "High-Quality Edge"}
    if score >= 60:
        return {"score_class": "warn", "score_label": "Medium Edge"}
    return {"score_class": "bad", "score_label": "Speculative"}


def stars_from_score(score: float) -> str:
    # 0..100 -> 0..5 stars (half-star)
    raw = clamp(score / 20.0, 0, 5)
    half = round(raw * 2) / 2
    full = int(math.floor(half))
    has_half = (half - full) >= 0.5
    empty = 5 - full - (1 if has_half else 0)
    return "★" * full + ("☆" if has_half else "") + "·" * empty  # "·" reads cleaner than empty star


def default_row(row: Dict[str, Any], idx: int) -> Dict[str, Any]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    score = float(row.get("arb_score", 0))

    # meters
    edge_meter = pct_to_meter(str(row.get("delta", "0%")))
    slippage_meter = clamp(float(row.get("slippage_meter", 40)), 0, 100)
    liquidity_meter = clamp(float(row.get("liquidity_meter", 60)), 0, 100)
    ttr_meter = clamp(float(row.get("ttr_meter", 55)), 0, 100)

    # fallback IDs
    rid = row.get("id")
    if not rid:
        rid = f"{slugify(row.get('match','match'))}-{idx:03d}"

    base = {
        "id": rid,
        "as_of": row.get("as_of", now),
        "match": row.get("match", "Team A vs Team B"),
        "league": row.get("league", "League"),
        "kickoff": row.get("kickoff", "Kickoff TBD"),
        "category": row.get("category", "Sports"),
        "market": row.get("market", "Market"),
        "edge_type": row.get("edge_type", "Standard"),
        "source": row.get("source", "Arbstack"),
        "arb_score": int(round(score)),
        "stars": row.get("stars", stars_from_score(score)),
        "execution_risk": row.get("execution_risk", "Medium"),
        "volatility": row.get("volatility", "Medium"),
        "confidence": row.get("confidence", 3.5),
        "sizing_tier": row.get("sizing_tier", "Small"),
        "best_price": row.get("best_price", "—"),
        "fair_price": row.get("fair_price", "—"),
        "delta": row.get("delta", "0.0%"),
        "drift": row.get("drift", "→ Flat"),
        "reason": row.get("reason", "Signal summary / why this edge exists (v1)."),
        "slippage": row.get("slippage", "Medium"),
        "liquidity": row.get("liquidity", "Medium"),
        "ttr": row.get("ttr", "—"),
        "note": row.get("note", "Not financial advice. Check rules, liquidity, and fills."),
        "edge_meter": edge_meter,
        "slippage_meter": slippage_meter,
        "liquidity_meter": liquidity_meter,
        "ttr_meter": ttr_meter,
    }

    base.update(label_from_score(score))
    return base


def load_rows(json_path: Path) -> List[Dict[str, Any]]:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "rows" in data:
        return data["rows"]
    if isinstance(data, list):
        return data
    raise ValueError("Input JSON must be an array of rows, or an object with a 'rows' key.")


def render_cards(
    input_json: Path,
    template_path: Path,
    output_dir: Path,
    out_format: str,
    quality: int,
    parallel: int = 1,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template(template_path.name)

    rows = load_rows(input_json)

    # Build HTML for each row
    html_items: List[Dict[str, Any]] = []
    for i, row in enumerate(rows, start=1):
        ctx = default_row(row, i)
        html = template.render(**ctx)
        html_items.append({"ctx": ctx, "html": html})

    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(
            viewport={"width": 980, "height": 560},
            device_scale_factor=2,
        )
        page = context.new_page()

        for item in html_items:
            ctx = item["ctx"]
            html = item["html"]

            # Use set_content with a stable wait
            page.set_content(html, wait_until="networkidle")

            filename = f"{slugify(ctx['match'])}__{slugify(str(ctx['market']))}__{slugify(str(ctx['id']))}.{out_format}"
            out_path = output_dir / filename

            if out_format.lower() in ("jpg", "jpeg"):
                page.screenshot(
                    path=str(out_path),
                    type="jpeg",
                    quality=quality,
                    full_page=True,
                )
            else:
                page.screenshot(
                    path=str(out_path),
                    type="png",
                    full_page=True,
                )

            print(f"Wrote: {out_path}")

        context.close()
        browser.close()


def main():
    ap = argparse.ArgumentParser(description="Render Arbstack ArbCards from JSON → JPG/PNG using HTML/CSS template.")
    ap.add_argument("--input", required=True, help="Path to input JSON (array of rows or {'rows':[...]})")
    ap.add_argument("--template", default="templates/arbcard_v1.html", help="Path to HTML template")
    ap.add_argument("--outdir", default="output", help="Output directory for images")
    ap.add_argument("--format", default="jpg", choices=["jpg", "jpeg", "png"], help="Output image format")
    ap.add_argument("--quality", type=int, default=92, help="JPEG quality (1-100). Ignored for PNG.")
    args = ap.parse_args()

    render_cards(
        input_json=Path(args.input),
        template_path=Path(args.template),
        output_dir=Path(args.outdir),
        out_format=args.format,
        quality=int(clamp(args.quality, 1, 100)),
    )


if __name__ == "__main__":
    main()

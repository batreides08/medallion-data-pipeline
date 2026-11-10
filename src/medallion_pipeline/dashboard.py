"""Static HTML dashboard over the Gold marts.

Renders revenue, customer value and product performance as one self-contained page (inline CSS and
SVG, no JavaScript or external assets), so it can be opened from disk or published as-is. The
rendering functions take plain Python rows and do not need Spark, which keeps them easy to test.
"""

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from html import escape
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from medallion_pipeline.config import LakePaths
from medallion_pipeline.tables import read_table

Row = Mapping[str, Any]
SEGMENT_ORDER = ("vip", "regular", "occasional", "inactive")
TOP_PRODUCTS = 10
TOP_COUNTRIES = 8

_CSS = """
:root{--bg:#fff;--fg:#1d2330;--muted:#5d6678;--card:#f5f7fa;--line:#d9dee7;--accent:#2f6fdd}
@media(prefers-color-scheme:dark){:root{--bg:#14171d;--fg:#e8ebf1;--muted:#9aa3b5;
--card:#1d222b;--line:#2f3644;--accent:#6ea2ff}}
body{margin:0;padding:24px 16px;background:var(--bg);color:var(--fg);
font:15px/1.5 system-ui,sans-serif}
main{max-width:960px;margin:0 auto}
h1{font-size:1.6rem;margin:0 0 4px}h2{font-size:1.1rem;margin:32px 0 8px}
.sub{color:var(--muted);margin:0 0 20px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px 14px}
.kpi b{display:block;font-size:1.4rem}.kpi span{color:var(--muted);font-size:.85rem}
svg{width:100%;height:auto;display:block}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line)}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
.bar{fill:var(--accent)}.axis{stroke:var(--line)}.lbl{fill:var(--muted);font-size:11px}
"""


def _num(value: Any) -> Decimal:
    return Decimal(0) if value is None else Decimal(str(value))


def _money(value: Decimal) -> str:
    return f"${value:,.2f}"


def _revenue_chart(daily: Sequence[Row]) -> str:
    """Line chart of gross revenue per day."""
    if not daily:
        return "<p class='sub'>No revenue yet.</p>"
    width, height, pad_l, pad_b, pad_t = 900, 260, 60, 28, 10
    values = [float(_num(r["gross_revenue"])) for r in daily]
    top = max(values) or 1.0
    span = max(len(values) - 1, 1)

    def x(i: int) -> float:
        return pad_l + (width - pad_l - 10) * i / span

    def y(v: float) -> float:
        return pad_t + (height - pad_b - pad_t) * (1 - v / top)

    points = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(values))
    first, last = str(daily[0]["order_date"]), str(daily[-1]["order_date"])
    return (
        f"<svg viewBox='0 0 {width} {height}' role='img' "
        f"aria-label='Daily gross revenue from {escape(first)} to {escape(last)}'>"
        f"<line class='axis' x1='{pad_l}' y1='{y(0):.1f}' x2='{width - 10}' y2='{y(0):.1f}'/>"
        f"<polyline points='{points}' fill='none' stroke='var(--accent)' stroke-width='2'/>"
        f"<text class='lbl' x='{pad_l - 6}' y='{y(top) + 4:.1f}' text-anchor='end'>"
        f"{top:,.0f}</text><text class='lbl' x='{pad_l - 6}' y='{y(0) + 4:.1f}' "
        f"text-anchor='end'>0</text>"
        f"<text class='lbl' x='{pad_l}' y='{height - 8}'>{escape(first)}</text>"
        f"<text class='lbl' x='{width - 10}' y='{height - 8}' text-anchor='end'>"
        f"{escape(last)}</text></svg>"
    )


def _bar_table(rows: Sequence[tuple[str, Decimal, str]]) -> str:
    """Horizontal bars as a table: label, bar scaled to the largest value, value text."""
    if not rows:
        return "<p class='sub'>No data.</p>"
    top = max(v for _, v, _ in rows) or Decimal(1)
    body = "".join(
        f"<tr><td>{escape(label)}</td><td style='width:55%'>"
        f"<svg viewBox='0 0 100 8' preserveAspectRatio='none' style='height:14px'>"
        f"<rect class='bar' width='{float(v / top) * 100:.1f}' height='8'/></svg></td>"
        f"<td class='n'>{escape(text)}</td></tr>"
        for label, v, text in rows
    )
    return f"<table><tbody>{body}</tbody></table>"


def render_dashboard(
    daily_revenue: Sequence[Row],
    customer_value: Sequence[Row],
    product_performance: Sequence[Row],
    batch_label: str = "",
) -> str:
    """Return the dashboard page for the three Gold marts."""
    revenue = sum((_num(r["gross_revenue"]) for r in daily_revenue), Decimal(0))
    orders = sum(int(r["orders"]) for r in daily_revenue)
    units = sum(int(r["units_sold"]) for r in daily_revenue)
    aov = revenue / orders if orders else Decimal(0)
    kpis = [
        ("Gross revenue", _money(revenue)),
        ("Orders", f"{orders:,}"),
        ("Avg order value", _money(aov)),
        ("Units sold", f"{units:,}"),
    ]

    segments = {s: [0, Decimal(0)] for s in SEGMENT_ORDER}
    by_country: dict[str, Decimal] = {}
    for r in customer_value:
        seg = segments.setdefault(str(r["value_segment"]), [0, Decimal(0)])
        seg[0] += 1
        seg[1] += _num(r["lifetime_revenue"])
        country = str(r["country"] or "unknown")
        by_country[country] = by_country.get(country, Decimal(0)) + _num(r["lifetime_revenue"])
    segment_rows = [
        (name, seg[1], f"{seg[0]:,} customers · {_money(seg[1])}")
        for name, seg in segments.items()
        if seg[0]
    ]
    countries = sorted(by_country.items(), key=lambda kv: kv[1], reverse=True)[:TOP_COUNTRIES]
    country_rows = [(c, v, _money(v)) for c, v in countries]

    top_products = sorted(product_performance, key=lambda r: _num(r["revenue"]), reverse=True)[
        :TOP_PRODUCTS
    ]
    product_rows = [
        (f"{r['name']} ({r['category']})", _num(r["revenue"]), _money(_num(r["revenue"])))
        for r in top_products
    ]
    categories: dict[str, Decimal] = {}
    for r in product_performance:
        categories[str(r["category"])] = categories.get(str(r["category"]), Decimal(0)) + _num(
            r["revenue"]
        )
    category_rows = [
        (c, v, _money(v)) for c, v in sorted(categories.items(), key=lambda kv: -kv[1])
    ]

    subtitle = f"Synthetic data · generated {date.today().isoformat()}"
    if batch_label:
        subtitle += f" · batch {batch_label}"
    kpi_html = "".join(
        f"<div class='kpi'><b>{escape(v)}</b><span>{escape(k)}</span></div>" for k, v in kpis
    )
    return (
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        f"<title>Medallion retail dashboard</title><style>{_CSS}</style></head><body><main>"
        f"<h1>Retail performance</h1><p class='sub'>{escape(subtitle)}</p>"
        f"<div class='kpis'>{kpi_html}</div>"
        f"<h2>Daily gross revenue</h2>{_revenue_chart(daily_revenue)}"
        f"<h2>Customer value segments</h2>{_bar_table(segment_rows)}"
        f"<h2>Lifetime revenue by country (top {TOP_COUNTRIES})</h2>{_bar_table(country_rows)}"
        f"<h2>Revenue by category</h2>{_bar_table(category_rows)}"
        f"<h2>Top {TOP_PRODUCTS} products</h2>{_bar_table(product_rows)}"
        "</main></body></html>"
    )


def build_dashboard(spark: SparkSession, paths: LakePaths, output: Path | None = None) -> Path:
    """Read the Gold marts from the lake and write the dashboard page."""
    marts = {
        name: [r.asDict() for r in read_table(spark, paths.gold / name).collect()]
        for name in ("daily_revenue", "customer_lifetime_value", "product_performance")
    }
    page = render_dashboard(
        marts["daily_revenue"], marts["customer_lifetime_value"], marts["product_performance"]
    )
    output = output or paths.root / "dashboard" / "index.html"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(page, encoding="utf-8")
    return output

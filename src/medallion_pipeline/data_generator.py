"""Deterministic synthetic retail data with a known number of injected defects.

All data is fabricated from a seeded RNG, so no real people or companies are involved. Because
the generator decides exactly which records are defective, it can also state how many rows each
layer must end up with, which gives the tests an independent source of truth.
"""

import csv
import json
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

CATEGORIES = ("books", "electronics", "garden", "home", "sports", "toys")
COUNTRIES = ("AR", "BR", "CL", "CO", "CR", "MX", "PE", "US")
FIRST_NAMES = ("Alex", "Camila", "Diego", "Elena", "Mateo", "Noa", "Sofia", "Tomas")
LAST_NAMES = ("Castro", "Mora", "Rojas", "Soto", "Vargas", "Vega")
VALID_STATUSES = ("placed", "shipped", "delivered", "cancelled", "returned")
STATUS_WEIGHTS = (10, 20, 55, 10, 5)

CUSTOMER_FIELDS = ("customer_id", "full_name", "email", "country", "updated_at")
PRODUCT_FIELDS = ("product_id", "name", "category", "list_price", "updated_at")


@dataclass(frozen=True)
class GenerationConfig:
    """Volumes, seed and number of defects to inject per kind."""

    seed: int = 42
    n_customers: int = 200
    n_products: int = 50
    n_orders: int = 2000
    start: datetime = datetime(2025, 1, 1, tzinfo=UTC)
    days: int = 365
    stale_customer_versions: int = 10
    invalid_email_customers: int = 5
    invalid_price_products: int = 3
    malformed_orders: int = 5
    missing_customer_orders: int = 10
    orphan_customer_orders: int = 10
    invalid_status_orders: int = 5
    invalid_timestamp_orders: int = 5
    duplicated_orders: int = 20
    invalid_quantity_orders: int = 15


@dataclass(frozen=True)
class ExpectedCounts:
    """Rows each Silver and quarantine table must hold after a correct run."""

    silver_customers: int
    silver_products: int
    silver_orders: int
    silver_order_items: int
    rejected_customers: int
    rejected_products: int
    rejected_orders: int
    rejected_order_items: int


def _timestamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_customers(cfg: GenerationConfig, rng: random.Random) -> list[dict[str, str]]:
    updated_at = cfg.start - timedelta(days=30)
    rows = []
    for number in range(1, cfg.n_customers + 1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        rows.append(
            {
                "customer_id": f"C{number:05d}",
                "full_name": f"{first} {last}",
                # Sloppy casing and padding that Silver has to normalise.
                "email": f"  {first}.{last}{number}@Example.com ",
                "country": rng.choice(COUNTRIES).lower(),
                "updated_at": _timestamp(updated_at),
            }
        )

    # Older versions of existing customers: Silver must keep the newest row.
    stale_versions = [
        {
            **row,
            "email": f"old.{row['customer_id'].lower()}@example.com",
            "updated_at": _timestamp(updated_at - timedelta(days=90)),
        }
        for row in rng.sample(rows, cfg.stale_customer_versions)
    ]

    # Extra customers that no order references, so rejecting them affects nothing downstream.
    invalid = [
        {
            "customer_id": f"C{cfg.n_customers + offset:05d}",
            "full_name": "Invalid Email",
            "email": "not-an-email",
            "country": "cr",
            "updated_at": _timestamp(updated_at),
        }
        for offset in range(1, cfg.invalid_email_customers + 1)
    ]
    return rows + stale_versions + invalid


def _build_products(cfg: GenerationConfig, rng: random.Random) -> list[dict[str, str]]:
    updated_at = _timestamp(cfg.start - timedelta(days=30))
    rows = [
        {
            "product_id": f"P{number:04d}",
            "name": f"Product {number}",
            "category": rng.choice(CATEGORIES),
            "list_price": str(Decimal(rng.randint(500, 25000)) / 100),
            "updated_at": updated_at,
        }
        for number in range(1, cfg.n_products + 1)
    ]
    invalid = [
        {
            "product_id": f"P{cfg.n_products + offset:04d}",
            "name": "Invalid Price",
            "category": "home",
            "list_price": "-1.00",
            "updated_at": updated_at,
        }
        for offset in range(1, cfg.invalid_price_products + 1)
    ]
    return rows + invalid


def _build_order(
    number: int, cfg: GenerationConfig, rng: random.Random, products: list[dict[str, str]]
) -> dict:
    moment = cfg.start + timedelta(seconds=rng.randrange(cfg.days * 86400))
    chosen = rng.sample(products[: cfg.n_products], rng.randint(1, 4))
    return {
        "order_id": f"O{number:07d}",
        "customer_id": f"C{rng.randint(1, cfg.n_customers):05d}",
        "status": rng.choices(VALID_STATUSES, weights=STATUS_WEIGHTS)[0],
        "order_ts": _timestamp(moment),
        "items": [
            {
                "product_id": product["product_id"],
                "quantity": rng.randint(1, 5),
                "unit_price": product["list_price"],
            }
            for product in chosen
        ],
    }


def _build_order_lines(
    cfg: GenerationConfig, rng: random.Random, products
) -> tuple[list, int, int]:
    """Return raw JSONL lines plus the expected valid order and order item counts."""
    orders = [_build_order(n, cfg, rng, products) for n in range(1, cfg.n_orders + 1)]

    # Each defect lands on its own order, which keeps the expected counts exact.
    kinds = (
        ("malformed", cfg.malformed_orders),
        ("missing_customer", cfg.missing_customer_orders),
        ("orphan_customer", cfg.orphan_customer_orders),
        ("invalid_status", cfg.invalid_status_orders),
        ("invalid_timestamp", cfg.invalid_timestamp_orders),
        ("duplicated", cfg.duplicated_orders),
        ("invalid_quantity", cfg.invalid_quantity_orders),
    )
    shuffled = rng.sample(range(cfg.n_orders), sum(count for _, count in kinds))
    defects: dict[int, str] = {}
    for kind, count in kinds:
        for _ in range(count):
            defects[shuffled.pop()] = kind

    lines = []
    valid_orders = 0
    valid_items = 0
    for index, order in enumerate(orders):
        defect = defects.get(index)
        if defect == "malformed":
            lines.append(json.dumps(order)[:40])
            continue
        if defect == "missing_customer":
            order["customer_id"] = None
        elif defect == "orphan_customer":
            order["customer_id"] = "C99999"
        elif defect == "invalid_status":
            order["status"] = "lost_in_space"
        elif defect == "invalid_timestamp":
            order["order_ts"] = "not-a-date"
        else:
            valid_orders += 1
            valid_items += len(order["items"])
            if defect == "invalid_quantity":
                order["items"][0]["quantity"] = rng.choice((0, -1))
                valid_items -= 1
        lines.append(json.dumps(order))
        if defect == "duplicated":
            lines.append(json.dumps(order))

    return rng.sample(lines, len(lines)), valid_orders, valid_items


def _write_csv(path: Path, fields: tuple[str, ...], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def generate_raw_data(raw_dir: Path, cfg: GenerationConfig | None = None) -> ExpectedCounts:
    """Write customers.csv, products.csv and orders.jsonl and return the expected counts."""
    cfg = cfg or GenerationConfig()
    rng = random.Random(cfg.seed)
    raw_dir.mkdir(parents=True, exist_ok=True)

    customers = _build_customers(cfg, rng)
    products = _build_products(cfg, rng)
    lines, valid_orders, valid_items = _build_order_lines(cfg, rng, products)

    _write_csv(raw_dir / "customers.csv", CUSTOMER_FIELDS, rng.sample(customers, len(customers)))
    _write_csv(raw_dir / "products.csv", PRODUCT_FIELDS, rng.sample(products, len(products)))
    (raw_dir / "orders.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")

    return ExpectedCounts(
        silver_customers=cfg.n_customers,
        silver_products=cfg.n_products,
        silver_orders=valid_orders,
        silver_order_items=valid_items,
        rejected_customers=cfg.invalid_email_customers,
        rejected_products=cfg.invalid_price_products,
        rejected_orders=cfg.n_orders - valid_orders,
        rejected_order_items=cfg.invalid_quantity_orders,
    )

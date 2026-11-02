"""Lake layout and business constants shared across layers."""

from dataclasses import dataclass
from pathlib import Path

ORDER_STATUSES = ("placed", "shipped", "delivered", "cancelled", "returned")
REVENUE_STATUSES = ("placed", "shipped", "delivered")

# Share of an entity's rows that may land in quarantine before the run is failed. The seeded
# generator rejects at most about 6% of any table, so 10% leaves room for normal defects while
# still stopping a delivery that is broken across the board.
DEFAULT_MAX_REJECTION_RATE = 0.10
ENTITIES = ("customers", "products", "orders", "order_items")


@dataclass(frozen=True)
class LakePaths:
    """Locations of every layer below a single root directory."""

    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def bronze(self) -> Path:
        return self.root / "bronze"

    @property
    def silver(self) -> Path:
        return self.root / "silver"

    @property
    def quarantine(self) -> Path:
        return self.root / "quarantine"

    @property
    def gold(self) -> Path:
        return self.root / "gold"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

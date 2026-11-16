"""Column contracts for raw CSV deliveries, checked before anything is ingested.

Spark maps CSV columns to a supplied schema by position and only logs a warning when the header
disagrees, so a delivery with reordered columns would silently put emails in ``full_name`` or
categories in ``name``. Bronze therefore reads CSV columns by header name, and this module rejects
a file whose header cannot be mapped: a required column is missing or appears more than once.
Extra columns are reported but do not fail the run, because ignoring them loses nothing that the
pipeline uses.
"""

import csv
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class HeaderCheck:
    """How the header of one CSV file compares with the columns the pipeline expects."""

    file: str
    missing: tuple[str, ...] = ()
    duplicated: tuple[str, ...] = ()
    extra: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return not self.missing and not self.duplicated

    def describe(self) -> str:
        parts = [
            f"{label} {', '.join(columns)}"
            for label, columns in (
                ("missing", self.missing),
                ("duplicated", self.duplicated),
                ("extra", self.extra),
            )
            if columns
        ]
        return f"{self.file}: {'; '.join(parts) or 'ok'}"


class RawContractError(RuntimeError):
    """Raised when a raw file breaks its contract. Retrying cannot fix the file."""

    def __init__(self, failures: list[HeaderCheck]) -> None:
        details = "; ".join(failure.describe() for failure in failures)
        super().__init__(f"raw delivery breaks its column contract: {details}")
        self.failures = failures


def read_csv_header(path: Path) -> list[str]:
    """Return the column names in the first line of ``path``; an empty file has none."""
    with path.open(newline="", encoding="utf-8") as handle:
        return next(csv.reader(handle), [])


def check_csv_header(path: Path, expected: Sequence[str]) -> HeaderCheck:
    """Compare the header of ``path`` with ``expected``. Order does not matter; names do.

    Names are matched exactly, including case and surrounding spaces, so a column that only looks
    right is reported instead of being mapped by guesswork.
    """
    header = read_csv_header(path)
    counts = Counter(header)
    # Spark resolves column names case-insensitively, so "Email" next to "email" is ambiguous.
    folded = Counter(name.casefold() for name in header)
    return HeaderCheck(
        file=path.name,
        missing=tuple(name for name in expected if name not in counts),
        duplicated=tuple(name for name in expected if folded[name.casefold()] > 1),
        extra=tuple(name for name in counts if name not in expected),
    )


def validate_csv_headers(files: dict[Path, Sequence[str]]) -> list[HeaderCheck]:
    """Check every file and raise :class:`RawContractError` listing all broken ones at once."""
    checks = [check_csv_header(path, expected) for path, expected in files.items()]
    failures = [check for check in checks if not check.passed]
    if failures:
        raise RawContractError(failures)
    return checks

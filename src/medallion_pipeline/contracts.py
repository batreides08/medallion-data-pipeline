"""Column contracts for raw deliveries, checked before anything is ingested.

Spark maps CSV columns to a supplied schema by position and only logs a warning when the header
disagrees, so a delivery with reordered columns would silently put emails in ``full_name`` or
categories in ``name``. Bronze therefore reads CSV columns by header name, and this module rejects
a file whose header cannot be mapped: a required column is missing or appears more than once.
Extra columns are reported but do not fail the run, because ignoring them loses nothing that the
pipeline uses.

The JSONL orders feed has the same weakness in another form: Spark reads it with a fixed schema and
fills any field it cannot find with NULL, so a renamed key (``order_ts`` delivered as ``orderTs``)
would null that field in every row and send the whole batch to quarantine with a misleading reason.
Its contract therefore checks which keys the delivery uses, at the top level and inside ``items``.
A key absent from one record is a row defect that Silver quarantines; a key absent from *every*
record means the feed changed shape, and the run stops.
"""

import csv
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
    return raise_on_failures([check_csv_header(path, expected) for path, expected in files.items()])


def check_jsonl_keys(
    path: Path, expected: Sequence[str], nested: Mapping[str, Sequence[str]] | None = None
) -> HeaderCheck:
    """Compare the keys used in the JSON objects of ``path`` with ``expected``.

    ``nested`` maps a key holding a list of objects (such as ``items``) to the keys those objects
    must use; they are reported as ``items.quantity``. A key is missing only when no record uses it.
    Lines that are not JSON objects are skipped: Bronze keeps them as corrupt records and Silver
    quarantines them. A file without any JSON object has nothing to compare, so it passes, and its
    rows are judged downstream. Names are matched exactly, as for CSV headers.
    """
    nested = nested or {}
    seen: dict[str, set[str]] = {"": set()} | {parent: set() for parent in nested}
    found_record = False
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            record = _parse_object(line)
            if record is None:
                continue
            found_record = True
            seen[""].update(record)
            for parent in nested:
                children = record.get(parent)
                if isinstance(children, list):
                    seen[parent].update(
                        key for child in children if isinstance(child, dict) for key in child
                    )
    if not found_record:
        return HeaderCheck(file=path.name)

    def compare(prefix: str, keys: Sequence[str], used: set[str]) -> tuple[list[str], list[str]]:
        missing = [f"{prefix}{key}" for key in keys if key not in used]
        extra = [f"{prefix}{key}" for key in sorted(used) if key not in keys]
        return missing, extra

    missing, extra = compare("", expected, seen[""])
    for parent, keys in nested.items():
        # A parent that never appears is already reported; its children add nothing.
        if parent in seen[""]:
            child_missing, child_extra = compare(f"{parent}.", keys, seen[parent])
            missing += child_missing
            extra += child_extra
    return HeaderCheck(file=path.name, missing=tuple(missing), extra=tuple(extra))


def _parse_object(line: str) -> dict[str, Any] | None:
    """The JSON object on ``line``, or None for a blank, malformed or non-object line."""
    try:
        value = json.loads(line)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def raise_on_failures(checks: Sequence[HeaderCheck]) -> list[HeaderCheck]:
    """Raise :class:`RawContractError` listing every failed check, or return ``checks``."""
    failures = [check for check in checks if not check.passed]
    if failures:
        raise RawContractError(failures)
    return list(checks)

"""Command line interface: ``medallion-pipeline generate|run|dashboard|vacuum``."""

import argparse
import logging
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

from medallion_pipeline.config import DEFAULT_MAX_REJECTION_RATE, LakePaths
from medallion_pipeline.dashboard import build_dashboard
from medallion_pipeline.data_generator import GenerationConfig, generate_raw_data
from medallion_pipeline.maintenance import DEFAULT_RETENTION_HOURS, vacuum_lake
from medallion_pipeline.orchestrator import RetryPolicy
from medallion_pipeline.pipeline import run_pipeline
from medallion_pipeline.quality import DataQualityError
from medallion_pipeline.spark import create_spark_session


def _fraction(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not a number") from None
    if not 0 <= number <= 1:
        raise argparse.ArgumentTypeError(f"{value} is not between 0 and 1")
    return number


def _non_negative_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not an integer") from None
    if number < 0:
        raise argparse.ArgumentTypeError(f"{value} is negative")
    return number


def _non_negative_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not a number") from None
    if number < 0:
        raise argparse.ArgumentTypeError(f"{value} is negative")
    return number


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="medallion-pipeline", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    generate = commands.add_parser("generate", help="write synthetic raw data")
    generate.add_argument("--data-dir", type=Path, default=Path("data"))
    generate.add_argument("--seed", type=int, default=GenerationConfig.seed)

    run = commands.add_parser("run", help="run Bronze, Silver, Gold and the quality gate")
    run.add_argument("--data-dir", type=Path, default=Path("data"))
    run.add_argument("--batch-id", help="defaults to the current UTC timestamp")
    run.add_argument(
        "--max-rejection-rate",
        type=_fraction,
        default=DEFAULT_MAX_REJECTION_RATE,
        help="fail the run if more than this share of any entity is quarantined (0 to 1)",
    )
    run.add_argument(
        "--incremental",
        action="store_true",
        help="merge only batches not yet in the watermark instead of rebuilding Silver",
    )
    run.add_argument(
        "--retries",
        type=_non_negative_int,
        default=0,
        help="retry a failing layer this many times with exponential backoff (default: 0)",
    )
    run.add_argument(
        "--retry-delay",
        type=float,
        default=2.0,
        help="seconds to wait before the first retry; doubles after each one (default: 2)",
    )

    dashboard = commands.add_parser(
        "dashboard", help="render an HTML dashboard from the Gold marts"
    )
    dashboard.add_argument("--data-dir", type=Path, default=Path("data"))
    dashboard.add_argument(
        "--output", type=Path, help="defaults to <data-dir>/dashboard/index.html"
    )

    vacuum = commands.add_parser(
        "vacuum", help="delete Delta files of versions older than the retention window"
    )
    vacuum.add_argument("--data-dir", type=Path, default=Path("data"))
    vacuum.add_argument(
        "--retain-hours",
        type=_non_negative_float,
        default=DEFAULT_RETENTION_HOURS,
        help=f"hours of history to keep readable (default: {DEFAULT_RETENTION_HOURS})",
    )
    vacuum.add_argument(
        "--allow-short-retention",
        action="store_true",
        help="permit a window under the default; only when nothing else reads the lake",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _build_parser().parse_args(argv)
    paths = LakePaths(args.data_dir.resolve())

    if args.command == "generate":
        expected = generate_raw_data(paths.raw, GenerationConfig(seed=args.seed))
        logging.info("Raw data written to %s; expected counts: %s", paths.raw, asdict(expected))
        return 0

    if args.command == "dashboard":
        if not (paths.gold / "daily_revenue").exists():
            logging.error("No Gold data in %s; run the run command first", paths.gold)
            return 2
        spark = create_spark_session()
        try:
            logging.info("Dashboard written to %s", build_dashboard(spark, paths, args.output))
        finally:
            spark.stop()
        return 0

    if args.command == "vacuum":
        if args.retain_hours < DEFAULT_RETENTION_HOURS and not args.allow_short_retention:
            logging.error(
                "--retain-hours %s is below the %d-hour safety window; add "
                "--allow-short-retention if nothing else reads the lake",
                args.retain_hours,
                DEFAULT_RETENTION_HOURS,
            )
            return 2
        spark = create_spark_session()
        try:
            results = vacuum_lake(spark, paths, args.retain_hours, args.allow_short_retention)
        finally:
            spark.stop()
        logging.info(
            "Vacuumed %d tables; removed %d files (%d bytes)",
            len(results),
            sum(r.files_removed for r in results),
            sum(r.bytes_removed for r in results),
        )
        return 0

    if not (paths.raw / "orders.jsonl").exists():
        logging.error("No raw data in %s; run the generate command first", paths.raw)
        return 2

    spark = create_spark_session()
    try:
        result = run_pipeline(
            spark,
            paths,
            args.batch_id,
            args.max_rejection_rate,
            args.incremental,
            RetryPolicy(args.retries, max(args.retry_delay, 0.0)),
        )
    except DataQualityError as error:
        logging.error("%s", error)
        return 1
    finally:
        spark.stop()
    logging.info("Row counts: %s", result.row_counts)
    return 0

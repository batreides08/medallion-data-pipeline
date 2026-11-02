"""Command line interface: ``medallion-pipeline generate|run``."""

import argparse
import logging
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

from medallion_pipeline.config import DEFAULT_MAX_REJECTION_RATE, LakePaths
from medallion_pipeline.data_generator import GenerationConfig, generate_raw_data
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _build_parser().parse_args(argv)
    paths = LakePaths(args.data_dir.resolve())

    if args.command == "generate":
        expected = generate_raw_data(paths.raw, GenerationConfig(seed=args.seed))
        logging.info("Raw data written to %s; expected counts: %s", paths.raw, asdict(expected))
        return 0

    if not (paths.raw / "orders.jsonl").exists():
        logging.error("No raw data in %s; run the generate command first", paths.raw)
        return 2

    spark = create_spark_session()
    try:
        result = run_pipeline(spark, paths, args.batch_id, args.max_rejection_rate)
    except DataQualityError as error:
        logging.error("%s", error)
        return 1
    finally:
        spark.stop()
    logging.info("Row counts: %s", result.row_counts)
    return 0

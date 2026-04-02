"""pipeline.py — Orchestrate the full Job Market Analysis ETL pipeline.

Runs all six steps in sequence, logging each with timestamps and halting
immediately on any failure with a descriptive error message.

Usage
-----
    python scripts/pipeline.py                # full run from scratch
    python scripts/pipeline.py --skip-extract # reuse existing raw CSV

Steps
-----
  1. auth     — validate Kaggle credentials
  2. extract  — download dataset from Kaggle  (skipped with --skip-extract)
  3. transform — clean data and engineer features
  4. load     — insert into PostgreSQL
  5. query    — execute analytical SQL and export CSVs
  6. report   — generate final_report.md and charts
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure the scripts/ directory is on sys.path so sibling modules import cleanly
SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step runner
# ---------------------------------------------------------------------------

def run_step(label: str, fn, *args, **kwargs) -> None:
    """Execute a single pipeline step, logging duration and halting on failure.

    Args:
        label: Human-readable step name for log output.
        fn: Callable to invoke.
        *args: Positional arguments forwarded to fn.
        **kwargs: Keyword arguments forwarded to fn.

    Raises:
        SystemExit: On any exception raised by fn (exit code 1).
    """
    logger.info("━━━ START  %s ━━━", label)
    t0 = time.perf_counter()
    try:
        fn(*args, **kwargs)
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        logger.error("━━━ FAILED %s after %.1fs ━━━", label, elapsed)
        logger.error("Error: %s", exc)
        sys.exit(1)
    elapsed = time.perf_counter() - t0
    logger.info("━━━ DONE   %s in %.1fs ━━━", label, elapsed)


# ---------------------------------------------------------------------------
# Final file-existence check
# ---------------------------------------------------------------------------

def final_validation(skip_extract: bool) -> None:
    """Assert that all expected pipeline output files exist.

    Args:
        skip_extract: Whether the extract step was skipped; the raw CSV is
            still expected on disk either way.

    Raises:
        SystemExit: If any expected file is absent (exit code 1).
    """
    expected: list[Path] = [
        PROJECT_ROOT / "data" / "raw"       / "global_ai_jobs.csv",
        PROJECT_ROOT / "data" / "processed" / "cleaned_jobs.parquet",
        PROJECT_ROOT / "reports" / "final_report.md",
        PROJECT_ROOT / "reports" / "charts" / "avg_salary_by_ai_specialization.png",
        PROJECT_ROOT / "reports" / "charts" / "yoy_salary_trend_top5_roles.png",
    ]

    query_results_dir = PROJECT_ROOT / "reports" / "query_results"
    expected_csvs = [
        "top_ai_specializations_by_salary.csv",
        "salary_by_experience_level.csv",
        "yoy_salary_trend_top5_roles.csv",
        "geographic_salary_comparison.csv",
        "work_mode_and_company_size_impact.csv",
        "automation_risk_profile.csv",
    ]
    for name in expected_csvs:
        expected.append(query_results_dir / name)

    missing = [p for p in expected if not p.exists()]
    if missing:
        logger.error("Final validation FAILED — missing files:")
        for p in missing:
            logger.error("  %s", p)
        sys.exit(1)

    logger.info("Final validation OK — all %d expected output files present", len(expected))


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed namespace with attribute skip_extract (bool).
    """
    parser = argparse.ArgumentParser(
        description="Job Market Analysis ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/pipeline.py                # full run\n"
            "  python scripts/pipeline.py --skip-extract # reuse existing CSV\n"
        ),
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        default=False,
        help=(
            "Skip the Kaggle download and reuse data/raw/global_ai_jobs.csv. "
            "Credentials are still validated."
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the complete ETL pipeline end-to-end."""
    args = parse_args()
    started_at = datetime.now()

    logger.info("=" * 60)
    logger.info("JOB MARKET ANALYSIS ETL PIPELINE")
    logger.info("Started at: %s", started_at.strftime("%Y-%m-%d %H:%M:%S"))
    if args.skip_extract:
        logger.info("Mode: --skip-extract (reusing existing raw CSV)")
    logger.info("=" * 60)

    # Lazy imports — each module configures its own logger on import;
    # importing here (not at module level) means logging is already
    # configured before any module-level code runs.
    from auth      import load_kaggle_credentials, validate_credentials  # noqa: PLC0415
    from extract   import extract                                          # noqa: PLC0415
    from transform import transform                                        # noqa: PLC0415
    from load      import load                                             # noqa: PLC0415
    from query     import query                                            # noqa: PLC0415
    from report    import report                                           # noqa: PLC0415

    # Step 1 — credentials (always runs)
    run_step("Step 1: Auth", load_kaggle_credentials)
    run_step("Step 1: Auth validate", validate_credentials)

    # Step 2 — extract (optional)
    if args.skip_extract:
        raw_csv = PROJECT_ROOT / "data" / "raw" / "global_ai_jobs.csv"
        if not raw_csv.exists():
            logger.error(
                "--skip-extract was set but '%s' does not exist. "
                "Run without --skip-extract first.",
                raw_csv,
            )
            sys.exit(1)
        logger.info("━━━ SKIP   Step 2: Extract (--skip-extract) ━━━")
    else:
        run_step("Step 2: Extract", extract)

    # Steps 3–6
    run_step("Step 3: Transform", transform)
    run_step("Step 4: Load",      load)
    run_step("Step 5: Query",     query)
    run_step("Step 6: Report",    report)

    # Final file-existence check
    final_validation(skip_extract=args.skip_extract)

    finished_at = datetime.now()
    total = (finished_at - started_at).total_seconds()
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("Finished at : %s", finished_at.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("Total time  : %.1f seconds", total)
    logger.info("Report      : %s", PROJECT_ROOT / "reports" / "final_report.md")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

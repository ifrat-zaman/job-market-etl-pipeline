"""extract.py — Download the Global AI & Data Jobs Salary Dataset from Kaggle.

Steps:
  1. Load credentials from Kaggle.txt via auth.py.
  2. Download the dataset using kagglehub.
  3. Copy the CSV to data/raw/global_ai_jobs.csv.
  4. Validate: 35 columns, ≥80,000 rows, no fully-null columns.
  5. Log summary statistics.
"""

import logging
import shutil
import sys
from pathlib import Path

import pandas as pd

# Project root is one level above this script
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_CSV = DATA_RAW_DIR / "global_ai_jobs.csv"

KAGGLE_DATASET = "mohankrishnathalla/global-ai-and-data-jobs-salary-dataset"

EXPECTED_COLUMNS = [
    "id", "country", "job_role", "ai_specialization", "experience_level",
    "experience_years", "salary_usd", "bonus_usd", "education_required",
    "industry", "company_size", "interview_rounds", "year", "work_mode",
    "weekly_hours", "company_rating", "job_openings", "hiring_difficulty_score",
    "layoff_risk", "ai_adoption_score", "company_funding_billion", "economic_index",
    "ai_maturity_years", "offer_acceptance_rate", "tax_rate_percent", "vacation_days",
    "skill_demand_score", "automation_risk", "job_security_score", "career_growth_score",
    "work_life_balance_score", "promotion_speed", "salary_percentile",
    "cost_of_living_index", "employee_satisfaction",
]

MIN_ROWS = 80_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_credentials() -> None:
    """Load Kaggle credentials from Kaggle.txt into the environment."""
    # Import here so extract.py can also be run standalone
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
    from auth import load_kaggle_credentials, validate_credentials  # noqa: PLC0415

    load_kaggle_credentials()
    validate_credentials()


def download_dataset() -> Path:
    """Download the Kaggle dataset and return the path to the downloaded directory.

    Returns:
        Path to the directory containing the downloaded CSV file(s).

    Raises:
        RuntimeError: If the download fails.
    """
    try:
        import kagglehub  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "kagglehub is not installed. Run: pip install kagglehub"
        ) from exc

    logger.info("Downloading dataset '%s' via kagglehub…", KAGGLE_DATASET)
    try:
        download_dir = kagglehub.dataset_download(KAGGLE_DATASET)
    except Exception as exc:
        raise RuntimeError(
            f"kagglehub download failed for dataset '{KAGGLE_DATASET}': {exc}"
        ) from exc

    download_path = Path(download_dir)
    logger.info("Dataset downloaded to: %s", download_path)
    return download_path


def find_csv(download_dir: Path) -> Path:
    """Locate the primary CSV file inside the downloaded directory.

    Args:
        download_dir: Directory returned by kagglehub.

    Returns:
        Path to the CSV file.

    Raises:
        FileNotFoundError: If no CSV file is found.
    """
    csv_files = list(download_dir.glob("*.csv"))
    if not csv_files:
        # Recurse one level for nested directories
        csv_files = list(download_dir.glob("**/*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV file found in downloaded directory '{download_dir}'. "
            "Check the Kaggle dataset structure."
        )

    if len(csv_files) > 1:
        logger.warning("Multiple CSVs found; using the largest: %s", csv_files)
        csv_files.sort(key=lambda p: p.stat().st_size, reverse=True)

    chosen = csv_files[0]
    logger.info("Using CSV: %s (%.1f MB)", chosen, chosen.stat().st_size / 1_048_576)
    return chosen


def copy_to_raw(source_csv: Path) -> None:
    """Copy the downloaded CSV to data/raw/global_ai_jobs.csv.

    Args:
        source_csv: Path to the source CSV file.
    """
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_csv, OUTPUT_CSV)
    logger.info("CSV copied to: %s", OUTPUT_CSV)


def validate_csv(df: pd.DataFrame) -> None:
    """Run all schema and data-quality assertions on the loaded DataFrame.

    Args:
        df: The DataFrame read from data/raw/global_ai_jobs.csv.

    Raises:
        AssertionError: If any validation check fails.
        ValueError: If column names do not match the expected schema.
    """
    # 1. Row count
    assert len(df) >= MIN_ROWS, (
        f"Row count too low: got {len(df):,}, expected ≥ {MIN_ROWS:,}. "
        "The downloaded file may be incomplete."
    )
    logger.info("Row count OK: %d rows", len(df))

    # 2. Column count
    assert len(df.columns) == 35, (
        f"Column count mismatch: got {len(df.columns)}, expected 35. "
        f"Actual columns: {list(df.columns)}"
    )
    logger.info("Column count OK: 35 columns")

    # 3. Column names match schema exactly
    actual_cols = list(df.columns)
    if actual_cols != EXPECTED_COLUMNS:
        missing = set(EXPECTED_COLUMNS) - set(actual_cols)
        extra = set(actual_cols) - set(EXPECTED_COLUMNS)
        raise ValueError(
            f"Column name mismatch.\n"
            f"  Missing from CSV : {sorted(missing)}\n"
            f"  Extra in CSV     : {sorted(extra)}"
        )
    logger.info("Column names OK: all 35 columns match schema")

    # 4. No completely null columns
    null_cols = df.columns[df.isnull().all()].tolist()
    assert not null_cols, (
        f"The following columns are entirely null: {null_cols}"
    )
    logger.info("Null-column check OK: no fully-null columns")

    # 5. Critical columns have no nulls
    for col in ("salary_usd", "job_role", "country"):
        null_count = df[col].isnull().sum()
        assert null_count == 0, (
            f"Critical column '{col}' has {null_count:,} null values."
        )
    logger.info("Critical-column null check OK")


def log_summary(df: pd.DataFrame) -> None:
    """Log row count, column list, and numeric summary statistics.

    Args:
        df: The validated DataFrame.
    """
    logger.info("=== Dataset Summary ===")
    logger.info("Shape: %d rows × %d columns", *df.shape)
    logger.info("Columns: %s", list(df.columns))
    logger.info(
        "salary_usd — min: $%s  median: $%s  max: $%s",
        f"{df['salary_usd'].min():,.0f}",
        f"{df['salary_usd'].median():,.0f}",
        f"{df['salary_usd'].max():,.0f}",
    )
    logger.info(
        "bonus_usd  — min: $%s  median: $%s  max: $%s",
        f"{df['bonus_usd'].min():,.0f}",
        f"{df['bonus_usd'].median():,.0f}",
        f"{df['bonus_usd'].max():,.0f}",
    )
    logger.info("Years covered: %s", sorted(df["year"].unique().tolist()))
    logger.info("Countries: %d unique", df["country"].nunique())
    logger.info("Job roles: %d unique", df["job_role"].nunique())


def extract() -> pd.DataFrame:
    """Run the full extraction pipeline.

    Returns:
        The validated DataFrame loaded from data/raw/global_ai_jobs.csv.

    Raises:
        FileNotFoundError: If credentials or download files are missing.
        AssertionError | ValueError: If validation fails.
        RuntimeError: If the Kaggle download fails.
    """
    logger.info("=== STEP 1: EXTRACT ===")

    load_credentials()
    download_dir = download_dataset()
    source_csv = find_csv(download_dir)
    copy_to_raw(source_csv)

    # Verify the destination file exists
    assert OUTPUT_CSV.exists(), f"Expected output CSV not found at '{OUTPUT_CSV}'"

    logger.info("Loading CSV into pandas for validation…")
    try:
        df = pd.read_csv(OUTPUT_CSV)
    except Exception as exc:
        raise RuntimeError(f"Failed to read '{OUTPUT_CSV}' with pandas: {exc}") from exc

    validate_csv(df)
    log_summary(df)

    logger.info("=== EXTRACT COMPLETE — all validations passed ===")
    return df


if __name__ == "__main__":
    extract()

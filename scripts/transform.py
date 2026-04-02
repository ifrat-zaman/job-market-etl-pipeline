"""transform.py — Clean and feature-engineer the raw Global AI Jobs dataset.

Reads  : data/raw/global_ai_jobs.csv
Writes : data/processed/cleaned_jobs.parquet

Transformations applied
-----------------------
Type enforcement
  year                → int
  salary_usd,
  bonus_usd           → float
  experience_level,
  work_mode,
  company_size,
  education_required,
  industry,
  ai_specialization   → pandas Categorical

Derived columns
  total_compensation  = salary_usd + bonus_usd
  salary_band         = quantile-based Low / Medium / High  (salary_usd)
  compensation_band   = quantile-based Low / Medium / High  (total_compensation)
  high_automation_risk= True where automation_risk ≥ 75th-percentile
  senior_flag         = True where experience_level in {'Senior', 'Lead'}
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CSV = PROJECT_ROOT / "data" / "raw" / "global_ai_jobs.csv"
OUTPUT_PARQUET = PROJECT_ROOT / "data" / "processed" / "cleaned_jobs.parquet"

CATEGORICAL_COLS = [
    "experience_level",
    "work_mode",
    "company_size",
    "education_required",
    "industry",
    "ai_specialization",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _quantile_band(series: pd.Series, labels: list[str]) -> pd.Series:
    """Assign quantile-based band labels to a numeric series.

    Uses pandas.qcut with three equal-width quantile buckets (0–33%, 33–67%,
    67–100%).  duplicate_edge='drop' is passed to avoid errors when the
    distribution has repeated boundary values.

    Args:
        series: Numeric column to bin.
        labels: Three-element list of string labels for low / medium / high.

    Returns:
        Categorical Series with the three band labels.
    """
    return pd.qcut(
        series,
        q=3,
        labels=labels,
        duplicates="drop",
    )


# ---------------------------------------------------------------------------
# Transform steps
# ---------------------------------------------------------------------------

def enforce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to their authoritative types.

    Args:
        df: Raw DataFrame from CSV.

    Returns:
        DataFrame with corrected dtypes.
    """
    df = df.copy()

    df["year"] = df["year"].astype(int)
    df["salary_usd"] = df["salary_usd"].astype(float)
    df["bonus_usd"] = df["bonus_usd"].astype(float)

    for col in CATEGORICAL_COLS:
        df[col] = df[col].astype("category")

    logger.info("Type enforcement complete.")
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Engineer all derived feature columns.

    Args:
        df: Type-enforced DataFrame.

    Returns:
        DataFrame with six additional columns.
    """
    df = df.copy()

    # total_compensation
    df["total_compensation"] = df["salary_usd"] + df["bonus_usd"]
    logger.info("Derived: total_compensation")

    # salary_band  (Low / Medium / High based on salary_usd terciles)
    df["salary_band"] = _quantile_band(
        df["salary_usd"], labels=["Low", "Medium", "High"]
    )
    logger.info("Derived: salary_band")

    # compensation_band  (same logic on total_compensation)
    df["compensation_band"] = _quantile_band(
        df["total_compensation"], labels=["Low", "Medium", "High"]
    )
    logger.info("Derived: compensation_band")

    # high_automation_risk — True where automation_risk is in top quartile (≥ Q3)
    q3_automation = df["automation_risk"].quantile(0.75)
    df["high_automation_risk"] = df["automation_risk"] >= q3_automation
    logger.info(
        "Derived: high_automation_risk  (Q3 threshold = %s)", q3_automation
    )

    # senior_flag — True for Senior or Lead experience level
    df["senior_flag"] = df["experience_level"].isin(["Senior", "Lead"])
    logger.info("Derived: senior_flag")

    return df


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_transformed(df: pd.DataFrame) -> None:
    """Assert data quality of the transformed DataFrame.

    Checks:
      - No nulls in salary_usd, job_role, country.
      - All six derived columns are fully populated (no nulls).
      - total_compensation min/max/mean are plausible.

    Args:
        df: Fully transformed DataFrame.

    Raises:
        AssertionError: On any failed check.
    """
    # Critical source columns must remain null-free
    for col in ("salary_usd", "job_role", "country"):
        null_count = df[col].isnull().sum()
        assert null_count == 0, (
            f"Critical column '{col}' has {null_count:,} null values after transform."
        )
    logger.info("Null check on critical columns: OK")

    # All derived columns must be fully populated
    derived = [
        "total_compensation",
        "salary_band",
        "compensation_band",
        "high_automation_risk",
        "senior_flag",
    ]
    for col in derived:
        null_count = df[col].isnull().sum()
        assert null_count == 0, (
            f"Derived column '{col}' has {null_count:,} null values — "
            "check the quantile binning logic."
        )
    logger.info("Derived columns fully populated: OK")

    # Sanity-check total_compensation figures
    tc_min = df["total_compensation"].min()
    tc_max = df["total_compensation"].max()
    tc_mean = df["total_compensation"].mean()
    logger.info(
        "total_compensation — min: $%s  mean: $%s  max: $%s",
        f"{tc_min:,.0f}",
        f"{tc_mean:,.0f}",
        f"{tc_max:,.0f}",
    )

    assert tc_min > 0, f"total_compensation minimum is non-positive: {tc_min}"
    assert tc_mean > 50_000, (
        f"total_compensation mean looks too low ({tc_mean:,.0f}); expected > $50,000"
    )


def log_transform_summary(df: pd.DataFrame) -> None:
    """Log a concise summary of the transformed dataset.

    Args:
        df: Fully validated transformed DataFrame.
    """
    logger.info("=== Transform Summary ===")
    logger.info("Shape: %d rows × %d columns", *df.shape)
    logger.info(
        "salary_band distribution:\n%s",
        df["salary_band"].value_counts().sort_index().to_string(),
    )
    logger.info(
        "high_automation_risk: %d True / %d False",
        df["high_automation_risk"].sum(),
        (~df["high_automation_risk"]).sum(),
    )
    logger.info(
        "senior_flag: %d Senior/Lead / %d other",
        df["senior_flag"].sum(),
        (~df["senior_flag"]).sum(),
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def transform() -> pd.DataFrame:
    """Run the full transformation pipeline and write cleaned_jobs.parquet.

    Returns:
        The fully transformed and validated DataFrame.

    Raises:
        FileNotFoundError: If the raw CSV is missing.
        AssertionError | ValueError: If validation fails.
        OSError: If the output Parquet file cannot be written.
    """
    logger.info("=== STEP 2: TRANSFORM ===")

    if not INPUT_CSV.exists():
        raise FileNotFoundError(
            f"Raw CSV not found at '{INPUT_CSV}'. "
            "Run scripts/extract.py first."
        )

    logger.info("Reading raw CSV: %s", INPUT_CSV)
    try:
        df = pd.read_csv(INPUT_CSV)
    except Exception as exc:
        raise RuntimeError(f"Failed to read '{INPUT_CSV}': {exc}") from exc

    logger.info("Loaded %d rows × %d columns", *df.shape)

    df = enforce_types(df)
    df = add_derived_columns(df)

    validate_transformed(df)
    log_transform_summary(df)

    # Write output
    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(OUTPUT_PARQUET, index=False, engine="pyarrow")
    except Exception as exc:
        raise OSError(
            f"Failed to write Parquet to '{OUTPUT_PARQUET}': {exc}"
        ) from exc

    logger.info("Parquet written: %s  (%.1f MB)", OUTPUT_PARQUET,
                OUTPUT_PARQUET.stat().st_size / 1_048_576)
    logger.info("=== TRANSFORM COMPLETE — all validations passed ===")
    return df


if __name__ == "__main__":
    transform()

"""load.py — Load the cleaned dataset into PostgreSQL across three normalised tables.

Tables created
--------------
  jobs          Core fact table — one row per job posting.
  job_metrics   Numeric scores per job (FK → jobs.id).
  job_features  Engineered flags and bands per job (FK → jobs.id).

Idempotency
-----------
  On every run the three tables are truncated (child tables first, then parent)
  before data is inserted, so re-running the script produces identical results.

Connection
----------
  Reads DB_NAME / DB_USER / DB_PASSWORD / DB_HOST / DB_PORT from the
  environment.  Falls back to local Homebrew defaults (ifratzaman, no password).
  If the target database does not exist it is created automatically.
"""

import logging
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql as pgsql
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PARQUET = PROJECT_ROOT / "data" / "processed" / "cleaned_jobs.parquet"

# ---------------------------------------------------------------------------
# DB connection defaults  (Homebrew PostgreSQL — Mac system user, no password)
# ---------------------------------------------------------------------------
DB_NAME = os.getenv("DB_NAME", "job_analysis")
DB_USER = os.getenv("DB_USER", "ifratzaman")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")          # must stay empty for Homebrew
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column groups (must match CLAUDE.md schema table exactly)
# ---------------------------------------------------------------------------

JOBS_COLS = [
    "id", "country", "job_role", "ai_specialization", "experience_level",
    "experience_years", "salary_usd", "bonus_usd", "total_compensation",
    "education_required", "industry", "company_size", "year", "work_mode",
    "weekly_hours", "company_rating", "job_openings",
]

JOB_METRICS_COLS = [
    "id", "hiring_difficulty_score", "layoff_risk", "ai_adoption_score",
    "company_funding_billion", "economic_index", "ai_maturity_years",
    "offer_acceptance_rate", "tax_rate_percent", "vacation_days",
    "skill_demand_score", "automation_risk", "job_security_score",
    "career_growth_score", "work_life_balance_score", "promotion_speed",
    "salary_percentile", "cost_of_living_index", "employee_satisfaction",
]

JOB_FEATURES_COLS = [
    "id", "salary_band", "compensation_band", "high_automation_risk", "senior_flag",
]

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_JOBS = """
CREATE TABLE IF NOT EXISTS jobs (
    id                  INTEGER         PRIMARY KEY,
    country             TEXT            NOT NULL,
    job_role            TEXT            NOT NULL,
    ai_specialization   TEXT            NOT NULL,
    experience_level    TEXT            NOT NULL,
    experience_years    INTEGER         NOT NULL,
    salary_usd          NUMERIC(12, 2)  NOT NULL,
    bonus_usd           NUMERIC(12, 2)  NOT NULL,
    total_compensation  NUMERIC(12, 2)  NOT NULL,
    education_required  TEXT            NOT NULL,
    industry            TEXT            NOT NULL,
    company_size        TEXT            NOT NULL,
    year                INTEGER         NOT NULL,
    work_mode           TEXT            NOT NULL,
    weekly_hours        NUMERIC(5, 2)   NOT NULL,
    company_rating      NUMERIC(4, 2)   NOT NULL,
    job_openings        INTEGER         NOT NULL
);
"""

DDL_JOB_METRICS = """
CREATE TABLE IF NOT EXISTS job_metrics (
    id                      INTEGER         PRIMARY KEY
                                            REFERENCES jobs (id) ON DELETE CASCADE,
    hiring_difficulty_score NUMERIC(7, 4)   NOT NULL,  -- 0–100.0000; needs 3 pre-decimal digits
    layoff_risk             NUMERIC(6, 4)   NOT NULL,  -- 0–1; 2 pre-decimal digits sufficient
    ai_adoption_score       INTEGER         NOT NULL,
    company_funding_billion NUMERIC(10, 4)  NOT NULL,
    economic_index          NUMERIC(8, 4)   NOT NULL,
    ai_maturity_years       INTEGER         NOT NULL,
    offer_acceptance_rate   NUMERIC(7, 4)   NOT NULL,  -- 0–100 range; widened from (6,4)
    tax_rate_percent        NUMERIC(7, 4)   NOT NULL,  -- 0–100 range; widened from (6,4)
    vacation_days           INTEGER         NOT NULL,
    skill_demand_score      INTEGER         NOT NULL,
    automation_risk         INTEGER         NOT NULL,
    job_security_score      INTEGER         NOT NULL,
    career_growth_score     INTEGER         NOT NULL,
    work_life_balance_score INTEGER         NOT NULL,
    promotion_speed         INTEGER         NOT NULL,
    salary_percentile       INTEGER         NOT NULL,
    cost_of_living_index    NUMERIC(8, 4)   NOT NULL,
    employee_satisfaction   INTEGER         NOT NULL
);
"""

DDL_JOB_FEATURES = """
CREATE TABLE IF NOT EXISTS job_features (
    id                   INTEGER  PRIMARY KEY
                                  REFERENCES jobs (id) ON DELETE CASCADE,
    salary_band          TEXT     NOT NULL,
    compensation_band    TEXT     NOT NULL,
    high_automation_risk BOOLEAN  NOT NULL,
    senior_flag          BOOLEAN  NOT NULL
);
"""


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _connection_string(db_name: str) -> str:
    """Build a psycopg2-compatible SQLAlchemy connection string.

    An empty password is represented by omitting the password component
    entirely so that Homebrew PostgreSQL peer/trust auth works correctly.

    Args:
        db_name: Target database name.

    Returns:
        SQLAlchemy connection URL string.
    """
    if DB_PASSWORD:
        return (
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
            f"@{DB_HOST}:{DB_PORT}/{db_name}"
        )
    return f"postgresql+psycopg2://{DB_USER}@{DB_HOST}:{DB_PORT}/{db_name}"


def ensure_database_exists() -> None:
    """Create the target database if it does not already exist.

    Connects to the 'postgres' maintenance database first, then issues a
    CREATE DATABASE statement if needed.  Uses autocommit because CREATE
    DATABASE cannot run inside a transaction block.

    Raises:
        psycopg2.OperationalError: If the server is unreachable.
    """
    logger.info("Ensuring database '%s' exists…", DB_NAME)
    conn_kwargs: dict = dict(
        dbname="postgres",
        user=DB_USER,
        host=DB_HOST,
        port=int(DB_PORT),
    )
    if DB_PASSWORD:
        conn_kwargs["password"] = DB_PASSWORD

    try:
        conn = psycopg2.connect(**conn_kwargs)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,)
            )
            if cur.fetchone() is None:
                # Identifier must be composed safely — not via %s
                cur.execute(
                    pgsql.SQL("CREATE DATABASE {}").format(
                        pgsql.Identifier(DB_NAME)
                    )
                )
                logger.info("Database '%s' created.", DB_NAME)
            else:
                logger.info("Database '%s' already exists.", DB_NAME)
        conn.close()
    except psycopg2.OperationalError as exc:
        raise psycopg2.OperationalError(
            f"Cannot connect to PostgreSQL at {DB_HOST}:{DB_PORT} as '{DB_USER}'. "
            f"Is the server running?  Original error: {exc}"
        ) from exc


def get_engine():
    """Create and return a SQLAlchemy engine for the target database.

    Returns:
        sqlalchemy.engine.Engine connected to DB_NAME.
    """
    url = _connection_string(DB_NAME)
    engine = create_engine(url, future=True)
    return engine


# ---------------------------------------------------------------------------
# Schema & truncation
# ---------------------------------------------------------------------------

def create_tables(engine) -> None:
    """Drop (if exists) and recreate all three tables with the current schema.

    Dropping ensures that any DDL change (e.g. widened NUMERIC precision) is
    applied on every run rather than silently skipped by IF NOT EXISTS.
    Child tables are dropped before the parent to satisfy FK constraints.

    Args:
        engine: SQLAlchemy engine connected to the target database.
    """
    with engine.begin() as conn:
        # Drop child tables first, then parent
        conn.execute(text("DROP TABLE IF EXISTS job_features;"))
        conn.execute(text("DROP TABLE IF EXISTS job_metrics;"))
        conn.execute(text("DROP TABLE IF EXISTS jobs;"))
        # Recreate with current schema
        conn.execute(text(DDL_JOBS))
        conn.execute(text(DDL_JOB_METRICS))
        conn.execute(text(DDL_JOB_FEATURES))
    logger.info("Tables dropped and recreated: jobs, job_metrics, job_features")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_table(
    df: pd.DataFrame,
    cols: list[str],
    table_name: str,
    engine,
    chunk_size: int = 5_000,
) -> None:
    """Insert a column slice of df into the named PostgreSQL table.

    Uses pandas.DataFrame.to_sql with method='multi' for efficient bulk
    inserts.  The table must already exist (if_exists='append').

    Args:
        df: Full transformed DataFrame.
        cols: Column names to select from df for this table.
        table_name: Target PostgreSQL table name.
        engine: SQLAlchemy engine.
        chunk_size: Rows per INSERT batch.
    """
    subset = df[cols].copy()

    # Convert pandas Categorical → plain str so SQLAlchemy maps cleanly to TEXT
    for col in subset.select_dtypes(include="category").columns:
        subset[col] = subset[col].astype(str)

    # Convert numpy bool → Python bool for psycopg2 compatibility
    for col in subset.select_dtypes(include="bool").columns:
        subset[col] = subset[col].astype(bool)

    try:
        subset.to_sql(
            table_name,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=chunk_size,
            method="multi",
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to insert data into '{table_name}': {exc}"
        ) from exc

    logger.info("Inserted %d rows into '%s'", len(subset), table_name)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_row_counts(df: pd.DataFrame, engine) -> None:
    """Compare SELECT COUNT(*) results against the source DataFrame length.

    Args:
        df: The source DataFrame that was loaded.
        engine: SQLAlchemy engine for running COUNT queries.

    Raises:
        AssertionError: If any table count does not match the source.
    """
    expected = len(df)
    with engine.connect() as conn:
        for table in ("jobs", "job_metrics", "job_features"):
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))  # noqa: S608
            count = result.scalar()
            assert count == expected, (
                f"Row count mismatch in '{table}': "
                f"loaded {count:,}, expected {expected:,}"
            )
            logger.info("Row count OK — %s: %d rows", table, count)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def load() -> None:
    """Run the full load pipeline: create schema, truncate, insert, validate.

    Raises:
        FileNotFoundError: If the cleaned Parquet file is missing.
        psycopg2.OperationalError: If PostgreSQL is unreachable.
        AssertionError: If post-load row counts do not match.
    """
    logger.info("=== STEP 3: LOAD ===")

    if not INPUT_PARQUET.exists():
        raise FileNotFoundError(
            f"Cleaned Parquet not found at '{INPUT_PARQUET}'. "
            "Run scripts/transform.py first."
        )

    logger.info("Reading Parquet: %s", INPUT_PARQUET)
    try:
        df = pd.read_parquet(INPUT_PARQUET, engine="pyarrow")
    except Exception as exc:
        raise RuntimeError(f"Failed to read Parquet '{INPUT_PARQUET}': {exc}") from exc

    logger.info("Loaded %d rows × %d columns from Parquet", *df.shape)

    ensure_database_exists()
    engine = get_engine()

    create_tables(engine)

    load_table(df, JOBS_COLS, "jobs", engine)
    load_table(df, JOB_METRICS_COLS, "job_metrics", engine)
    load_table(df, JOB_FEATURES_COLS, "job_features", engine)

    validate_row_counts(df, engine)

    engine.dispose()
    logger.info("=== LOAD COMPLETE — all row counts validated ===")


if __name__ == "__main__":
    load()

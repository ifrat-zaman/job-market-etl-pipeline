"""query.py — Execute analytical SQL queries and export results.

Reads  : sql/analysis_queries.sql
Writes : reports/query_results/<query_name>.csv  (one file per query)
Prints : tabular results to stdout via pandas

The SQL file is split on lines that match the sentinel pattern:
    -- [query_N: <name>]
Each named block is executed independently; results are validated
(non-empty), saved as CSV, and printed to the console.
"""

import logging
import os
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_FILE = PROJECT_ROOT / "sql" / "analysis_queries.sql"
RESULTS_DIR = PROJECT_ROOT / "reports" / "query_results"

DB_NAME = os.getenv("DB_NAME", "job_analysis")
DB_USER = os.getenv("DB_USER", "ifratzaman")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_engine():
    """Create a SQLAlchemy engine for the target database.

    Returns:
        sqlalchemy.engine.Engine
    """
    if DB_PASSWORD:
        url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    else:
        url = f"postgresql+psycopg2://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, future=True)


# ---------------------------------------------------------------------------
# SQL parsing
# ---------------------------------------------------------------------------

# Matches sentinel lines like:  -- [query_1: top_ai_specializations_by_salary]
_SENTINEL = re.compile(r"--\s*\[query_\d+:\s*([^\]]+)\]")


def parse_queries(sql_path: Path) -> list[tuple[str, str]]:
    """Parse the SQL file into named (name, sql_text) pairs.

    Splits the file on sentinel comment lines of the form:
        -- [query_N: <name>]
    and associates each block of SQL that follows with its name.

    Args:
        sql_path: Path to the SQL file.

    Returns:
        List of (name, sql_text) tuples in file order.

    Raises:
        FileNotFoundError: If the SQL file does not exist.
        ValueError: If no query blocks are found.
    """
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: '{sql_path}'")

    raw = sql_path.read_text(encoding="utf-8")
    lines = raw.splitlines(keepends=True)

    queries: list[tuple[str, str]] = []
    current_name: str | None = None
    current_lines: list[str] = []

    for line in lines:
        m = _SENTINEL.match(line.strip())
        if m:
            # Flush previous block
            if current_name is not None:
                sql_text = "".join(current_lines).strip().rstrip(";")
                if sql_text:
                    queries.append((current_name, sql_text))
            current_name = m.group(1).strip()
            current_lines = []
        else:
            if current_name is not None:
                current_lines.append(line)

    # Flush last block
    if current_name is not None:
        sql_text = "".join(current_lines).strip().rstrip(";")
        if sql_text:
            queries.append((current_name, sql_text))

    if not queries:
        raise ValueError(
            f"No query blocks found in '{sql_path}'. "
            "Each query must be preceded by a sentinel comment: "
            "-- [query_N: <name>]"
        )

    logger.info("Parsed %d queries from '%s'", len(queries), sql_path)
    return queries


# ---------------------------------------------------------------------------
# Execution & export
# ---------------------------------------------------------------------------

def run_query(name: str, sql: str, engine) -> pd.DataFrame:
    """Execute a single SQL query and return the result as a DataFrame.

    Args:
        name: Human-readable query name (used in log messages).
        sql: SQL text to execute.
        engine: SQLAlchemy engine.

    Returns:
        DataFrame containing the query results.

    Raises:
        RuntimeError: If the query fails.
    """
    logger.info("Running query: %s", name)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
    except Exception as exc:
        raise RuntimeError(f"Query '{name}' failed: {exc}") from exc
    return df


def save_csv(df: pd.DataFrame, name: str) -> Path:
    """Save a query result DataFrame to a CSV file.

    Args:
        df: Query result.
        name: Query name — used as the CSV filename stem.

    Returns:
        Path to the written CSV file.
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULTS_DIR / f"{name}.csv"
    df.to_csv(out_path, index=False)
    logger.info("Saved: %s  (%d rows)", out_path, len(df))
    return out_path


def print_table(name: str, df: pd.DataFrame) -> None:
    """Print a query result to stdout in a readable tabular format.

    Args:
        name: Query name displayed as a heading.
        df: Query result DataFrame.
    """
    separator = "=" * 72
    print(f"\n{separator}")
    print(f"  {name.replace('_', ' ').upper()}")
    print(separator)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 120)
    pd.set_option("display.float_format", "{:,.4f}".format)
    print(df.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_results(results: list[tuple[str, pd.DataFrame]]) -> None:
    """Assert every query returned at least one row.

    Args:
        results: List of (name, DataFrame) pairs.

    Raises:
        AssertionError: If any query returned zero rows.
    """
    for name, df in results:
        assert len(df) > 0, (
            f"Query '{name}' returned 0 rows — investigate the SQL and data."
        )
    logger.info("Validation OK — all %d queries returned non-empty results", len(results))


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def query() -> list[tuple[str, pd.DataFrame]]:
    """Run all queries, save CSVs, print results, validate non-empty.

    Returns:
        List of (name, DataFrame) pairs in query file order.

    Raises:
        FileNotFoundError: If the SQL file is missing.
        RuntimeError: If any query fails to execute.
        AssertionError: If any query returns zero rows.
    """
    logger.info("=== STEP 4: QUERY ===")

    queries = parse_queries(SQL_FILE)
    engine = get_engine()

    results: list[tuple[str, pd.DataFrame]] = []

    for name, sql in queries:
        df = run_query(name, sql, engine)
        print_table(name, df)
        save_csv(df, name)
        results.append((name, df))

    validate_results(results)

    engine.dispose()
    logger.info("=== QUERY COMPLETE — %d queries executed and saved ===", len(results))
    return results


if __name__ == "__main__":
    query()

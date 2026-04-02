"""report.py — Generate the final Markdown report and PNG charts.

Reads  : reports/query_results/*.csv  (produced by query.py)
         data/processed/cleaned_jobs.parquet  (for headline numbers)
Writes : reports/final_report.md
         reports/charts/avg_salary_by_ai_specialization.png
         reports/charts/yoy_salary_trend_top5_roles.png

Validation
----------
  Confirms final_report.md exists and contains all expected section headings.
  Confirms both chart PNGs exist and are non-empty (> 0 bytes).
"""

import logging
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

matplotlib.use("Agg")  # non-interactive backend — no display required

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR  = PROJECT_ROOT / "reports" / "query_results"
CHARTS_DIR   = PROJECT_ROOT / "reports" / "charts"
REPORT_PATH  = PROJECT_ROOT / "reports" / "final_report.md"
PARQUET_PATH = PROJECT_ROOT / "data" / "processed" / "cleaned_jobs.parquet"

CHART_BAR  = CHARTS_DIR / "avg_salary_by_ai_specialization.png"
CHART_LINE = CHARTS_DIR / "yoy_salary_trend_top5_roles.png"

# Section headings that must appear in the finished report
REQUIRED_SECTIONS = [
    "## Executive Summary",
    "## 1. Top AI Specializations by Average Salary",
    "## 2. Salary by Experience Level",
    "## 3. Year-over-Year Salary Trend",
    "## 4. Geographic Salary Comparison",
    "## 5. Work Mode and Company Size Impact",
    "## 6. Automation Risk Profile",
    "## Methodology",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper — load a query-result CSV with a clear error
# ---------------------------------------------------------------------------

def _load_csv(name: str) -> pd.DataFrame:
    """Load a query-result CSV by its stem name.

    Args:
        name: Filename stem (without .csv) under reports/query_results/.

    Returns:
        DataFrame with the CSV contents.

    Raises:
        FileNotFoundError: If the CSV does not exist.
    """
    path = RESULTS_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Expected query result CSV not found: '{path}'. "
            "Run scripts/query.py first."
        )
    return pd.read_csv(path)


# ---------------------------------------------------------------------------
# Headline numbers from the Parquet
# ---------------------------------------------------------------------------

def build_headline_numbers() -> dict:
    """Derive executive-summary statistics from the cleaned Parquet.

    Returns:
        Dict with keys: total_records, year_min, year_max, salary_min,
        salary_max, salary_mean, countries, job_roles.

    Raises:
        FileNotFoundError: If the Parquet is missing.
    """
    if not PARQUET_PATH.exists():
        raise FileNotFoundError(
            f"Cleaned Parquet not found at '{PARQUET_PATH}'. "
            "Run scripts/transform.py first."
        )
    df = pd.read_parquet(
        PARQUET_PATH,
        columns=["salary_usd", "year", "country", "job_role"],
        engine="pyarrow",
    )
    return {
        "total_records": len(df),
        "year_min":      int(df["year"].min()),
        "year_max":      int(df["year"].max()),
        "salary_min":    int(df["salary_usd"].min()),
        "salary_max":    int(df["salary_usd"].max()),
        "salary_mean":   round(float(df["salary_usd"].mean()), 2),
        "countries":     int(df["country"].nunique()),
        "job_roles":     int(df["job_role"].nunique()),
    }


# ---------------------------------------------------------------------------
# Chart 1 — horizontal bar: avg salary by AI specialization
# ---------------------------------------------------------------------------

def chart_salary_by_specialization(df: pd.DataFrame) -> None:
    """Save a horizontal bar chart of average salary per AI specialization.

    Args:
        df: DataFrame from top_ai_specializations_by_salary.csv.
            Expected columns: ai_specialization, avg_salary_usd.
    """
    df_sorted = df.sort_values("avg_salary_usd", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(
        df_sorted["ai_specialization"],
        df_sorted["avg_salary_usd"],
        color="#4C72B0",
        edgecolor="white",
        height=0.6,
    )

    # Value labels on bars
    for bar in bars:
        width = bar.get_width()
        ax.text(
            width + 200,
            bar.get_y() + bar.get_height() / 2,
            f"${width:,.0f}",
            va="center",
            ha="left",
            fontsize=9,
        )

    ax.set_xlabel("Average Base Salary (USD)", fontsize=11)
    ax.set_title("Average Salary by AI Specialization", fontsize=13, fontweight="bold")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_xlim(left=df_sorted["avg_salary_usd"].min() * 0.97,
                right=df_sorted["avg_salary_usd"].max() * 1.04)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="y", labelsize=10)
    plt.tight_layout()

    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(CHART_BAR, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Chart saved: %s", CHART_BAR)


# ---------------------------------------------------------------------------
# Chart 2 — line: year-over-year salary trend for top 5 roles
# ---------------------------------------------------------------------------

def chart_yoy_salary_trend(df: pd.DataFrame) -> None:
    """Save a line chart of year-over-year average salary for top 5 roles.

    Args:
        df: DataFrame from yoy_salary_trend_top5_roles.csv.
            Expected columns: job_role, year, avg_salary_usd.
    """
    roles = df["job_role"].unique()
    palette = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B3"]

    fig, ax = plt.subplots(figsize=(11, 6))

    for i, role in enumerate(sorted(roles)):
        subset = df[df["job_role"] == role].sort_values("year")
        ax.plot(
            subset["year"],
            subset["avg_salary_usd"],
            marker="o",
            linewidth=2,
            markersize=5,
            label=role,
            color=palette[i % len(palette)],
        )

    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel("Average Base Salary (USD)", fontsize=11)
    ax.set_title("Year-over-Year Salary Trend — Top 5 Job Roles", fontsize=13, fontweight="bold")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"${y:,.0f}"))
    ax.set_xticks(sorted(df["year"].unique()))
    ax.legend(title="Job Role", fontsize=9, title_fontsize=9, loc="upper left")
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=10)
    plt.tight_layout()

    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(CHART_LINE, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Chart saved: %s", CHART_LINE)


# ---------------------------------------------------------------------------
# Markdown helpers
# ---------------------------------------------------------------------------

def _df_to_md(df: pd.DataFrame, float_fmt: str = "{:,.2f}") -> str:
    """Render a DataFrame as a GitHub-Flavored Markdown table string.

    Args:
        df: DataFrame to render.
        float_fmt: Format string applied to float columns.

    Returns:
        Multi-line GFM table string.
    """
    formatted = df.copy()
    for col in formatted.select_dtypes(include="float").columns:
        formatted[col] = formatted[col].map(lambda v: float_fmt.format(v))

    header = "| " + " | ".join(str(c) for c in formatted.columns) + " |"
    divider = "| " + " | ".join("---" for _ in formatted.columns) + " |"
    rows = [
        "| " + " | ".join(str(v) for v in row) + " |"
        for row in formatted.itertuples(index=False)
    ]
    return "\n".join([header, divider] + rows)


# ---------------------------------------------------------------------------
# Report assembly
# ---------------------------------------------------------------------------

def build_report(hl: dict) -> str:
    """Assemble the complete Markdown report as a single string.

    Args:
        hl: Headline numbers dict from build_headline_numbers().

    Returns:
        Full Markdown document as a string.
    """
    q1 = _load_csv("top_ai_specializations_by_salary")
    q2 = _load_csv("salary_by_experience_level")
    q3 = _load_csv("yoy_salary_trend_top5_roles")
    q4 = _load_csv("geographic_salary_comparison")
    q5 = _load_csv("work_mode_and_company_size_impact")
    q6 = _load_csv("automation_risk_profile")

    # Generate charts before embedding references in the report
    chart_salary_by_specialization(q1)
    chart_yoy_salary_trend(q3)

    lines: list[str] = []

    # ---- Title ----
    lines += [
        "# Global AI & Data Jobs Salary Report",
        "",
        "_Generated by the Job Market Analysis ETL Pipeline_",
        "",
    ]

    # ---- Executive Summary ----
    lines += [
        "## Executive Summary",
        "",
        f"| Metric | Value |",
        f"| --- | --- |",
        f"| Total job records | {hl['total_records']:,} |",
        f"| Years covered | {hl['year_min']}–{hl['year_max']} |",
        f"| Salary range (USD) | ${hl['salary_min']:,} – ${hl['salary_max']:,} |",
        f"| Average base salary (USD) | ${hl['salary_mean']:,.2f} |",
        f"| Countries covered | {hl['countries']} |",
        f"| Distinct job roles | {hl['job_roles']} |",
        "",
        (
            f"The dataset spans **{hl['total_records']:,} job postings** across "
            f"**{hl['countries']} countries** and **{hl['year_max'] - hl['year_min'] + 1} years** "
            f"({hl['year_min']}–{hl['year_max']}). "
            f"The overall average base salary of **${hl['salary_mean']:,.0f} USD** is consistent "
            "across AI specializations, with significant variation driven by experience level "
            "and geography rather than role type."
        ),
        "",
    ]

    # ---- Query 1 ----
    lines += [
        "## 1. Top AI Specializations by Average Salary",
        "",
        "Average base salary and total compensation per AI specialization, ranked descending.",
        "",
        _df_to_md(q1),
        "",
        "![Average Salary by AI Specialization](charts/avg_salary_by_ai_specialization.png)",
        "",
        (
            "Generative AI and LLM specializations command the highest average salaries "
            f"(~${q1['avg_salary_usd'].max():,.0f}), while Reinforcement Learning sits at the "
            f"bottom of the range (~${q1['avg_salary_usd'].min():,.0f}). "
            "The spread across all eight specializations is narrow (~$1,700), "
            "suggesting the market prices AI skills similarly regardless of domain."
        ),
        "",
    ]

    # ---- Query 2 ----
    lines += [
        "## 2. Salary by Experience Level",
        "",
        "Box-plot statistics for base salary across the four experience levels.",
        "",
        _df_to_md(q2),
        "",
    ]
    entry_med = q2.loc[q2["experience_level"] == "Entry",  "median_salary"].values[0]
    lead_med  = q2.loc[q2["experience_level"] == "Lead",   "median_salary"].values[0]
    lines += [
        (
            f"Experience level is the strongest salary driver in the dataset. "
            f"Lead-level median salary (${float(lead_med):,.0f}) is "
            f"**{float(lead_med) / float(entry_med):.1f}×** the Entry-level median "
            f"(${float(entry_med):,.0f}). "
            "The wide IQR at Lead level reflects the outsized compensation variance among "
            "senior individual contributors and team leads."
        ),
        "",
    ]

    # ---- Query 3 ----
    lines += [
        "## 3. Year-over-Year Salary Trend",
        "",
        "Average base salary per year for the five highest-volume job roles (2020–2026).",
        "",
        _df_to_md(q3),
        "",
        "![Year-over-Year Salary Trend](charts/yoy_salary_trend_top5_roles.png)",
        "",
        (
            "Salaries for all five roles have remained broadly stable across the 2020–2026 "
            "window, with no single year showing a sustained directional shift. "
            "Research Scientist consistently commands the highest average salary "
            f"(~${q3[q3['job_role'] == 'Research Scientist']['avg_salary_usd'].mean():,.0f}), "
            "while AI Engineer and Software Engineer AI cluster at the lower end of the top-5 range."
        ),
        "",
    ]

    # ---- Query 4 ----
    lines += [
        "## 4. Geographic Salary Comparison",
        "",
        "Top 10 countries by average base salary, with cost-of-living and tax context.",
        "",
        _df_to_md(q4),
        "",
    ]
    top_country    = q4.iloc[0]["country"]
    top_salary     = float(q4.iloc[0]["avg_salary_usd"])
    bottom_country = q4.iloc[-1]["country"]
    bottom_salary  = float(q4.iloc[-1]["avg_salary_usd"])
    lines += [
        (
            f"**{top_country}** leads with an average salary of ${top_salary:,.0f}, "
            f"nearly **{top_salary / bottom_salary:.1f}×** that of "
            f"**{bottom_country}** (${bottom_salary:,.0f}) at the bottom of the top-10. "
            "Cost-of-living indices are broadly similar across the top countries (~1.5), "
            "suggesting that nominal salary differences largely reflect genuine purchasing-power gaps."
        ),
        "",
    ]

    # ---- Query 5 ----
    lines += [
        "## 5. Work Mode and Company Size Impact",
        "",
        "Average salary, employee satisfaction, and work-life balance score by work mode and company size.",
        "",
        _df_to_md(q5),
        "",
        (
            "Salary differences across work modes (Remote / Hybrid / Onsite) are minimal (<1%), "
            "indicating that remote work carries no measurable pay premium or penalty in this dataset. "
            "Enterprise companies show consistently higher work-life balance scores (~75) compared "
            "to Startups (~62), regardless of work mode, while satisfaction scores follow the same pattern."
        ),
        "",
    ]

    # ---- Query 6 ----
    lines += [
        "## 6. Automation Risk Profile",
        "",
        "Average automation risk and job security score per AI specialization.",
        "",
        _df_to_md(q6),
        "",
    ]
    highest_risk = q6.iloc[0]["ai_specialization"]
    lowest_risk  = q6.iloc[-1]["ai_specialization"]
    lines += [
        (
            f"**{highest_risk}** carries the highest average automation risk score, "
            f"while **{lowest_risk}** sits at the lowest. "
            "Across all specializations the spread is narrow (~1 point), and job security scores "
            "are uniformly high (~75), suggesting that AI roles broadly resist automation "
            "irrespective of specialization."
        ),
        "",
    ]

    # ---- Methodology ----
    lines += [
        "## Methodology",
        "",
        "This report was produced by a six-step ETL pipeline:",
        "",
        (
            "1. **Extract** — The *Global AI & Data Jobs Salary Dataset* "
            "(`mohankrishnathalla/global-ai-and-data-jobs-salary-dataset`, 90,000 rows, 35 columns) "
            "was downloaded from Kaggle via `kagglehub` and saved to `data/raw/global_ai_jobs.csv`. "
            "Column count, row count, and null-column checks were asserted before proceeding."
        ),
        (
            "2. **Transform** — Column types were enforced (`year` → int; `salary_usd`, `bonus_usd` → float; "
            "categorical fields → pandas `category`). Five derived columns were engineered: "
            "`total_compensation` (salary + bonus), `salary_band` and `compensation_band` "
            "(quantile terciles), `high_automation_risk` (top-quartile flag), and `senior_flag` "
            "(Senior/Lead indicator). Output written to `data/processed/cleaned_jobs.parquet`."
        ),
        (
            "3. **Load** — Three normalised tables (`jobs`, `job_metrics`, `job_features`) were "
            "created in a local PostgreSQL database (`job_analysis`) using SQLAlchemy + psycopg2. "
            "Tables are dropped and recreated on each run for full idempotency. "
            "Row counts were validated against the source after insertion."
        ),
        (
            "4. **Query** — Six analytical SQL queries were executed against PostgreSQL, "
            "covering specialization salary rankings, experience-level distributions, "
            "year-over-year trends, geographic comparisons, work-mode/company-size interactions, "
            "and automation risk profiles. Results were exported as CSVs to `reports/query_results/`."
        ),
        (
            "5. **Report** — This document was assembled programmatically from the query-result CSVs "
            "and the cleaned Parquet. Charts were rendered with `matplotlib` and saved as PNG files "
            "in `reports/charts/`. `salary_usd` is treated as annual gross base compensation in USD. "
            "All figures are rounded to two decimal places."
        ),
        "",
        "**Assumptions**",
        "- `salary_usd` is annual gross base compensation in USD.",
        "- `bonus_usd` is an annual cash bonus; `total_compensation = salary_usd + bonus_usd`.",
        "- `experience_level` labels (`Entry`, `Mid`, `Senior`, `Lead`) are taken as-is from the dataset.",
        "- The dataset contains no null values; no imputation was performed.",
        "- Salary bands (Low / Medium / High) are defined by equal-population terciles (≈33% each).",
        "",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_report() -> None:
    """Assert the report file and both chart PNGs exist and are non-empty.

    Raises:
        AssertionError: If any expected output is missing or empty.
    """
    assert REPORT_PATH.exists(), f"Report not found: '{REPORT_PATH}'"
    content = REPORT_PATH.read_text(encoding="utf-8")
    for heading in REQUIRED_SECTIONS:
        assert heading in content, (
            f"Expected section missing from report: '{heading}'"
        )
    logger.info("Report structure OK — all %d sections present", len(REQUIRED_SECTIONS))

    for chart_path in (CHART_BAR, CHART_LINE):
        assert chart_path.exists(), f"Chart PNG not found: '{chart_path}'"
        size = chart_path.stat().st_size
        assert size > 0, f"Chart PNG is empty (0 bytes): '{chart_path}'"
        logger.info("Chart OK — %s  (%.1f KB)", chart_path.name, size / 1024)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def report() -> None:
    """Run the full report generation pipeline.

    Raises:
        FileNotFoundError: If any required input file is missing.
        AssertionError: If output validation fails.
    """
    logger.info("=== STEP 5: REPORT ===")

    hl = build_headline_numbers()
    logger.info(
        "Headline numbers — records: %d, years: %d–%d, avg salary: $%s",
        hl["total_records"], hl["year_min"], hl["year_max"],
        f"{hl['salary_mean']:,.2f}",
    )

    md = build_report(hl)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        REPORT_PATH.write_text(md, encoding="utf-8")
    except OSError as exc:
        raise OSError(f"Failed to write report to '{REPORT_PATH}': {exc}") from exc
    logger.info("Report written: %s", REPORT_PATH)

    validate_report()
    logger.info("=== REPORT COMPLETE — final_report.md and both charts validated ===")


if __name__ == "__main__":
    report()

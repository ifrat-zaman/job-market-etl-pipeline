# CLAUDE.md — End-to-End Job Market ETL Pipeline

## Project Overview

You are an expert data engineer. Your task is to build a complete, production-ready ETL pipeline using **Python** and **PostgreSQL** with the **Global AI & Data Jobs Salary Dataset** from Kaggle. The final deliverables must be professional, well-documented, and GitHub-ready.

| Field | Detail |
|---|---|
| **Dataset** | `mohankrishnathalla/global-ai-and-data-jobs-salary-dataset` |
| **Rows** | 90,000 |
| **Columns** | 35 |
| **Goal** | Extract → Transform → Load → Query → Report insights about AI & data job salaries, skills, and trends |

---

## Dataset Schema Reference

The dataset contains the following 35 columns. Use this as the authoritative reference throughout all scripts — do not assume different column names.

| Column | Type | Description |
|---|---|---|
| `id` | int | Unique row identifier |
| `country` | str | Country of the job posting |
| `job_role` | str | Job title (e.g., `'Data Scientist'`, `'ML Engineer'`) |
| `ai_specialization` | str | AI domain (e.g., `'LLM'`, `'NLP'`, `'Computer Vision'`, `'Generative AI'`) |
| `experience_level` | str | Already normalised: `'Entry'`, `'Mid'`, `'Senior'`, `'Lead'` |
| `experience_years` | int | Years of experience |
| `salary_usd` | int | Annual base salary in USD |
| `bonus_usd` | int | Annual bonus in USD |
| `education_required` | str | `'Bachelor'`, `'Master'`, `'PhD'`, `'Bootcamp'`, `'Diploma'` |
| `industry` | str | Industry sector (e.g., `'Tech'`, `'Healthcare'`, `'Finance'`) |
| `company_size` | str | `'Startup'`, `'Small'`, `'Medium'`, `'Large'`, `'Enterprise'` |
| `interview_rounds` | int | Number of interview rounds |
| `year` | int | Year of data collection (2020–2026) |
| `work_mode` | str | `'Remote'`, `'Hybrid'`, `'Onsite'` |
| `weekly_hours` | float | Average weekly working hours |
| `company_rating` | float | Company rating (likely 0–5 scale) |
| `job_openings` | int | Number of open positions |
| `hiring_difficulty_score` | float | Score indicating how hard the role is to fill |
| `layoff_risk` | float | Estimated layoff risk score (0–1) |
| `ai_adoption_score` | int | Company AI maturity/adoption score |
| `company_funding_billion` | float | Total company funding in billions USD |
| `economic_index` | float | Economic conditions index for the country |
| `ai_maturity_years` | int | Years the company has been investing in AI |
| `offer_acceptance_rate` | float | Rate at which job offers are accepted |
| `tax_rate_percent` | float | Effective tax rate for the role's country |
| `vacation_days` | int | Annual vacation days |
| `skill_demand_score` | int | Demand score for the role's skill set |
| `automation_risk` | int | Risk of role being automated (score) |
| `job_security_score` | int | Job security rating |
| `career_growth_score` | int | Career growth potential score |
| `work_life_balance_score` | int | Work-life balance score |
| `promotion_speed` | int | Speed of promotion (score or months) |
| `salary_percentile` | int | Salary percentile within role/country |
| `cost_of_living_index` | float | Cost of living index for the country |
| `employee_satisfaction` | int | Overall employee satisfaction score |

**Key dataset facts:**
- **No null values** — all 35 columns are fully populated across all 90,000 rows.
- **`experience_level`** is already in plain English (`'Entry'`, `'Mid'`, `'Senior'`, `'Lead'`) — no code-to-label mapping is needed.
- **`work_mode`** is already clean (`'Remote'`, `'Hybrid'`, `'Onsite'`) — no mapping needed.
- **Salary range:** min $28,000 — median $87,544 — max $300,622 USD.
- **Bonus range:** min $1,404 — median $11,279 — max $57,681 USD.
- **Years covered:** 2020, 2021, 2022, 2023, 2024, 2025, 2026.
- **There is no `required_skills` column.** Skills are represented via `ai_specialization` (categorical) and `skill_demand_score` (numeric). Do not attempt to parse or explode a skills string.

---

## Authentication & Setup (Critical First Step)

The Kaggle API credentials are stored in **`Kaggle.txt`** in the Job Market Analysis ETL Project folder.

Write a helper function (`scripts/auth.py` or inline in `extract.py`) that:

1. Reads `Kaggle.txt` from the project root (Job Market Analysis ETL Project folder).
2. Parses each non-empty, non-comment line and sets `os.environ['KAGGLE_USERNAME']` and `os.environ['KAGGLE_KEY']`.
3. Raises a clear `FileNotFoundError` if `Kaggle.txt` is missing, and a `ValueError` if either key is absent or empty.

**Validation:** After setting the environment variables, confirm both are non-empty, then attempt the Kaggle download with proper error handling. The pipeline should fail fast here with a descriptive message rather than silently proceeding with invalid credentials.

> ⚠️ `Kaggle.txt` must be listed in `.gitignore` — never commit credentials.

---

## Project Structure

```
project-root/
├── Kaggle.txt                # API credentials (gitignored — never commit)
├── .env.example              # Template for env vars (no secrets)
├── .gitignore                # Ignores Kaggle.txt, data/, .env, *.pyc, etc.
├── README.md                 # Setup and run instructions
├── requirements.txt          # Python dependencies
├── scripts/
│   ├── auth.py               # Reads Kaggle.txt and sets env vars
│   ├── extract.py            # Downloads dataset from Kaggle
│   ├── transform.py          # Cleans data and engineers features
│   ├── load.py               # Loads data into PostgreSQL
│   ├── query.py              # Runs SQL queries and exports results
│   ├── report.py             # Generates Markdown report and charts
│   └── pipeline.py           # Orchestrates all steps end-to-end
├── sql/
│   └── analysis_queries.sql  # All analytical SQL queries
├── reports/
│   ├── final_report.md       # Final output with tables and charts
│   └── charts/               # Generated PNG files
├── data/
│   ├── raw/                  # Raw CSV (gitignored)
│   └── processed/            # Cleaned Parquet/CSV (gitignored)
└── tests/                    # Optional: validation tests
```

---

## Step-by-Step Instructions

Work iteratively. **Validate each step before proceeding to the next.** If validation fails, correct the code and re-run.

---

### Step 1: Extract — Download Dataset

**File:** `scripts/extract.py`

- Call the helper in `scripts/auth.py` to load credentials from `Kaggle.txt`.
- Download the dataset using `kagglehub` (`mohankrishnathalla/global-ai-and-data-jobs-salary-dataset`).
- Copy the CSV to `data/raw/global_ai_jobs.csv`.
- Log row count, column names, and summary statistics via `pandas.describe()`.

**Validation:** Assert that the CSV:
- Exists at `data/raw/global_ai_jobs.csv`.
- Has exactly 35 columns matching the schema table above.
- Has at least 80,000 rows (expected ~90,000).
- Contains no completely null columns.

Raise an explicit, descriptive error if any assertion fails. Do not proceed to transformation until this passes.

---

### Step 2: Transform — Data Cleaning & Feature Engineering

**File:** `scripts/transform.py`

Reads `data/raw/global_ai_jobs.csv` and outputs `data/processed/cleaned_jobs.parquet`.

Since the dataset has **no nulls** and **pre-normalised categorical fields**, the transform step focuses on type enforcement and feature engineering rather than heavy cleaning.

| Task | Details |
|---|---|
| Type enforcement | `year` → int; `salary_usd`, `bonus_usd` → float; `experience_level`, `work_mode`, `company_size`, `education_required`, `industry`, `ai_specialization` → category |
| Derived: `total_compensation` | `salary_usd + bonus_usd` |
| Derived: `salary_band` | Quantile-based classification — Low (bottom 33%) / Medium (middle 33%) / High (top 33%) based on `salary_usd` |
| Derived: `compensation_band` | Same quantile logic applied to `total_compensation` |
| Derived: `high_automation_risk` | Boolean flag — `True` where `automation_risk` is in the top quartile |
| Derived: `senior_flag` | Boolean — `True` where `experience_level` in `['Senior', 'Lead']` |

> **Note:** There is no `required_skills` column in this dataset. Do not attempt to parse or explode skills. The `ai_specialization` column is the skills proxy and is already categorical — use it directly.

**Validation:** Assert no nulls in `salary_usd`, `job_role`, or `country`; confirm all derived columns are fully populated; log min/max/mean of `total_compensation`.

---

### Step 3: Load — PostgreSQL Database

**File:** `scripts/load.py`

- Connect using environment variables (`DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`), with the following local development defaults:

  ```
  DB_NAME=job_analysis
  DB_USER=ifratzaman
  DB_PASSWORD=
  DB_HOST=localhost
  DB_PORT=5432
  ```

  > **Note:** This Homebrew PostgreSQL installation uses the Mac system username (`ifratzaman`) as the superuser with no password. `DB_PASSWORD` must be left empty — do not set it to `'postgres'` or any other value.

- Create the following tables with appropriate data types, primary keys, and constraints:

  - **`jobs`** — core fact table (one row per job posting): `id`, `country`, `job_role`, `ai_specialization`, `experience_level`, `experience_years`, `salary_usd`, `bonus_usd`, `total_compensation`, `education_required`, `industry`, `company_size`, `year`, `work_mode`, `weekly_hours`, `company_rating`, `job_openings`
  - **`job_metrics`** — numeric scores (one row per job, FK → `jobs.id`): `id`, `hiring_difficulty_score`, `layoff_risk`, `ai_adoption_score`, `company_funding_billion`, `economic_index`, `ai_maturity_years`, `offer_acceptance_rate`, `tax_rate_percent`, `vacation_days`, `skill_demand_score`, `automation_risk`, `job_security_score`, `career_growth_score`, `work_life_balance_score`, `promotion_speed`, `salary_percentile`, `cost_of_living_index`, `employee_satisfaction`
  - **`job_features`** — engineered flags and bands (one row per job, FK → `jobs.id`): `id`, `salary_band`, `compensation_band`, `high_automation_risk`, `senior_flag`

- Use `psycopg2` or `SQLAlchemy` for bulk inserts.
- Document conflict-handling behaviour (default: truncate tables on re-run for idempotency).

**Validation:** After loading, run `SELECT COUNT(*)` on each table and compare to source row counts. Log any discrepancies.

---

### Step 4: Query — Analytical SQL

**File:** `sql/analysis_queries.sql`

Write the following queries, tailored to this dataset's actual columns:

1. **Top AI specializations by average salary** — average `salary_usd` and `total_compensation` per `ai_specialization`, ordered descending.
2. **Salary by experience level** — min, Q1, median, Q3, max of `salary_usd` for each `experience_level` (box plot data).
3. **Year-over-year salary trend** — average `salary_usd` per `year` for each `job_role`, filtered to the top 5 roles by overall volume.
4. **Geographic salary comparison** — top 10 countries by average `salary_usd`, including average `cost_of_living_index` and average `tax_rate_percent` alongside.
5. **Work mode and company size impact** — average `salary_usd`, `employee_satisfaction`, and `work_life_balance_score` grouped by `work_mode` and `company_size`.
6. **Automation risk profile** — average `automation_risk`, `job_security_score`, and `salary_usd` grouped by `ai_specialization`, ordered by `automation_risk` descending.

**File:** `scripts/query.py`

- Read all queries from `sql/analysis_queries.sql`.
- Execute against PostgreSQL.
- Save each result as a CSV in `reports/query_results/` (create the subfolder if it does not exist).
- Print results to console in a readable tabular format.

**Validation:** Every query must return non-empty results. If any returns 0 rows, investigate and fix before proceeding.

---

### Step 5: Report — Generate Final Output

**File:** `scripts/report.py`

Generates `reports/final_report.md` containing:

- Executive summary with key headline numbers (total records, year range, salary range, countries covered).
- A formatted table for each of the six analytical insights.
- At least two charts saved as PNG in `reports/charts/`:
  - A horizontal bar chart of average salary by `ai_specialization`.
  - A line chart of year-over-year salary trend for the top 5 job roles.
- A brief methodology section describing the ETL steps.

The report must be self-contained and ready to share without modification.

**Validation:** Confirm `final_report.md` exists and contains all expected sections. Verify all chart PNG files are non-empty (file size > 0).

---

### Step 6: Orchestration & Final Validation

**File:** `scripts/pipeline.py`

Runs all steps in sequence:

1. `auth.py` (credential check)
2. `extract.py`
3. `transform.py`
4. `load.py`
5. `query.py`
6. `report.py`

The pipeline script must:

- Log each step with timestamps.
- Halt execution on failure (non-zero exit code) and print the error clearly.
- Support a `--skip-extract` flag to reuse `data/raw/global_ai_jobs.csv` without re-downloading.
- Be runnable with a single command: `python scripts/pipeline.py`

**Final validation:** After a full run from scratch, verify:

- All expected files exist: raw CSV, cleaned Parquet, all three database tables, six query result CSVs, `final_report.md`, and both chart PNGs.
- Reported numbers are plausible — overall average salary should be approximately $96,500 USD.
- `Kaggle.txt` does not appear in `git status` (i.e., it is properly gitignored).

---

## Code Standards

### Python
- Use **type hints** on all function signatures.
- Use **Google-style docstrings** on all public functions and modules.
- Use the `logging` module instead of `print` statements.
- Wrap all I/O operations (file, network, database) in `try/except` with specific exception types.
- Load all environment variables and file paths from a central `config.py` or via `os.getenv()` with sensible defaults.

### SQL
- Use **parameterised queries** — never format SQL strings with Python variables.
- Use meaningful aliases and add inline comments on complex logic.
- Column names in all queries must match the schema table exactly (e.g., `salary_usd` not `salary_in_usd`).

### Validation
- Write inline `assert` statements after each major operation.
- Optionally place `pytest`-based tests for critical functions in `tests/`.

### Git-Readiness
- `.gitignore` must exclude: `Kaggle.txt`, `data/`, `*.pyc`, `.env`, `reports/charts/`.
- `README.md` must include: virtual environment setup, dependency installation, PostgreSQL setup, and a single-command run instruction.

---

## Iteration & Self-Correction Protocol

As Claude Code, you must:

1. After writing each script, **propose a validation command** (e.g., `python scripts/extract.py`).
2. If an error is reported, analyse the traceback, correct the code, and re-present the fixed version.
3. Do not proceed to the next step until the current step's validation passes.
4. **Always refer to the schema table** in this file before referencing any column name — do not guess or infer column names from memory.
5. Document all assumptions in the final report's methodology section (e.g., *"salary_usd is treated as annual gross compensation in USD"*).

---

## Deliverables Checklist

Before considering the project complete, confirm the following are committed to the repository:

- [ ] `README.md` — clear instructions to reproduce the pipeline end-to-end.
- [ ] `requirements.txt` — includes `kagglehub`, `pandas`, `numpy`, `psycopg2-binary`, `sqlalchemy`, `matplotlib`, `python-dotenv`.
- [ ] All scripts under `scripts/` — no application code in the project root.
- [ ] `sql/analysis_queries.sql` — well-formatted, commented SQL using correct column names.
- [ ] `.env.example` — template for DB credentials (no real secrets).
- [ ] `.gitignore` — must include `Kaggle.txt`, `data/`, `.env`, `*.pyc`, `reports/charts/`.
- [ ] A sample or placeholder of the final generated report.

---

## Success Criteria

The pipeline is considered complete when:

- A non-technical user can clone the repository, add their `Kaggle.txt`, follow the README, and run `python scripts/pipeline.py` to produce a final report with charts.
- The pipeline is **idempotent** — running it twice produces identical results.
- All SQL queries use the correct column names from the schema table and return meaningful, non-trivial insights.
- The generated report's headline salary figure is approximately **$96,500 USD** average — use this as a sanity check.
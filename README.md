# Job Market Analysis ETL Pipeline

An end-to-end ETL pipeline that downloads the **Global AI & Data Jobs Salary Dataset** from Kaggle, loads it into PostgreSQL, runs six analytical queries, and produces a self-contained Markdown report with charts.

## Output

- `reports/final_report.md` — executive summary, six insight tables, two charts
- `reports/charts/` — PNG charts (gitignored; regenerated on each run)
- `reports/query_results/` — six CSV files, one per analytical query

---

## Prerequisites

| Requirement | Version |
| --- | --- |
| Python | 3.9 or later |
| PostgreSQL | 13 or later (Homebrew on macOS recommended) |
| Kaggle account | Required for dataset download |

---

## Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd "Job Market Analysis ETL Project"
```

### 2. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate      # macOS / Linux
# .venv\Scripts\activate       # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Add your Kaggle credentials

Create a file named `Kaggle.txt` in the project root (this file is gitignored and must never be committed):

```
Kaggle username: your_kaggle_username
Api token: your_kaggle_api_token
```

You can find your API token at <https://www.kaggle.com/settings> → **API** → **Create New Token**.

### 5. Configure PostgreSQL

**macOS Homebrew (default — no password required):**

```bash
brew install postgresql@14
brew services start postgresql@14
createdb job_analysis
```

The pipeline connects as your macOS system username with no password. No further configuration is needed.

**Other setups:** copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
# edit .env with your DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
```

Then load the `.env` before running:

```bash
export $(grep -v '^#' .env | xargs)
python3 scripts/pipeline.py
```

---

## Running the pipeline

### Full run (download + all steps)

```bash
python3 scripts/pipeline.py
```

### Skip re-downloading (reuse existing raw CSV)

```bash
python3 scripts/pipeline.py --skip-extract
```

### Run individual steps

```bash
python3 scripts/extract.py    # Step 1+2: download & validate
python3 scripts/transform.py  # Step 3: clean & feature-engineer
python3 scripts/load.py       # Step 4: load into PostgreSQL
python3 scripts/query.py      # Step 5: run analytical SQL
python3 scripts/report.py     # Step 6: generate report & charts
```

---

## Project structure

```
├── Kaggle.txt                     # API credentials (gitignored)
├── .env.example                   # DB credential template
├── .gitignore
├── README.md
├── requirements.txt
├── scripts/
│   ├── auth.py                    # Reads Kaggle.txt, sets env vars
│   ├── extract.py                 # Downloads dataset from Kaggle
│   ├── transform.py               # Cleans data, engineers features
│   ├── load.py                    # Loads into PostgreSQL
│   ├── query.py                   # Runs SQL, exports CSVs
│   ├── report.py                  # Generates report and charts
│   └── pipeline.py                # Orchestrates all steps
├── sql/
│   └── analysis_queries.sql       # Six analytical queries
├── data/
│   ├── raw/                       # global_ai_jobs.csv (gitignored)
│   └── processed/                 # cleaned_jobs.parquet (gitignored)
└── reports/
    ├── final_report.md            # Generated report
    ├── charts/                    # PNG charts (gitignored)
    └── query_results/             # Per-query CSVs
```

---

## Database schema

| Table | Rows | Description |
| --- | --- | --- |
| `jobs` | 90,000 | Core fact table — one row per job posting |
| `job_metrics` | 90,000 | Numeric scores (FK → `jobs.id`) |
| `job_features` | 90,000 | Engineered flags and bands (FK → `jobs.id`) |

---

## Analytical queries

| # | Name | Description |
| --- | --- | --- |
| 1 | `top_ai_specializations_by_salary` | Avg salary & total compensation by AI specialization |
| 2 | `salary_by_experience_level` | Box-plot stats (min/Q1/median/Q3/max) by experience level |
| 3 | `yoy_salary_trend_top5_roles` | Year-over-year avg salary for top 5 job roles |
| 4 | `geographic_salary_comparison` | Top 10 countries by avg salary with cost-of-living context |
| 5 | `work_mode_and_company_size_impact` | Salary, satisfaction, WLB by work mode × company size |
| 6 | `automation_risk_profile` | Automation risk, job security, salary by AI specialization |

---

## Idempotency

Running the pipeline twice produces identical results:

- **Extract:** overwrites `data/raw/global_ai_jobs.csv`
- **Transform:** overwrites `data/processed/cleaned_jobs.parquet`
- **Load:** drops and recreates all three tables before inserting
- **Query:** overwrites CSV files in `reports/query_results/`
- **Report:** overwrites `reports/final_report.md` and chart PNGs

---

## Success criteria

After a successful full run you should see:

```
PIPELINE COMPLETE
```

The average base salary in the report should be approximately **$96,500 USD**.

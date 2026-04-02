-- =============================================================
-- analysis_queries.sql
-- Analytical queries for the Global AI & Data Jobs dataset.
-- All column names match the authoritative schema in CLAUDE.md.
-- Tables: jobs, job_metrics, job_features
-- query.py splits this file on the "-- [query_N:" sentinel lines.
-- =============================================================


-- [query_1: top_ai_specializations_by_salary]
-- Average base salary and total compensation per AI specialisation,
-- ordered by average salary descending.
SELECT
    j.ai_specialization,
    ROUND(AVG(j.salary_usd)::NUMERIC,        2) AS avg_salary_usd,
    ROUND(AVG(j.total_compensation)::NUMERIC, 2) AS avg_total_compensation,
    COUNT(*)                                     AS job_count
FROM jobs j
GROUP BY j.ai_specialization
ORDER BY avg_salary_usd DESC;


-- [query_2: salary_by_experience_level]
-- Box-plot statistics (min, Q1, median, Q3, max) of salary_usd
-- for each experience level.
SELECT
    j.experience_level,
    ROUND(MIN(j.salary_usd)::NUMERIC,                                               2) AS min_salary,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY j.salary_usd)::NUMERIC,      2) AS q1_salary,
    ROUND(PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY j.salary_usd)::NUMERIC,      2) AS median_salary,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY j.salary_usd)::NUMERIC,      2) AS q3_salary,
    ROUND(MAX(j.salary_usd)::NUMERIC,                                               2) AS max_salary,
    COUNT(*)                                                                            AS job_count
FROM jobs j
GROUP BY j.experience_level
-- Logical sort order: Entry → Mid → Senior → Lead
ORDER BY CASE j.experience_level
    WHEN 'Entry'  THEN 1
    WHEN 'Mid'    THEN 2
    WHEN 'Senior' THEN 3
    WHEN 'Lead'   THEN 4
    ELSE 5
END;


-- [query_3: yoy_salary_trend_top5_roles]
-- Year-over-year average salary for the top 5 job roles by overall posting volume.
-- Uses a CTE to identify the top 5 roles, then pivots by year.
WITH top_roles AS (
    -- Identify the 5 most-posted job roles across all years
    SELECT   job_role
    FROM     jobs
    GROUP BY job_role
    ORDER BY COUNT(*) DESC
    LIMIT 5
)
SELECT
    j.job_role,
    j.year,
    ROUND(AVG(j.salary_usd)::NUMERIC, 2) AS avg_salary_usd,
    COUNT(*)                              AS job_count
FROM jobs j
JOIN top_roles tr ON tr.job_role = j.job_role
GROUP BY j.job_role, j.year
ORDER BY j.job_role, j.year;


-- [query_4: geographic_salary_comparison]
-- Top 10 countries by average base salary, with cost-of-living and tax context.
-- Joins jobs → job_metrics for the country-level indices.
SELECT
    j.country,
    ROUND(AVG(j.salary_usd)::NUMERIC,            2) AS avg_salary_usd,
    ROUND(AVG(jm.cost_of_living_index)::NUMERIC,  4) AS avg_cost_of_living_index,
    ROUND(AVG(jm.tax_rate_percent)::NUMERIC,      2) AS avg_tax_rate_percent,
    COUNT(*)                                         AS job_count
FROM jobs j
JOIN job_metrics jm ON jm.id = j.id
GROUP BY j.country
ORDER BY avg_salary_usd DESC
LIMIT 10;


-- [query_5: work_mode_and_company_size_impact]
-- Average salary, employee satisfaction, and work-life balance score
-- grouped by work mode and company size.
SELECT
    j.work_mode,
    j.company_size,
    ROUND(AVG(j.salary_usd)::NUMERIC,              2) AS avg_salary_usd,
    ROUND(AVG(jm.employee_satisfaction)::NUMERIC,  2) AS avg_employee_satisfaction,
    ROUND(AVG(jm.work_life_balance_score)::NUMERIC, 2) AS avg_work_life_balance_score,
    COUNT(*)                                           AS job_count
FROM jobs j
JOIN job_metrics jm ON jm.id = j.id
GROUP BY j.work_mode, j.company_size
-- Logical ordering: work_mode alphabetically, then company size by scale
ORDER BY j.work_mode, CASE j.company_size
    WHEN 'Startup'    THEN 1
    WHEN 'Small'      THEN 2
    WHEN 'Medium'     THEN 3
    WHEN 'Large'      THEN 4
    WHEN 'Enterprise' THEN 5
    ELSE 6
END;


-- [query_6: automation_risk_profile]
-- Average automation risk, job security score, and base salary
-- per AI specialisation, ordered by automation risk descending.
SELECT
    j.ai_specialization,
    ROUND(AVG(jm.automation_risk)::NUMERIC,    2) AS avg_automation_risk,
    ROUND(AVG(jm.job_security_score)::NUMERIC, 2) AS avg_job_security_score,
    ROUND(AVG(j.salary_usd)::NUMERIC,          2) AS avg_salary_usd,
    COUNT(*)                                      AS job_count
FROM jobs j
JOIN job_metrics jm ON jm.id = j.id
GROUP BY j.ai_specialization
ORDER BY avg_automation_risk DESC;

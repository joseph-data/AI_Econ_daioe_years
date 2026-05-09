# Yearly Employed Persons by Occupation and DAIOEs

<p align="left">
  <img src="logos/lab.svg" alt="AI-Econ Lab logo" width="200" height="">
</p>

## Overview

This repository contains the data pipeline for processing and mapping DAIOE (AI Occupational Exposure) metrics to the Swedish Standard Classification of Occupations (SSYK 2012) using data from Statistics Sweden (SCB). The goal is to evaluate AI's economic impact and labor market exposure across Swedish occupations over time — producing an interactive Shiny dashboard backed by a fully automated, daily-refreshed dataset.

---

## Architecture: Branch-Driven ETL Pipeline

Rather than a traditional directory-first layout, this repository uses a **branch-driven pipeline** where each branch represents a distinct stage in the ETL process, orchestrated by GitHub Actions. Data flows automatically and sequentially across branches:

```text
SCB API ──→ [scb_pull] ──→ [daioe_pull] ──→ [development] ──→ [main]
```

| Branch | Role | Trigger |
| --- | --- | --- |
| `scb_pull` | Data ingestion & aggregation from SCB | Daily cron / manual push |
| `daioe_pull` | DAIOE-SCB merge & exposure scoring | Automatic on `scb_pull` push |
| `development` | Analysis, notebooks, Shiny dashboard | Automatic on `daioe_pull` push |
| `main` | Stable production codebase | Manual |

---

## Branch Details

### 1. `scb_pull` — Data Ingestion & Aggregation

Pulls raw occupational labor statistics from the SCB API and builds a clean, hierarchically aggregated dataset.

**Workflow:** `.github/workflows/01_scb_pull_to_daioe_pull.yml`
Triggered daily at 00:00 UTC or manually. Runs scripts sequentially, then pushes output to `daioe_pull`.

**Scripts:**

- **`scripts/pull.py`** — Fetches data from four SSYK classification tables via `pyscbwrapper`. Downloads run in parallel (up to 8 workers) and output per-table `.parquet` files plus update logs.

    ```python
    tables = {
        "ssyk96_05_to_13": ("en", "AM", "AM0208", "AM0208E", "Yreg34"),
        "ssyk12_14_to_18": ("en", "AM", "AM0208", "AM0208E", "YREG51"),
        "ssyk12_19_to_21": ("en", "AM", "AM0208", "AM0208E", "YREG51N"),
        "ssyk12_20_to_24": ("en", "AM", "AM0208", "AM0208E", "YREG51BAS"),
    }
    ```

- **`scripts/merge.py`** — Combines the four SSYK12 parquet files, resolves duplicates (keeps newest record per identity), and tracks data provenance. Identity columns: `code`, `occupation`, `age`, `year`, `sex`.

- **`scripts/aggregate.py`** — Loads the combined dataset and the `structure_ssyk12.csv` name map, then produces a four-level SSYK hierarchy (SSYK4 → SSYK3 → SSYK2 → SSYK1), preserving `age`, `sex`, and `year` dimensions.

**Output:** `data/processed/ssyk12_aggregated_ssyk4_to_ssyk1.parquet`

---

### 2. `daioe_pull` — AI Exposure Merging

Joins the clean SCB aggregation with DAIOE metrics and produces exposure scores for every SSYK level.

**Workflow:** `.github/workflows/02_daioe_pull_to_development.yml`
Triggered automatically by a push from `scb_pull`. Runs `scripts/main.py`, then commits output to `development`.

**Scripts:**

- **`scripts/main.py`** — CLI entry point; orchestrates the pipeline via `pipeline.py`.

- **`scripts/pipeline.py`** — Core pipeline logic. Key steps:
    1. Load DAIOE metrics CSV from the external repo `joseph-data/07_translate_ssyk`
    2. Load SCB parquet from `daioe_pull`
    3. Filter military occupations (codes starting with `"0"`)
    4. Build SSYK code hierarchy (`code_1` … `code_4`) and map age bands to career-stage labels
    5. Join DAIOE with SCB SSYK4 employment counts
    6. Aggregate to all SSYK levels with employment-weighted and unweighted DAIOE averages
    7. Compute percentile ranks within each `(year, level)` group
    8. Convert percentiles to 1–5 exposure buckets
    9. Final join with the full SCB dataset

    **Key functions:**

    | Function | Purpose |
    | --- | --- |
    | `add_ssyk_hierarchy()` | Creates `code_1`–`code_4` from 4-digit SSYK |
    | `extend_daioe_years_to_match_scb()` | Forward-fills DAIOE to latest SCB year |
    | `scb_level4_counts()` | Filters SCB to SSYK4 and sums employment |
    | `add_age_group()` | Maps raw age bands to career-stage labels |
    | `aggregate_daioe_level()` | Weighted/unweighted DAIOE per SSYK level |
    | `add_exposure_levels_from_weighted_percentiles()` | Percentile ranks → 1–5 exposure buckets |
    | `build_all_levels()` | Concatenates aggregations across all SSYK levels |
    | `build_pipeline()` | Assembles the full lazy Polars pipeline |

**Output:** `data/daioe_scb_years_all_levels.parquet`

---

### 3. `development` — Analysis & Dashboard

The active development hub. Receives the merged dataset automatically, then applies feature engineering and serves an interactive Shiny dashboard.

**Notebooks:**

- **`notebooks/data_edits.ipynb`** — Computes 1-, 3-, and 5-year employment changes (absolute and percent) and reorders columns. Outputs `data/processed_data.parquet`.
- **`notebooks/visuals.ipynb`** — Exploratory Plotly visualizations and metric distribution analysis.

**Modules:**

- **`src/data_edits.py`** — Standalone callable version of `data_edits.ipynb`.
- **`src/fcts.py`** — Utility helpers (`inspect_lazy()` for efficient LazyFrame inspection).
- **`setup.py`** — Dashboard setup: loads `processed_data.parquet` as a global LazyFrame, pre-computes UI choices (LEVELS, SEXES, AGES, YEARS, METRICS), and provides helpers for column name formatting, KPI display, Great Tables HTML rendering, and data export.
- **`app.py`** — Shiny Express dashboard with three panels:
  - **Occupation View** — KPI cards (employment, percentile rank, 1/3/5-yr changes), dual-axis employment + exposure trend line, AI capability radar chart, detailed metrics table
  - **Sector View** — Comparative metrics and peer benchmarking across occupations
  - **Time Series** — Broad trend analysis with year-over-year comparisons
- **`_brand.yml`** — Shiny UI theme (Nunito Sans body, Montserrat headings, Fira Code mono; navy/violet palette).

---

### 4. `main` — Production

Stable, production-ready codebase. Serves as the source of truth for pipeline scripting logic. All feature work branches off `development` and merges back there before eventual promotion to `main`.

---

## Data Flow

```text
SCB API
  │
  ▼ pull.py  (parallel fetch × 4 tables)
data/raw/<table_id>.parquet
  │
  ▼ merge.py  (deduplicate, resolve conflicts)
data/processed/ssyk12_combined_cleaned.parquet
  │
  ▼ aggregate.py  (SSYK4→1 aggregation + name mapping)
data/processed/ssyk12_aggregated_ssyk4_to_ssyk1.parquet
  │                                       [GitHub artifact → daioe_pull branch]
  ▼
scripts/main.py + pipeline.py  (join DAIOE, compute exposure scores)
  │
data/daioe_scb_years_all_levels.parquet  [→ development branch]
  │
  ▼ data_edits.py  (1/3/5-yr change features)
data/processed_data.parquet
  │
  ▼ setup.py + app.py
Shiny Dashboard
```

---

## Data Files

| File | Branch | Description |
| --- | --- | --- |
| `data/raw/<table_id>.parquet` | `scb_pull` | Raw SCB occupational data (4 SSYK tables, 2005–2024) |
| `data/processed/ssyk12_combined_cleaned.parquet` | `scb_pull` | Merged & deduplicated SCB data |
| `data/processed/ssyk12_aggregated_ssyk4_to_ssyk1.parquet` | `scb_pull` / `daioe_pull` | SSYK hierarchy aggregation |
| `data/daioe_scb_years_all_levels.parquet` | `daioe_pull` / `development` | Full DAIOE + SCB dataset at all SSYK levels |
| `data/processed_data.parquet` | `development` | Final dataset with employment change features |
| `structure_ssyk12.csv` | `scb_pull` | SSYK code ↔ occupation name mapping |

---

## Tech Stack

| Layer | Libraries |
| --- | --- |
| Data processing | [Polars](https://pola.rs/) >= 1.38, PyArrow, Pandas |
| SCB API | [pyscbwrapper](https://github.com/kirajano/pyscbwrapper) |
| Visualization | Plotly Express, Great Tables |
| Dashboard | [Shiny for Python](https://shiny.posit.co/py/) (Express + theme), shinywidgets, faicons |
| CI/CD | GitHub Actions |
| Python | >= 3.14 |

---

## Key Concepts

**SSYK 2012** — Swedish Standard Classification of Occupations, organized in a four-level hierarchy (SSYK1 major group → SSYK4 unit group).

**DAIOE** — AI Occupational Exposure index. Measures the degree to which specific AI capabilities (e.g., NLP, vision, reasoning) are relevant to each occupation. Sourced from `joseph-data/07_translate_ssyk`.

**Exposure scoring** — DAIOE values are ranked as percentiles within each `(year, SSYK level)` cohort, then bucketed into a 1–5 ordinal scale for interpretability.

**Weighted aggregation** — When rolling SSYK4 metrics up to SSYK3/2/1, DAIOE values are weighted by employment counts to preserve sector composition.

**Career stage labels** — SCB age bands (e.g., `"25-29"`) are mapped to interpretable labels (`"Early Career 2 (25-29)"`) across five stages: Early Career 1, Early Career 2, Mid-Career, Experienced, Senior.

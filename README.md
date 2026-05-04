# DAIOE × SCB (SSYK 2012) Pipeline

## Overview

This repository contains the data pipeline for processing and mapping DAIOE (AI Occupational Exposure) metrics to the Swedish Standard Classification of Occupations (SSYK 2012) using data from Statistics Sweden (SCB). The goal of this pipeline is to evaluate the economic impact and exposure of AI across different sectors of the Swedish labor market over time.

## Project Organization & Branch Workflow

This repository replaces a traditional directory-first organization with a branch-driven automated data pipeline. Each branch represents a specific stage in the ETL (Extract, Transform, Load) process, powered by GitHub Actions. Data progresses sequentially across the following branches:

1.  **`scb_pull` (Data Ingestion & Aggregation)**
    *   **Purpose**: The initial entry point for pulling raw labor data from Statistics Sweden.
    *   **Workflow Actions**: Triggered daily via cron (or manually on push), it runs `scripts/pull.py`, `scripts/merge.py`, and `scripts/aggregate.py`.
    *   **Output**: Generates `data/processed/ssyk12_aggregated_ssyk4_to_ssyk1.parquet` (and relevant logs) and automatically pushes the result to the `daioe_pull` branch.

2.  **`daioe_pull` (AI Exposure Merging)**
    *   **Purpose**: The intermediate staging area for joining the pre-processed labor statistics with the DAIOE metrics.
    *   **Workflow Actions**: Triggered automatically by the incoming dataset push from `scb_pull`. It executes `scripts/main.py`.
    *   **Output**: Generates the fully combined analytical dataset, `data/daioe_scb_years_all_levels.parquet`, and automatically pushes it to the `development` branch.

3.  **`development` (Active Development)**
    *   **Purpose**: The primary hub for iterative development and analysis.
    *   **State**: Receives the finalized `daioe_scb_years_all_levels.parquet` dataset automatically. All new feature code, analysis logic, or bug fixes should be branched off and merged back into here.

4.  **`main` (Production)**
    *   **Purpose**: Contains the stable, production-ready codebase. Considered the ultimate source of truth for the project's scripting logic.


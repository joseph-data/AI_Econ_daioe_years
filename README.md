# DAIOE × SCB (SSYK 2012) Aggregation Pipeline

This project builds a multi‑level SSYK aggregation of DAIOE indicators joined to SCB employment counts. The pipeline stays lazy in Polars and writes a single Parquet output with SSYK1–SSYK4 levels, simple averages, weighted averages, and percentiles.

## What It Produces
A Parquet file with:
- `level` ∈ {`SSYK1`, `SSYK2`, `SSYK3`, `SSYK4`}
- `ssyk_code` (1–4 digits)
- `year`
- `weight_sum` (SCB employment counts)
- `daioe_*_avg` (simple means)
- `daioe_*_wavg` (employment‑weighted means)
- `pctl_*` (within‑year percentiles for avg/wavg metrics)

Default output path:
- `data/daioe_scb_all_levels.parquet`

## Project Layout
- `scripts/main.py` — entry point
- `scripts/pipeline.py` — reusable pipeline steps
- `data/` — output location

## Setup
Create the environment (example using `uv`):

```bash
uv sync
```

## Run

```bash
uv run ./scripts/main.py
```

## Configuration
The default data sources are defined in `scripts/pipeline.py` inside `default_config()`.
Update these if the data lives elsewhere:
- `daioe_source` — DAIOE translated SSYK2012 CSV (remote)
- `scb_source` — SCB Parquet (remote/local path)

Point `scb_source` to a valid raw GitHub URL or a local parquet file.

## Notes
- The pipeline keeps everything lazy until `sink_parquet()` for efficiency.
- By default, rows with `code_1 == "0"` (military/army) are dropped.

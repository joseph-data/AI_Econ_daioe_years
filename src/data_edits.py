"""Build processed unemployment data from the raw parquet source.

This script mirrors the transformation pipeline in `data_edits.ipynb`.
"""

import polars as pl
from pathlib import Path

try:
    from scripts.fcts import inspect_lazy
except ModuleNotFoundError:
    # Supports running as `python scripts/data_edits.py`.
    from fcts import inspect_lazy

# ---------------------------------------------------
# Preliminary
# ---------------------------------------------------

KEYS = ["level", "ssyk_code", "age", "sex"]
JOIN_COLS = ["year", "level", "ssyk_code", "age", "sex"]
FIRST_COLS = [
    "year",
    "level",
    "ssyk_code",
    "occupation",
    "sex",
    "age",
    "age_group",
    "count",
    "weight_sum",
    "unemp_count",
    "chg_1y",
    "chg_3y",
    "chg_5y",
    "pct_chg_1y",
    "pct_chg_3y",
    "pct_chg_5y",
]


def compute_change_features(pulled_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Compute 1y/3y/5y absolute and percent changes for unemployment count."""
    return (
        pulled_lf
        .group_by([*KEYS, "year"])
        .agg(pl.col("count").sum().alias("unemp_count"))
        .sort([*KEYS, "year"])
        .with_columns(
            [
                (pl.col("unemp_count") - pl.col("unemp_count").shift(1).over(KEYS)).alias("chg_1y"),
                (pl.col("unemp_count") - pl.col("unemp_count").shift(3).over(KEYS)).alias("chg_3y"),
                (pl.col("unemp_count") - pl.col("unemp_count").shift(5).over(KEYS)).alias("chg_5y"),
                ((pl.col("unemp_count") / pl.col("unemp_count").shift(1).over(KEYS) - 1) * 100).alias("pct_chg_1y"),
                ((pl.col("unemp_count") / pl.col("unemp_count").shift(3).over(KEYS) - 1) * 100).alias("pct_chg_3y"),
                ((pl.col("unemp_count") / pl.col("unemp_count").shift(5).over(KEYS) - 1) * 100).alias("pct_chg_5y"),
            ]
        )
    )


def merge_and_reorder(
    pulled_lf: pl.LazyFrame,
    pulled_with_changes: pl.LazyFrame,
) -> pl.LazyFrame:
    """Join change features onto source data and reorder output columns."""
    processed_lf = pulled_lf.join(
        pulled_with_changes,
        left_on=JOIN_COLS,
        right_on=JOIN_COLS,
        how="left",
    )

    schema_cols = list(processed_lf.collect_schema())
    missing_first_cols = [col for col in FIRST_COLS if col not in schema_cols]
    if missing_first_cols:
        raise ValueError(f"Columns missing from processed_lf: {missing_first_cols}")

    other_cols = [col for col in schema_cols if col not in FIRST_COLS]
    return (
        processed_lf
        .select(FIRST_COLS + other_cols)
        .sort(JOIN_COLS, nulls_last=True, descending=True)
    )


def main() -> None:
    """Run the processing."""
    root = Path(__file__).resolve().parents[1]
    data_path = root / "data"
    pulled_data = data_path / "daioe_scb_years_all_levels.parquet"
    processed_data = data_path / "processed_data.parquet"

    pulled_lf = pl.scan_parquet(pulled_data)

    # Read-data sanity check.
    pulled_lf.collect_schema()

    pulled_with_changes = compute_change_features(pulled_lf)
    inspect_lazy(pulled_with_changes)

    processed_lf_reorder_cols = merge_and_reorder(pulled_lf, pulled_with_changes)
    inspect_lazy(processed_lf_reorder_cols)

    processed_lf_reorder_cols.sink_parquet(processed_data)
    print(f"Wrote processed data: {processed_data}")


if __name__ == "__main__":
    main()

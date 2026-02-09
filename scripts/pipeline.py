"""
DAIOE × SCB (SSYK 2012) multi-level aggregation pipeline.

This module contains the reusable pipeline steps. See main.py for the CLI entrypoint.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import polars as pl


# ----------------------------
# Configuration
# ----------------------------

@dataclass(frozen=True)
class PipelineConfig:
    data_dir: Path
    daioe_source: str
    scb_source: str
    out_file: Path
    min_year: int = 2014
    drop_code1_zero: bool = True
    add_percentiles: bool = True
    pct_scale: int = 100
    descending: bool = False
    daioe_prefix: str = "daioe_"


def default_config(root: Path | None = None) -> PipelineConfig:
    root = root or Path.cwd().resolve()
    data_dir = root / "data"

    daioe_source = (
        "https://raw.githubusercontent.com/joseph-data/07_translate_ssyk/main/"
        "03_translated_files/daioe_ssyk2012_translated.csv"
    )

    scb_source = str(
        data_dir / "processed" / "ssyk12_aggregated_ssyk4_to_ssyk1.parquet"
    )

    out_file = data_dir / "daioe_scb_all_levels.parquet"

    return PipelineConfig(
        data_dir=data_dir,
        daioe_source=daioe_source,
        scb_source=scb_source,
        out_file=out_file,
    )


# ----------------------------
# Utilities
# ----------------------------

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def inspect_lazy(lf: pl.LazyFrame, name: str = "LazyFrame") -> None:
    """Lightweight inspect: prints row/col counts without collecting full data."""
    n_rows = lf.select(pl.len()).collect().item()
    n_cols = len(lf.collect_schema())
    print(f"[{name}] Rows: {n_rows:,} | Columns: {n_cols}")


def add_ssyk_hierarchy(
    daioe_lf: pl.LazyFrame,
    code4_col: str = "ssyk2012_4",
    min_year: int = 2014,
) -> pl.LazyFrame:
    """Create SSYK hierarchy code_1..code_4 from a 4-digit SSYK code column."""
    return (
        daioe_lf
        .with_columns(pl.col(code4_col).cast(pl.Utf8))
        .with_columns(
            pl.col(code4_col).str.slice(0, 1).alias("code_1"),
            pl.col(code4_col).str.slice(0, 2).alias("code_2"),
            pl.col(code4_col).str.slice(0, 3).alias("code_3"),
            pl.col(code4_col).str.slice(0, 4).alias("code_4"),
        )
        # Drop original SSYK columns if your file has multiple ssyk2012_* fields
        .drop(pl.col(r"^ssyk2012.*$"))
        # Keep only years that exist in SCB regime
        .filter(pl.col("year") >= min_year)
    )


def extend_daioe_years_to_match_scb(
    daioe_lf: pl.LazyFrame,
    scb_lf: pl.LazyFrame,
    year_col: str = "year",
) -> pl.LazyFrame:
    """
    Extend DAIOE forward in time if SCB has newer years.
    Replicates the last DAIOE year values across missing years.
    """
    daioe_max = daioe_lf.select(pl.max(year_col)).collect().item()
    scb_max = scb_lf.select(pl.max(year_col)).collect().item()

    missing = list(range(daioe_max + 1, scb_max + 1))
    if not missing:
        return daioe_lf

    year_dtype = daioe_lf.collect_schema()[year_col]

    years_lf = (
        pl.LazyFrame({year_col: missing})
        .with_columns(pl.col(year_col).cast(year_dtype))
    )

    replicated = (
        daioe_lf
        .filter(pl.col(year_col) == daioe_max)
        .drop(year_col)
        .join(years_lf, how="cross")
        .select(daioe_lf.collect_schema().names())  # keep same column order
    )

    return pl.concat([daioe_lf, replicated], how="vertical")


def scb_level4_counts(
    scb_lf: pl.LazyFrame,
    code_col: str = "ssyk_code",
    year_col: str = "year",
    count_col: str = "count",
) -> pl.LazyFrame:
    """Filter SCB to SSYK4 (len==4) and sum counts per (year, ssyk_code)."""
    return (
        scb_lf
        .with_columns(pl.col(code_col).cast(pl.Utf8))
        .filter(pl.col(code_col).str.len_chars() == 4)
        .group_by([year_col, code_col])
        .agg(pl.col(count_col).sum().alias("total_count"))
    )


def collect_daioe_columns(lf: pl.LazyFrame, prefix: str = "daioe_") -> list[str]:
    return [c for c in lf.collect_schema().names() if c.startswith(prefix)]


def aggregate_daioe_level(
    lf: pl.LazyFrame,
    daioe_cols: Iterable[str],
    code_col: str,
    level_label: str,
    weight_col: str = "total_count",
    prefix: str = "daioe_",
    add_percentiles: bool = True,
    pct_scale: int = 100,
    descending: bool = False,
) -> pl.LazyFrame:
    """
    Aggregate DAIOE columns to a given SSYK level and optionally add percentile ranks
    within each (year, level) for both simple and employment-weighted averages.

    Notes
    -----
    - Percentiles are computed in a version-compatible way (no Expr.rank(pct=...)).
    - Percentiles are scaled to 0..pct_scale (default 0..100).
    """
    w = pl.col(weight_col)

    out = (
        lf
        .group_by(["year", code_col])
        .agg(
            w.sum().alias("weight_sum"),
            pl.col(list(daioe_cols)).mean().name.suffix("_avg"),
            ((pl.col(list(daioe_cols)) * w).sum() / w.sum()).name.suffix("_wavg"),
        )
        .with_columns(pl.lit(level_label).alias("level"))
        .rename({code_col: "ssyk_code"})
    )

    if not add_percentiles:
        return out

    group_keys = ["year", "level"]

    rank_expr = (
        pl.col(f"^{prefix}.*_(avg|wavg)$")
        .rank(method="average", descending=descending)
        .over(group_keys)
    )
    n_expr = pl.len().over(group_keys)

    # 0..1 inclusive percentiles: (rank-1)/(n-1), then scale
    return out.with_columns(
        (
            pl.when(n_expr > 1)
            .then((rank_expr - 1) / (n_expr - 1))
            .otherwise(0.0)
            * pct_scale
        ).name.prefix("pctl_")
    )


def build_all_levels(
    daioe_scb_joined: pl.LazyFrame,
    daioe_cols: Iterable[str],
    add_percentiles: bool = True,
    pct_scale: int = 100,
    descending: bool = False,
) -> pl.LazyFrame:
    """Aggregate SSYK4/3/2/1 and concatenate to a single LazyFrame."""
    levels = (
        ("code_4", "SSYK4"),
        ("code_3", "SSYK3"),
        ("code_2", "SSYK2"),
        ("code_1", "SSYK1"),
    )

    aggregated = [
        aggregate_daioe_level(
            daioe_scb_joined,
            daioe_cols=daioe_cols,
            code_col=col,
            level_label=label,
            add_percentiles=add_percentiles,
            pct_scale=pct_scale,
            descending=descending,
        )
        for col, label in levels
    ]

    return pl.concat(aggregated, how="vertical")


# ----------------------------
# Pipeline assembly
# ----------------------------

def build_pipeline(config: PipelineConfig) -> pl.LazyFrame:
    """Build the full LazyFrame pipeline without executing it."""
    # 1) Load
    daioe_lf = pl.scan_csv(config.daioe_source)
    scb_lf = pl.scan_parquet(config.scb_source)

    # 2) Prepare DAIOE codes (code_1..code_4)
    daioe_lf = add_ssyk_hierarchy(daioe_lf, min_year=config.min_year)

    # 3) SCB counts at SSYK4
    scb_lv4 = scb_level4_counts(scb_lf)

    # 4) Extend DAIOE years to match SCB max year
    daioe_lf = extend_daioe_years_to_match_scb(daioe_lf, scb_lv4)

    # 5) Join DAIOE with SCB counts (left)
    daioe_scb = (
        daioe_lf
        .join(
            scb_lv4,
            left_on=["year", "code_4"],
            right_on=["year", "ssyk_code"],
            how="left",
        )
    )

    # Optional: drop military/army etc.
    if config.drop_code1_zero:
        daioe_scb = daioe_scb.filter(pl.col("code_1") != "0")

    # Pre-compute DAIOE columns once for reuse
    daioe_cols = collect_daioe_columns(daioe_scb, prefix=config.daioe_prefix)

    # 6) Aggregate all levels
    daioe_all_levels = (
        build_all_levels(
            daioe_scb,
            daioe_cols=daioe_cols,
            add_percentiles=config.add_percentiles,
            pct_scale=config.pct_scale,
            descending=config.descending,
        )
        .sort(["level", "year", "ssyk_code"])
    )

    return daioe_all_levels

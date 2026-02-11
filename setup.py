from pathlib import Path
import polars as pl

root = Path.cwd().resolve()
data_dir = root / "data"

df = pl.scan_parquet(data_dir / "daioe_scb_years_all_levels.parquet")

print(df.collect_schema())

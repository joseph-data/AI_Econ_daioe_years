"""
DAIOE × SCB (SSYK 2012) multi-level aggregation pipeline.

Entry point that wires config + pipeline and writes the Parquet output.
"""

from pipeline import (
    build_pipeline,
    default_config,
    ensure_dir,
    inspect_lazy,
)


def main() -> None:
    print("== DAIOE × SCB SSYK12 pipeline ==")

    config = default_config()
    ensure_dir(config.data_dir)

    lf = build_pipeline(config)

    # Set to True if you want a quick row/column count check (this triggers execution).
    inspect = True
    if inspect:
        inspect_lazy(lf, "daioe_all_levels")

    print(f"Writing parquet -> {config.out_file}")
    lf.sink_parquet(config.out_file)
    print("Done.")


if __name__ == "__main__":
    main()

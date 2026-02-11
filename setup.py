from __future__ import annotations

from pathlib import Path
import polars as pl

DATA_PATH = (
    Path.cwd().resolve() / "data" / "daioe_scb_years_all_levels.parquet"
)

lf = pl.scan_parquet(DATA_PATH)

# Small cache of unique values for UI choices (collected once at startup)
LEVELS = (
    lf.select(pl.col("level").unique().sort())
    .collect()
    .to_series()
    .to_list()
)

SEXES = (
    lf.select(pl.col("sex").unique().sort())
    .collect()
    .to_series()
    .to_list()
)

AGES = (
    lf.select(pl.col("age").unique().sort())
    .collect()
    .to_series()
    .to_list()
)

YEARS = (
    lf.select(pl.col("year").unique().sort())
    .collect()
    .to_series()
    .to_list()
)

YEAR_MIN, YEAR_MAX = min(YEARS), max(YEARS)

#df.collect_schema()

METRICS: dict[str, str] = {
    "daioe_allapps": "📚 All Applications",
    "daioe_stratgames": "♟️ Strategy Games",
    "daioe_videogames": "🎮 Video Games (Real-Time)",
    "daioe_imgrec": "🖼️ Image Recognition",
    "daioe_imgcompr": "🧩 Image Comprehension",
    "daioe_imggen": "🎨 Image Generation",
    "daioe_readcompr": "📖 Reading Comprehension",
    "daioe_lngmod": "✍️ Language Modeling",
    "daioe_translat": "🌐 Translation",
    "daioe_speechrec": "🎙️ Speech Recognition",
    "daioe_genai": "🧠 Generative AI",
}





# EXPOSURE_LABELS: dict[int, str] = {
#     1: "Very low",
#     2: "Low",
#     3: "Medium",
#     4: "High",
#     5: "Very high",
# }

# SEX_CHOICES: dict[str, str] = {
#     "women": "Women",
#     "men": "Men",
# }

# LEVEL_CHOICES: dict[str, str] = {
#     "SSYK1": "Level 1 (1-digit)",
#     "SSYK2": "Level 2 (2-digit)",
#     "SSYK3": "Level 3 (3-digit)",
#     "SSYK4": "Level 4 (4-digit)",

# }



# df2 = df.limit(20).collect(
# )

from pathlib import Path
import polars as pl

# ---------------------------------------------------
# Data Preliminaries
# ---------------------------------------------------

DATA_PATH = (
    Path.cwd().resolve() / "data" / "daioe_scb_years_all_levels.parquet"
)

lf = pl.scan_parquet(DATA_PATH)

lf.collect_schema()


# ---------------------------------------------------
# Defining Input Values
# ---------------------------------------------------

# 1. SSYK12 Levels

LEVELS = (
    lf.select(pl.col("level").unique().sort())
    .collect()
    .to_series()
    .to_list()
)

# 2. Men and Women

SEXES = (
    lf.select(pl.col("sex").unique().sort())
    .collect()
    .to_series()
    .to_list()
)

# 3. Age groupings

AGE_ORDER = [
    "Early Career 1 (16-24)",
    "Early Career 2 (25-29)",
    "Developing (30-34)",
    "Mid-Career 1 (35-39)",
    "Mid-Career 1 (40-44)",
    "Mid-Career 2 (45-49)",
    "Senior (50+)",
]

present = (
    lf.select(pl.col("age_group").unique())
      .collect()
      .to_series()
      .to_list()
)

AGES = [x for x in AGE_ORDER if x in present]


YEARS = (
    lf.select(pl.col("year").unique().sort())
    .collect()
    .to_series()
    .to_list()
)

# 4. Years from the dataset

YEAR_MIN, YEAR_MAX = min(YEARS), max(YEARS)

# 5. AI Sub-Indexes

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

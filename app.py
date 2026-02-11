import polars as pl
import pandas as pd
import plotly.express as px

from shiny.express import ui, render, input
from shiny import reactive

import re


# -----------------------------
# Data (lazy)
# -----------------------------
DATA_PATH = Path.cwd().resolve() / "data" / "daioe_scb_years_all_levels.parquet"



# -----------------------------
# Helpers
# -----------------------------
METRICS = [
    "allapps",
    "stratgames",
    "videogames",
    "imgrec",
    "imgcompr",
    "imggen",
    "readcompr",
    "lngmod",
    "translat",
    "speechrec",
    "genai",
]


def metric_cols(metric: str, agg: str):
    """
    metric: one of METRICS
    agg: "avg" or "wavg"
    """
    value_col = f"daioe_{metric}_{agg}"
    pctl_col = f"pctl_daioe_{metric}_{agg}"
    bin_col = f"daioe_{metric}_Level_Exposure"
    return value_col, pctl_col, bin_col


def normalize_index(df: pl.DataFrame, group_cols: list[str], y_col: str, base_year: int):
    """
    Adds an 'index' column: y / y(base_year) within group_cols.
    If base_year is missing for some group, index will be null for that group.
    """
    base = (
        df.filter(pl.col("year") == base_year)
        .select(group_cols + [pl.col(y_col).alias("base_y")])
    )
    out = (
        df.join(base, on=group_cols, how="left")
        .with_columns((pl.col(y_col) / pl.col("base_y")).alias("index"))
    )
    return out


def safe_contains(col: str, pattern: str):
    """Case-insensitive contains filter. If pattern empty, returns True."""
    if not pattern:
        return pl.lit(True)
    # Escape regex meta so user text doesn't break it
    pat = re.escape(pattern)
    return pl.col(col).str.to_lowercase().str.contains(pat.lower())


# -----------------------------
# UI
# -----------------------------
ui.page_opts(title="DAIOE × SCB Explorer", fillable=True)

with ui.navset_bar(title="DAIOE × SCB Explorer"):

    with ui.nav_panel("Trends"):
        with ui.sidebar(open="desktop"):

            ui.h4("Core switches")
            ui.input_select("level", "SSYK level", choices=LEVELS, selected=LEVELS[0] if LEVELS else None)
            ui.input_select("metric", "AI metric", choices=METRICS, selected="genai")
            ui.input_radio_buttons("agg", "Aggregation", choices=["avg", "wavg"], selected="wavg", inline=True)

            ui.input_radio_buttons(
                "grouping",
                "Group exposure by",
                choices={
                    "bin": "Exposure bins (Level_Exposure)",
                    "pctl": "Percentiles (pctl_*)",
                },
                selected="bin",
            )

            ui.hr()

            ui.h4("Population filters")
            ui.input_radio_buttons(
                "sex",
                "Sex",
                choices=["all"] + SEXES,
                selected="all",
                inline=True,
            )
            ui.input_checkbox_group(
                "ages",
                "Age",
                choices=AGES,
                selected=AGES,  # default: all
            )
            ui.input_text("occupation_q", "Occupation search", placeholder="e.g. engineers, nurses, sales...")

            ui.hr()

            ui.h4("Time & view")
            ui.input_slider("baseline_year", "Baseline year (index)", YEAR_MIN, YEAR_MAX, value=min(2022, YEAR_MAX))
            ui.input_radio_buttons(
                "view_mode",
                "Show",
                choices={"index": "Index (relative to baseline)", "raw": "Raw employment"},
                selected="index",
                inline=True,
            )

            ui.hr()
            ui.h4("Table")
            ui.input_slider("top_n", "Top occupations in table", 10, 200, value=50)

        # Main layout
        with ui.layout_column_wrap(width=1, fill=True):
            ui.h3("A) Employment trend (by exposure group)")
            ui.output_ui("trend_caption")
            ui.output_plot("trend_plot")  # plotly figure

        with ui.layout_column_wrap(width=1/2, fill=True):
            with ui.card():
                ui.card_header("C) Exposure composition (simple vs employment-weighted)")
                ui.output_plot("composition_plot")
            with ui.card():
                ui.card_header("D) Employment share by exposure group")
                ui.output_plot("share_plot")

        with ui.card():
            ui.card_header("G) Drilldown table (occupations)")
            ui.output_data_frame("occ_table")

    with ui.nav_panel("Distribution"):
        with ui.sidebar(open="desktop"):
            ui.input_select("level_d", "SSYK level", choices=LEVELS, selected=LEVELS[0] if LEVELS else None)
            ui.input_select("metric_d", "AI metric", choices=METRICS, selected="genai")
            ui.input_radio_buttons("agg_d", "Aggregation", choices=["avg", "wavg"], selected="wavg", inline=True)
            ui.input_slider("year_d", "Year", YEAR_MIN, YEAR_MAX, value=YEAR_MAX)
            ui.input_radio_buttons("sex_d", "Sex", choices=["all"] + SEXES, selected="all", inline=True)
            ui.input_checkbox_group("ages_d", "Age", choices=AGES, selected=AGES)

        with ui.layout_column_wrap(width=1/2, fill=True):
            with ui.card():
                ui.card_header("E) Exposure distribution (box plot)")
                ui.output_plot("dist_plot")
            with ui.card():
                ui.card_header("F) Heatmap (year × exposure group)")
                ui.output_plot("heatmap_plot")


# -----------------------------
# Reactive data pipeline (Trends tab)
# -----------------------------
@reactive.calc
def filtered_lf() -> pl.LazyFrame:
    level = input.level()
    metric = input.metric()
    agg = input.agg()
    value_col, pctl_col, bin_col = metric_cols(metric, agg)

    sex = input.sex()
    ages_sel = input.ages()
    occ_q = input.occupation_q()

    base = lf.filter(pl.col("level") == level)

    if sex != "all":
        base = base.filter(pl.col("sex") == sex)

    if ages_sel:
        base = base.filter(pl.col("age").is_in(ages_sel))

    base = base.filter(safe_contains("occupation", occ_q))

    # Keep only needed columns for speed
    base = base.select(
        [
            "year",
            "age",
            "sex",
            "ssyk_code",
            "occupation",
            "count",
            "weight_sum",
            pl.col(value_col).alias("exposure_value"),
            pl.col(pctl_col).alias("exposure_pctl"),
            pl.col(bin_col).alias("exposure_bin"),
        ]
    )

    return base


@reactive.calc
def trend_df() -> pl.DataFrame:
    grouping = input.grouping()
    baseline_year = int(input.baseline_year())

    # Aggregate employment by year × age × exposure group
    base = filtered_lf()

    if grouping == "bin":
        grp = ["year", "age", "exposure_bin"]
    else:
        # percentiles: bucket into 5 groups for plotting (0–20, 20–40, ...)
        base = base.with_columns(
            (pl.col("exposure_pctl") // 20).cast(pl.Int32).clip(0, 4).alias("exposure_bin")
        )
        grp = ["year", "age", "exposure_bin"]

    df_agg = (
        base.group_by(grp)
        .agg(
            pl.col("count").sum().alias("employment"),
        )
        .collect()
    )

    df_agg = normalize_index(df_agg, group_cols=["age", "exposure_bin"], y_col="employment", base_year=baseline_year)
    return df_agg


# UI
ui.output_text("trend_caption")

# Server
@render.text
def trend_caption():
    return (
        f"Level={input.level()}, metric=daioe_{input.metric()}_{input.agg()}, "
        f"grouping={'Level_Exposure' if input.grouping()=='bin' else 'percentile buckets'}, "
        f"sex={input.sex()}, baseline={input.baseline_year()}."
    )



@render.plot
def trend_plot():
    dfp = trend_df().to_pandas()
    y = "index" if input.view_mode() == "index" else "employment"

    fig = px.line(
        dfp,
        x="year",
        y=y,
        color="exposure_bin",
        facet_col="age",
        facet_col_wrap=4,
        markers=True,
        title="Employment over time by age × exposure group",
    )
    fig.update_layout(legend_title_text="Exposure group")
    return fig


# -----------------------------
# Context plots (Trends tab)
# -----------------------------
@reactive.calc
def composition_df() -> pl.DataFrame:
    # Simple mean vs employment-weighted mean exposure over time
    base = filtered_lf()

    dfc = (
        base.group_by("year")
        .agg(
            pl.col("exposure_value").mean().alias("simple_mean"),
            (pl.col("exposure_value") * pl.col("weight_sum")).sum().alias("num"),
            pl.col("weight_sum").sum().alias("den"),
        )
        .with_columns((pl.col("num") / pl.col("den")).alias("employment_weighted_mean"))
        .select(["year", "simple_mean", "employment_weighted_mean"])
        .collect()
    )
    return dfc


@render.plot
def composition_plot():
    dfc = composition_df().to_pandas()
    df_long = dfc.melt(id_vars=["year"], var_name="series", value_name="value")

    fig = px.line(
        df_long,
        x="year",
        y="value",
        color="series",
        markers=True,
        title="Exposure composition over time",
    )
    fig.update_layout(legend_title_text="")
    return fig


@reactive.calc
def share_df() -> pl.DataFrame:
    # Employment share by exposure group (stacked area)
    base = filtered_lf()

    if input.grouping() == "bin":
        grp = ["year", "exposure_bin"]
        b = base
    else:
        b = base.with_columns((pl.col("exposure_pctl") // 20).cast(pl.Int32).clip(0, 4).alias("exposure_bin"))
        grp = ["year", "exposure_bin"]

    df_s = (
        b.group_by(grp)
        .agg(pl.col("count").sum().alias("employment"))
        .with_columns(
            (pl.col("employment") / pl.col("employment").sum().over("year")).alias("share")
        )
        .collect()
    )
    return df_s


@render.plot
def share_plot():
    dfs = share_df().to_pandas()
    fig = px.area(
        dfs,
        x="year",
        y="share",
        color="exposure_bin",
        title="Employment share by exposure group",
        groupnorm=None,
    )
    fig.update_layout(legend_title_text="Exposure group")
    return fig


# -----------------------------
# Drilldown table (Trends tab)
# -----------------------------
@reactive.calc
def table_df() -> pl.DataFrame:
    # Show top occupations by total employment in current filter
    top_n = int(input.top_n())
    base = filtered_lf()

    dft = (
        base.group_by(["ssyk_code", "occupation"])
        .agg(
            pl.col("count").sum().alias("employment_total"),
            pl.col("exposure_value").mean().alias("exposure_mean"),
        )
        .sort("employment_total", descending=True)
        .head(top_n)
        .collect()
    )
    return dft


@render.data_frame
def occ_table():
    dft = table_df().to_pandas()
    # Shiny renders a nice interactive grid from pandas
    return render.DataGrid(dft)


# -----------------------------
# Distribution tab (box + heatmap)
# -----------------------------
@reactive.calc
def dist_filtered() -> pl.DataFrame:
    level = input.level_d()
    metric = input.metric_d()
    agg = input.agg_d()
    year = int(input.year_d())
    sex = input.sex_d()
    ages_sel = input.ages_d()

    value_col, pctl_col, bin_col = metric_cols(metric, agg)

    base = lf.filter(pl.col("level") == level).filter(pl.col("year") == year)

    if sex != "all":
        base = base.filter(pl.col("sex") == sex)
    if ages_sel:
        base = base.filter(pl.col("age").is_in(ages_sel))

    # keep exposure + employment for distribution
    return (
        base.select(
            [
                "ssyk_code",
                "occupation",
                "age",
                "sex",
                "count",
                pl.col(value_col).alias("exposure_value"),
                pl.col(pctl_col).alias("exposure_pctl"),
                pl.col(bin_col).alias("exposure_bin"),
            ]
        ).collect()
    )


@render.plot
def dist_plot():
    dfd = dist_filtered().to_pandas()
    fig = px.box(
        dfd,
        x="exposure_bin",
        y="exposure_value",
        points="all",
        title="Exposure distribution by exposure bin",
    )
    fig.update_layout(xaxis_title="Exposure bin", yaxis_title="Exposure value")
    return fig


@reactive.calc
def heatmap_df() -> pl.DataFrame:
    # heatmap across all years: year × exposure_bin by employment share
    level = input.level_d()
    metric = input.metric_d()
    agg = input.agg_d()
    sex = input.sex_d()
    ages_sel = input.ages_d()

    value_col, pctl_col, bin_col = metric_cols(metric, agg)

    base = lf.filter(pl.col("level") == level)

    if sex != "all":
        base = base.filter(pl.col("sex") == sex)
    if ages_sel:
        base = base.filter(pl.col("age").is_in(ages_sel))

    hm = (
        base.group_by(["year", pl.col(bin_col).alias("exposure_bin")])
        .agg(pl.col("count").sum().alias("employment"))
        .with_columns(
            (pl.col("employment") / pl.col("employment").sum().over("year")).alias("share")
        )
        .collect()
    )
    return hm


@render.plot
def heatmap_plot():
    hmd = heatmap_df().to_pandas()
    fig = px.density_heatmap(
        hmd,
        x="year",
        y="exposure_bin",
        z="share",
        histfunc="avg",
        title="Employment share heatmap (year × exposure bin)",
    )
    fig.update_layout(xaxis_title="Year", yaxis_title="Exposure bin")
    return fig

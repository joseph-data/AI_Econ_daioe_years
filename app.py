import polars as pl
import plotly.express as px
from shiny import reactive
from shiny.express import input, render, ui
from shinywidgets import render_widget

from setup import (
    AGES,
    LEVELS,
    METRICS,
    SEXES,
    YEAR_MAX,
    YEAR_MIN,
    apply_plot_style,
    as_great_table_html,
    download_extension,
    download_media_type,
    empty_figure,
    export_filtered_data,
    lf,
)

BRAND = {
    "primary": "#441170",
    "secondary": "#2C054E",
    "accent": "#12CDB3",
    "background": "#F4F6F9",
    "surface": "#FFFFFF",
    "text": "#1F2D3D",
}

SCORE_TYPES: dict[str, str] = {
    "avg": "Average",
    "wavg": "Weighted average",
    "pctl": "Percentile (weighted average)",
}

AGE_GROUPS = AGES
BROAD_GROUP_LABELS = {
    "1": "1 Managers",
    "2": "2 Professionals",
    "3": "3 Technicians and Associate Professionals",
    "4": "4 Clerical Support Workers",
    "5": "5 Services and Sales Workers",
    "6": "6 Skilled Agricultural, Forestry and Fishery Workers",
    "7": "7 Craft and Related Trades Workers",
    "8": "8 Plant and Machine Operators and Assemblers",
    "9": "9 Elementary Occupations",
}
PAPER_COLORS = [
    "#5BB0F0",
    "#ED6A5A",
    "#7A5AA6",
    "#C2A83E",
    "#5CB85C",
    "#E84A9B",
    "#3E7BFA",
]
SOFTWARE_ENGINEER_CODE = "2512"
SOFTWARE_ENGINEER_LABEL = "Software- and system developers"

ui.page_opts(
    title="AI Exposure Dashboard",
    theme=ui.Theme.from_brand(__file__),
    fillable=True,
)

ui.head_content(
    ui.tags.style(
        f"""
        body {{
            background: {BRAND["background"]};
            color: {BRAND["text"]};
            font-family: "Inter", "Nunito Sans", "Segoe UI", sans-serif;
        }}
        .card {{
            border: 1px solid #E6E8EE !important;
            border-radius: 0.8rem !important;
            box-shadow: 0 1px 4px rgba(0, 0, 0, 0.04);
        }}
        .nav-pills .nav-link.active {{
            background-color: {BRAND["primary"]} !important;
        }}
        """
    )
)

with ui.sidebar(position="left", bg=BRAND["surface"]):
    ui.h5("Filters", style=f"color: {BRAND['primary']}; font-weight: 700;")
    ui.input_select("level", "SSYK level", choices=LEVELS, selected="SSYK4")
    ui.input_selectize(
        "occupation_search",
        "Search occupation title",
        choices={"__all__": "All occupations (all levels)"},
        selected="__all__",
        multiple=False,
    )
    ui.input_select("metric", "AI capability", choices=METRICS, selected="daioe_genai")
    ui.input_radio_buttons(
        "score_type",
        "Score type",
        choices=SCORE_TYPES,
        selected="wavg",
        inline=False,
    )
    ui.input_selectize("sex", "Sex", choices=SEXES, selected=SEXES, multiple=True)
    ui.input_selectize("age", "Age group", choices=AGE_GROUPS, selected=AGE_GROUPS, multiple=True)
    ui.input_slider(
        "year_range",
        "Year range",
        min=YEAR_MIN,
        max=YEAR_MAX,
        value=(YEAR_MIN, YEAR_MAX),
        sep="",
    )


@reactive.calc
def metric_col() -> str:
    base = input.metric()
    score_type = input.score_type()
    if score_type == "avg":
        return f"{base}_avg"
    if score_type == "wavg":
        return f"{base}_wavg"
    return f"pctl_{base}_wavg"


@reactive.calc
def score_axis_label() -> str:
    return SCORE_TYPES.get(input.score_type(), "Score")


def weighted_avg_expr(col: str) -> pl.Expr:
    return (pl.col(col) * pl.col("count")).sum() / pl.col("count").sum()


def apply_paper_style(fig):
    fig.update_layout(
        template="simple_white",
        font=dict(family="Times New Roman, serif", size=12, color="#303030"),
        margin=dict(l=60, r=20, t=30, b=55),
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.98,
            xanchor="left",
            x=1.02,
            font=dict(size=10),
            bgcolor="rgba(255,255,255,0.7)",
        ),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="#E1E1E1",
        griddash="dash",
        linecolor="#909090",
        zeroline=False,
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="#E1E1E1",
        griddash="dash",
        linecolor="#909090",
        zeroline=False,
    )
    return fig


@reactive.calc
def base_filtered_lf() -> pl.LazyFrame:
    selected_sex = list(input.sex()) if input.sex() else SEXES
    selected_age = list(input.age()) if input.age() else AGE_GROUPS
    year_start, year_end = input.year_range()

    return lf.filter(
        pl.col("sex").is_in(selected_sex),
        pl.col("age_group").is_in(selected_age),
        pl.col("year").is_between(year_start, year_end),
    )


@reactive.calc
def filtered_lf() -> pl.LazyFrame:
    selected_level = input.level() or "SSYK4"
    return base_filtered_lf().filter(pl.col("level") == selected_level)


@reactive.calc
def occupation_search_options() -> pl.DataFrame:
    return (
        base_filtered_lf()
        .select(["level", "ssyk_code", "occupation"])
        .filter(pl.col("ssyk_code").is_not_null(), pl.col("occupation").is_not_null())
        .with_columns(pl.col("occupation").fill_null("Unknown"))
        .unique()
        .sort(["level", "occupation", "ssyk_code"])
        .collect()
    )


@reactive.effect
def _sync_occupation_search_choices() -> None:
    df = occupation_search_options()
    choices: dict[str, str] = {"__all__": "All occupations (all levels)"}

    for level, code, occupation in df.iter_rows():
        key = f"{level}::{code}"
        choices[key] = f"[{level}] {occupation} ({code})"

    with reactive.isolate():
        selected = input.occupation_search() or "__all__"

    if selected not in choices:
        selected = "__all__"

    ui.update_selectize(
        "occupation_search",
        choices=choices,
        selected=selected,
        server=True,
    )


@reactive.calc
def selected_occupation_parts() -> tuple[str, str] | None:
    key = input.occupation_search()
    if not key or key == "__all__":
        return None
    if "::" not in key:
        return None
    level, code = str(key).split("::", 1)
    if not level or not code:
        return None
    return level, code


@reactive.calc
def selected_occupation_level() -> str | None:
    parts = selected_occupation_parts()
    if parts is None:
        return None
    return parts[0]


@reactive.calc
def selected_occupation_code() -> str | None:
    parts = selected_occupation_parts()
    if parts is None:
        return None
    return parts[1]


@reactive.calc
def selected_occupation_label() -> str:
    parts = selected_occupation_parts()
    if parts is None:
        return "All occupations"
    level, code = parts

    df = (
        occupation_search_options()
        .filter(pl.col("level") == level, pl.col("ssyk_code") == code)
        .select("occupation")
    )
    if df.is_empty():
        return f"[{level}] {code}"
    return f"[{level}] {df['occupation'][0]} ({code})"


@reactive.calc
def trend_data() -> pl.DataFrame:
    col = metric_col()
    return (
        filtered_lf()
        .group_by("year")
        .agg(
            pl.col("count").sum().alias("workers"),
            weighted_avg_expr(col).alias("score"),
        )
        .sort("year")
        .collect()
    )


@reactive.calc
def software_engineer_trend_data() -> pl.DataFrame:
    col = metric_col()
    ssyk4_lf = base_filtered_lf().filter(pl.col("level") == "SSYK4")

    software_df = (
        ssyk4_lf
        .filter(pl.col("ssyk_code") == SOFTWARE_ENGINEER_CODE)
        .group_by("year")
        .agg(weighted_avg_expr(col).alias("score"))
        .collect()
        .with_columns(pl.lit("Software engineers").alias("series"))
    )

    overall_df = (
        ssyk4_lf
        .group_by("year")
        .agg(weighted_avg_expr(col).alias("score"))
        .collect()
        .with_columns(pl.lit("All SSYK4 occupations").alias("series"))
    )

    if software_df.is_empty() and overall_df.is_empty():
        return pl.DataFrame()

    return pl.concat([software_df, overall_df], how="vertical").sort(["series", "year"])


@reactive.calc
def software_engineer_end_year_summary() -> dict[str, float | int | None]:
    col = metric_col()
    _, year_end = input.year_range()
    ssyk4_lf = base_filtered_lf().filter(pl.col("level") == "SSYK4")

    peers = (
        ssyk4_lf
        .filter(pl.col("year") == year_end)
        .group_by(["ssyk_code", "occupation"])
        .agg(
            pl.col("count").sum().alias("workers"),
            weighted_avg_expr(col).alias("score"),
        )
        .sort("score", descending=True)
        .collect()
    )
    if peers.is_empty():
        return {"score": None, "rank": None, "total": None, "gap": None}

    ranked = peers.with_columns(
        pl.col("score").rank("ordinal", descending=True).cast(pl.Int64).alias("rank"),
    )
    software_row = ranked.filter(pl.col("ssyk_code") == SOFTWARE_ENGINEER_CODE)
    if software_row.is_empty():
        return {"score": None, "rank": None, "total": ranked.height, "gap": None}

    level_avg = (
        ssyk4_lf
        .filter(pl.col("year") == year_end)
        .select(weighted_avg_expr(col).alias("avg_score"))
        .collect()
    )
    avg_score = float(level_avg["avg_score"][0]) if level_avg.height else 0.0
    software_score = float(software_row["score"][0])

    return {
        "score": software_score,
        "rank": int(software_row["rank"][0]),
        "total": ranked.height,
        "gap": software_score - avg_score,
    }


@reactive.calc
def end_year_occupation_scores() -> pl.DataFrame:
    col = metric_col()
    _, year_end = input.year_range()
    return (
        filtered_lf()
        .filter(pl.col("year") == year_end)
        .group_by(["ssyk_code", "occupation"])
        .agg(
            pl.col("count").sum().alias("workers"),
            weighted_avg_expr(col).alias("score"),
        )
        .filter(pl.col("ssyk_code").is_not_null())
        .with_columns(pl.col("occupation").fill_null("Unknown"))
        .sort("score")
        .collect()
    )


@reactive.calc
def selected_occupation_trend_data() -> pl.DataFrame:
    parts = selected_occupation_parts()
    if parts is None:
        return pl.DataFrame()
    level, code = parts

    col = metric_col()
    level_lf = base_filtered_lf().filter(pl.col("level") == level)
    occ_df = (
        level_lf
        .filter(pl.col("ssyk_code") == code)
        .group_by("year")
        .agg(weighted_avg_expr(col).alias("score"))
        .collect()
        .with_columns(pl.lit("Selected occupation").alias("series"))
    )
    lvl_df = (
        level_lf
        .group_by("year")
        .agg(weighted_avg_expr(col).alias("score"))
        .collect()
        .with_columns(pl.lit("Level average").alias("series"))
    )
    if occ_df.is_empty():
        return pl.DataFrame()
    return pl.concat([occ_df, lvl_df], how="vertical").sort(["series", "year"])


@reactive.calc
def selected_occupation_end_year_summary() -> dict[str, float | int | None]:
    parts = selected_occupation_parts()
    if parts is None:
        return {"score": None, "rank": None, "total": None, "gap": None}
    level, code = parts

    col = metric_col()
    _, year_end = input.year_range()
    level_lf = base_filtered_lf().filter(pl.col("level") == level)

    peers = (
        level_lf
        .filter(pl.col("year") == year_end)
        .group_by(["ssyk_code", "occupation"])
        .agg(
            pl.col("count").sum().alias("workers"),
            weighted_avg_expr(col).alias("score"),
        )
        .with_columns(pl.col("occupation").fill_null("Unknown"))
        .sort("score", descending=True)
        .collect()
    )
    if peers.is_empty():
        return {"score": None, "rank": None, "total": None, "gap": None}

    ranked = peers.with_columns(
        pl.col("score").rank("ordinal", descending=True).cast(pl.Int64).alias("rank"),
    )
    row = ranked.filter(pl.col("ssyk_code") == code)
    if row.is_empty():
        return {"score": None, "rank": None, "total": ranked.height, "gap": None}

    level_avg = (
        level_lf
        .filter(pl.col("year") == year_end)
        .select(weighted_avg_expr(col).alias("avg_score"))
        .collect()
    )
    avg_score = float(level_avg["avg_score"][0]) if level_avg.height else 0.0
    score = float(row["score"][0])

    return {
        "score": score,
        "rank": int(row["rank"][0]),
        "total": ranked.height,
        "gap": score - avg_score,
    }


@reactive.calc
def representative_occupations() -> pl.DataFrame:
    df = end_year_occupation_scores()
    if df.is_empty():
        return pl.DataFrame()

    n = df.height
    quantiles = [0.0, 0.10, 0.25, 0.50, 0.75, 0.90, 1.0]
    idxs = []
    for q in quantiles:
        idx = int(round(q * (n - 1)))
        idx = max(0, min(n - 1, idx))
        idxs.append(idx)

    idxs = sorted(set(idxs))
    reps = df[idxs].with_columns(
        (pl.col("ssyk_code") + " " + pl.col("occupation")).alias("series")
    )
    return reps.select(["ssyk_code", "series"])


@reactive.calc
def trajectories_data() -> pl.DataFrame:
    reps = representative_occupations()
    if reps.is_empty():
        return pl.DataFrame()

    codes = reps["ssyk_code"].to_list()
    col = metric_col()

    series_df = (
        filtered_lf()
        .filter(pl.col("ssyk_code").is_in(codes))
        .group_by(["year", "ssyk_code"])
        .agg(weighted_avg_expr(col).alias("score"))
        .collect()
        .join(reps, on="ssyk_code", how="left")
        .sort(["series", "year"])
    )
    return series_df


@reactive.calc
def broad_group_box_data() -> pl.DataFrame:
    df = end_year_occupation_scores()
    if df.is_empty():
        return pl.DataFrame()

    out = (
        df.with_columns(pl.col("ssyk_code").str.slice(0, 1).alias("broad_digit"))
        .filter(pl.col("broad_digit").is_in(list(BROAD_GROUP_LABELS.keys())))
        .with_columns(
            pl.col("broad_digit").replace_strict(BROAD_GROUP_LABELS).alias("broad_group")
        )
        .sort("broad_group")
    )
    return out


with ui.navset_pill(id="tab"):
    with ui.nav_panel("Visuals", value="visuals"):
        with ui.card(full_screen=True):
            ui.card_header("Search result: occupation visual")

            @render.text
            def selected_occupation_caption():
                if selected_occupation_code() is None:
                    return "Select an occupation title in the sidebar search box."
                return f"Selected occupation: {selected_occupation_label()}"

            with ui.layout_columns(col_widths=[4, 4, 4], gap="0.75rem"):
                with ui.value_box(theme="light"):
                    "End-year score"

                    @render.text
                    def selected_occ_score_kpi():
                        s = selected_occupation_end_year_summary()["score"]
                        if s is None:
                            return "No data"
                        return f"{float(s):.3f}"

                with ui.value_box(theme="light"):
                    "Rank within selected level"

                    @render.text
                    def selected_occ_rank_kpi():
                        summary = selected_occupation_end_year_summary()
                        if summary["rank"] is None:
                            return "No data"
                        return f"{int(summary['rank'])}/{int(summary['total'])}"

                with ui.value_box(theme="light"):
                    "Gap vs level average"

                    @render.text
                    def selected_occ_gap_kpi():
                        gap = selected_occupation_end_year_summary()["gap"]
                        if gap is None:
                            return "No data"
                        return f"{float(gap):+.3f}"

            @render_widget
            def selected_occupation_plot():
                df = selected_occupation_trend_data().to_pandas()
                if df.empty:
                    return empty_figure("Search and select one occupation to show its trend.", BRAND)

                fig = px.line(
                    df,
                    x="year",
                    y="score",
                    color="series",
                    markers=True,
                    color_discrete_map={
                        "Selected occupation": "#D9534F",
                        "Level average": "#4A90E2",
                    },
                )
                fig.update_traces(line=dict(width=2.2))
                fig.update_layout(
                    xaxis_title="Year",
                    yaxis_title=score_axis_label(),
                    legend_title="",
                )
                return apply_paper_style(fig)

        with ui.card(full_screen=True):
            ui.card_header(f"Software engineers focus ({SOFTWARE_ENGINEER_CODE}: {SOFTWARE_ENGINEER_LABEL})")

            with ui.layout_columns(col_widths=[4, 4, 4], gap="0.75rem"):
                with ui.value_box(theme="light"):
                    "End-year score"

                    @render.text
                    def software_score_kpi():
                        s = software_engineer_end_year_summary()["score"]
                        if s is None:
                            return "No data"
                        return f"{float(s):.3f}"

                with ui.value_box(theme="light"):
                    "Rank within SSYK4"

                    @render.text
                    def software_rank_kpi():
                        summary = software_engineer_end_year_summary()
                        if summary["rank"] is None:
                            return "No data"
                        return f"{int(summary['rank'])}/{int(summary['total'])}"

                with ui.value_box(theme="light"):
                    "Gap vs SSYK4 average"

                    @render.text
                    def software_gap_kpi():
                        gap = software_engineer_end_year_summary()["gap"]
                        if gap is None:
                            return "No data"
                        return f"{float(gap):+.3f}"

            @render_widget
            def software_trend_plot():
                df = software_engineer_trend_data().to_pandas()
                if df.empty:
                    return empty_figure("No software engineer data available for current filters.", BRAND)

                fig = px.line(
                    df,
                    x="year",
                    y="score",
                    color="series",
                    markers=True,
                    color_discrete_map={
                        "Software engineers": "#D9534F",
                        "All SSYK4 occupations": "#4A90E2",
                    },
                )
                fig.update_traces(line=dict(width=2.4))
                fig.update_layout(
                    xaxis_title="Year",
                    yaxis_title=score_axis_label(),
                    legend_title="",
                )
                return apply_paper_style(fig)

        with ui.layout_columns(col_widths=[6, 6], gap="1rem"):
            with ui.card(full_screen=True):
                ui.card_header("Figure-style trajectories for selected occupations")

                @render_widget
                def trajectories_plot():
                    df = trajectories_data().to_pandas()
                    if df.empty:
                        return empty_figure("No data available for current filters.", BRAND)

                    fig = px.line(
                        df,
                        x="year",
                        y="score",
                        color="series",
                        markers=False,
                        color_discrete_sequence=PAPER_COLORS,
                    )
                    fig.update_traces(line=dict(width=2))
                    fig.update_layout(
                        xaxis_title="Year",
                        yaxis_title=score_axis_label(),
                    )
                    return apply_paper_style(fig)

            with ui.card(full_screen=True):
                ui.card_header("AI exposure by broad occupation group")

                @render_widget
                def broad_group_boxplot():
                    df = broad_group_box_data().to_pandas()
                    if df.empty:
                        return empty_figure("No occupation distribution available.", BRAND)

                    order = list(BROAD_GROUP_LABELS.values())
                    fig = px.box(
                        df,
                        x="score",
                        y="broad_group",
                        orientation="h",
                        points="outliers",
                        category_orders={"broad_group": order},
                    )
                    fig.update_traces(
                        marker=dict(color="#4A90E2", size=3),
                        line=dict(color="#4A90E2", width=1.2),
                        fillcolor="rgba(74,144,226,0.35)",
                    )
                    fig.update_layout(
                        xaxis_title=score_axis_label(),
                        yaxis_title="",
                        showlegend=False,
                    )
                    return apply_paper_style(fig)

    with ui.nav_panel("Download", value="download_view"):
        with ui.card():
            ui.card_header("Export actions")
            ui.input_select(
                "download_format",
                "Download format",
                choices={"csv": "CSV", "parquet": "Parquet", "excel": "Excel (.xlsx)"},
                selected="csv",
            )

            @render.download(
                filename=lambda: f"ai_exposure_data.{download_extension(input.download_format() or 'csv')}",
                media_type=lambda: download_media_type(input.download_format() or "csv"),
                label="Download filtered dataset",
            )
            def download_filtered_data():
                df = filtered_lf().collect().to_pandas()
                return export_filtered_data(df, input.download_format() or "csv")

        with ui.card():
            ui.card_header("Filtered raw data (top 100)")

            @render.ui
            def sample_data():
                df = filtered_lf().head(100).collect().to_pandas()
                return as_great_table_html(df, METRICS)

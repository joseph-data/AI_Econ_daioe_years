import plotly.express as px
import plotly.graph_objects as go
import polars as pl
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
    YEARS,
    as_great_table_html,
    build_choices_by_level,
    download_extension,
    empty_figure,
    export_filtered_data,
    lf,
)

### ------------ BRANDING & THEME --------------- ###

NERO_NAVY = "#002B5C"
NERO_BLUE = "#005EB8"
NERO_GRAY = "#666666"
COLOR_UP = "#28a745"
COLOR_DOWN = "#dc3545"

### ------------ APP UI SETUP --------------- ###

ui.page_opts(
    title="DAIOE Explorer",
    theme=ui.Theme.from_brand(__file__),
    fillable=True,
    lang="en",
    full_width=True,
)

ui.head_content(
    ui.tags.style(
        f"""
        body {{ background-color: #F4F7F9; color: #333; font-family: 'Inter', sans-serif; }}
        .card {{ border-radius: 4px; border: 1px solid #D1D9E0; box-shadow: 0 1px 3px rgba(0,0,0,0.05); background: white; }}
        .nav-pills .nav-link.active {{ background-color: {NERO_NAVY} !important; }}
        .value-box {{ border: 1px solid #D1D9E0 !important; background: white !important; }}
        """,
    ),
)

with ui.navset_pill(id="main_tabs"):

    # PANEL 1: OCCUPATION VIEW
    with ui.nav_panel("1. Occupation View", value="occ_view"), ui.layout_sidebar():
        with ui.sidebar(bg="#FFFFFF", width=300, title="Focus Occupation"):
            ui.input_select("level", "SSYK Level", choices=["Search All Levels", *LEVELS], selected="SSYK4")
            ui.input_selectize("occ_select", "Select Occupation", choices={}, multiple=False,
                              options={"placeholder": "Search occupation title..."})
            ui.hr()
            ui.input_select("metric", "Primary AI Metric", choices=METRICS, selected="daioe_genai")
            ui.input_selectize("sex", "Sex", choices=SEXES, selected=SEXES, multiple=True)
            ui.input_selectize("age", "Age Group", choices=AGES, selected="Early Career 2 (25-29)", multiple=True)
            ui.input_slider("years", "Year Range", min=YEAR_MIN, max=YEAR_MAX, value=(YEAR_MIN, YEAR_MAX), sep="")

        @render.ui
        def occ_title():
            occ = input.occ_select()
            title = f": {occ}" if occ else ""
            return ui.markdown(f"### Occupation Deep-Dive{title}")

        with ui.layout_columns(col_widths=[6, 6], gap="1rem"):
            with ui.value_box(theme="light"):
                "Total Employment"
                @render.ui
                def emp_kpi():
                    d = occ_data_latest()
                    if d.is_empty():
                        return ui.span("---")
                    return ui.div(
                        ui.span(f"{int(d['count'][0]):,}", style="font-size: 1.5rem; font-weight: bold;"),
                        ui.span(f" ({d['year'][0]})", style="font-size: 0.9rem; color: #666; font-weight: normal;"),
                    )

            with ui.value_box(theme="light"):
                "Percentile Rank"
                @render.ui
                def ai_pctl_kpi():
                    d = occ_data_latest()
                    if d.is_empty():
                        return ui.span("---")
                    m = f"pctl_{input.metric()}_wavg"
                    return ui.div(
                        ui.span(f"{float(d[m][0]):.1f}%", style="font-size: 1.5rem; font-weight: bold;"),
                        ui.span(f" ({d['year'][0]})", style="font-size: 0.9rem; color: #666; font-weight: normal;"),
                    )

        with ui.layout_columns(col_widths=[4, 4, 4], gap="1rem"):
            with ui.value_box(theme="light"):
                "1-yr change"
                @render.ui
                def chg_1y_kpi():
                    d = occ_data_latest()
                    return format_pct_chg(d["pct_chg_1y"][0] if not d.is_empty() else None)

            with ui.value_box(theme="light"):
                "3-yr change"
                @render.ui
                def chg_3y_kpi():
                    d = occ_data_latest()
                    return format_pct_chg(d["pct_chg_3y"][0] if not d.is_empty() else None)

            with ui.value_box(theme="light"):
                "5-yr change"
                @render.ui
                def chg_5y_kpi():
                    d = occ_data_latest()
                    return format_pct_chg(d["pct_chg_5y"][0] if not d.is_empty() else None)

        with ui.layout_columns(col_widths=[7, 5], gap="1rem"):
            with ui.card(full_screen=True):
                ui.card_header("Employment & Exposure Percentile Trends")
                @render_widget
                def occ_trend_plot():
                    df = occ_trend_data()
                    if df.is_empty():
                        return empty_figure("No data for current filters", {"text": "#666"})

                    fig = px.line(df, x="year", y="count", labels={"count": "Employment"})
                    fig.update_traces(line_color=NERO_BLUE, name="Employment", showlegend=True)

                    m_col = f"pctl_{input.metric()}_wavg"
                    fig.add_scatter(x=df["year"], y=df[m_col], name=f"{METRICS[input.metric()]} (%)",
                                  yaxis="y2", line={"color": "#D9534F", "dash": "dash"})

                    fig.update_layout(
                        template="plotly_white",
                        yaxis={"title": "Employment"},
                        yaxis2={"title": "Exposure Percentile", "overlaying": "y", "side": "right", "range": [0, 105]},
                        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
                    )
                    return fig

            with ui.card(full_screen=True):
                ui.card_header("AI Capability Profile (Percentiles)")
                @render_widget
                def ai_profile_plot():
                    df = occ_data_latest()
                    if df.is_empty():
                        return empty_figure("No data", {"text": "#666"})

                    m_names = [METRICS[k] for k in METRICS]
                    m_vals = [df[f"pctl_{k}_wavg"][0] for k in METRICS]

                    fig = px.bar(x=m_vals, y=m_names, orientation="h",
                                labels={"x": "Percentile Rank (%)", "y": ""},
                                range_x=[0, 100])
                    fig.update_traces(marker_color=NERO_NAVY)
                    fig.update_layout(template="plotly_white", yaxis={"categoryorder": "total ascending"})
                    return fig

    # PANEL 2: COMPARISON VIEW
    with ui.nav_panel("2. Comparison View", value="comp_view"), ui.layout_sidebar():
        with ui.sidebar(bg="#FFFFFF", width=300, title="Benchmarking"):
            ui.input_select("comp_level", "SSYK Level", choices=["All Levels", *LEVELS], selected="SSYK4")
            ui.input_selectize("comp_occs", "Select Occupations", choices={}, multiple=True)
            ui.hr()
            ui.input_selectize("comp_age", "Age Group", choices=AGES, selected="Early Career 2 (25-29)", multiple=True)
            ui.hr()
            ui.input_select("comp_year", "Comparison Year (Radar)", choices=[str(y) for y in YEARS], selected=str(YEAR_MAX))

        with ui.card():
            ui.card_header("Benchmarking Summary")
            @render.ui
            def comparison_summary():
                df = comparison_data()
                if df.is_empty():
                    return ui.markdown("*Select occupations to generate a summary...*")

                summary_rows = []
                for occ in df["occupation"].unique():
                    sub = df.filter(pl.col("occupation") == occ).sort("year")

                    # Get latest data point
                    latest = sub.tail(1)
                    curr_yr = latest["year"][0]
                    curr_emp = latest["count"][0]

                    # Get historical points relative to the latest year available in the filtered set
                    emp_1y = sub.filter(pl.col("year") == curr_yr - 1)["count"]
                    emp_3y = sub.filter(pl.col("year") == curr_yr - 3)["count"]
                    emp_5y = sub.filter(pl.col("year") == curr_yr - 5)["count"]

                    val_1y = f"{int(emp_1y[0]):,}" if not emp_1y.is_empty() else "---"
                    val_3y = f"{int(emp_3y[0]):,}" if not emp_3y.is_empty() else "---"
                    val_5y = f"{int(emp_5y[0]):,}" if not emp_5y.is_empty() else "---"

                    summary_rows.append(
                        ui.tags.tr(
                            ui.tags.td(occ, style="font-weight: bold;"),
                            ui.tags.td(val_5y),
                            ui.tags.td(val_3y),
                            ui.tags.td(val_1y),
                            ui.tags.td(f"{int(curr_emp):,}", style="background-color: #f8f9fa; font-weight: bold;"),
                        ),
                    )

                latest_yr = df["year"].max()
                return ui.tags.table(
                    ui.tags.thead(
                        ui.tags.tr(
                            ui.tags.th("Occupation"),
                            ui.tags.th(f"Emp ({latest_yr-5})"),
                            ui.tags.th(f"Emp ({latest_yr-3})"),
                            ui.tags.th(f"Emp ({latest_yr-1})"),
                            ui.tags.th(f"Emp ({latest_yr})"),
                        ),
                    ),
                    ui.tags.tbody(*summary_rows),
                    class_="table table-sm table-hover",
                    style="font-size: 0.9rem;",
                )

        with ui.layout_columns(col_widths=[6, 6], gap="1rem"):
            with ui.card(full_screen=True):
                ui.card_header("Employment Trends (Selected Occupations)")

                @render_widget
                def comparison_employment_plot():
                    df = comparison_data()
                    if df.is_empty():
                        return empty_figure("Select occupations to compare", {"text": "#666"})

                    # Group to get total employment per year/occupation
                    fig = px.line(
                        df,
                        x="year",
                        y="count",
                        color="occupation",
                        markers=True,
                        labels={"count": "Total Employment", "year": "Year"},
                    )

                    fig.update_layout(
                        template="plotly_white",
                        legend={"orientation": "h", "yanchor": "bottom", "y": -0.2, "xanchor": "center", "x": 0.5},
                    )
                    return fig

            with ui.card(full_screen=True):
                ui.card_header("Radar Comparison (AI & Employment Percentiles)")
                @render_widget
                def comp_radar_plot():
                    df = comp_radar_data()
                    if df.is_empty():
                        return empty_figure("Select occupations to compare", {"text": "#666"})

                    # We use AI metrics as the axes
                    categories = [METRICS[k] for k in METRICS]

                    fig = go.Figure()

                    for occ in df["occupation"].unique():
                        sub = df.filter(pl.col("occupation") == occ)
                        # Pull values for AI Metrics
                        r_values = [sub[f"pctl_{k}_wavg"][0] for k in METRICS]

                        # Close the radar loop by repeating the first value
                        r_values_closed = [*r_values, r_values[0]]
                        categories_closed = [*categories, categories[0]]

                        fig.add_trace(go.Scatterpolar(
                            r=r_values_closed,
                            theta=categories_closed,
                            fill="toself",
                            name=occ,
                            hovertemplate="%{theta}: %{r:.1f}%<extra></extra>",
                        ))

                    fig.update_layout(
                        polar={
                            "radialaxis": {"visible": True, "range": [0, 100]},
                        },
                        showlegend=True,
                        template="plotly_white",
                        legend={"orientation": "h", "yanchor": "bottom", "y": -0.2, "xanchor": "center", "x": 0.5},
                    )
                    return fig

    # PANEL 3: DOWNLOAD DATA
    with ui.nav_panel("3. Download Data", value="dl_view"), ui.layout_sidebar():
        with ui.sidebar(bg="#FFFFFF", width=320, title="Export Filters"):
            ui.input_select("dl_level", "Level", choices=["All Levels", *LEVELS], selected="SSYK4")
            ui.input_selectize("dl_occs", "Select Specific Occupations", choices={}, multiple=True)
            ui.hr()
            ui.input_selectize("dl_sex", "Sex Filter", choices=SEXES, selected=SEXES, multiple=True)
            ui.input_selectize("dl_age", "Age Filter", choices=AGES, selected=AGES, multiple=True)
            ui.hr()
            ui.input_select("dl_format", "File Format", choices={"csv": "CSV", "parquet": "Parquet", "xlsx": "Excel"})

            @render.download(
                filename=lambda: f"ai_exposure_export.{download_extension(input.dl_format())}",
                label="Download Dataset",
            )
            def download_data():
                return export_filtered_data(dl_filtered_lf().collect().to_pandas(), input.dl_format())

        with ui.card(full_screen=True):
            ui.card_header("Filtered Data Preview (First 50 records)")
            @render.ui
            def data_preview():
                return as_great_table_html(dl_filtered_lf().head(50).collect().to_pandas(), METRICS)

### ------- RE-USABLES & LOGIC ------------##

CHOICES_BY_LEVEL = build_choices_by_level(lf, LEVELS)
ALL_OCCS_LIST = lf.select(pl.col("occupation").unique().sort()).collect().to_series().to_list()
CHOICES_BY_LEVEL["Search All Levels"] = {o: o for o in ALL_OCCS_LIST}
CHOICES_BY_LEVEL["All Levels"] = CHOICES_BY_LEVEL["Search All Levels"]

def weighted_avg_expr(col: str) -> pl.Expr:
    return (pl.col(col) * pl.col("count")).sum() / pl.col("count").sum()

def format_pct_chg(val: float | None):
    if val is None:
        return ui.span("---")
    icon = "▲" if val > 0 else "▼" if val < 0 else ""
    color = COLOR_UP if val > 0 else COLOR_DOWN if val < 0 else "#666"
    return ui.div(
        ui.span(f"{icon} {abs(val):.1f}%", style=f"color: {color}; font-size: 1.5rem; font-weight: bold;"),
    )

@reactive.effect
def _sync_choices():
    ui.update_selectize("occ_select", choices=CHOICES_BY_LEVEL[input.level()], server=True)
    ui.update_selectize("comp_occs", choices=CHOICES_BY_LEVEL[input.comp_level()], server=True)
    ui.update_selectize("dl_occs", choices=CHOICES_BY_LEVEL[input.dl_level()], server=True)

@reactive.calc
def filtered_base() -> pl.LazyFrame:
    # Use different inputs based on active tab if necessary,
    # but here we'll simplify: if on comparison tab, use comp_age
    active_tab = input.main_tabs()

    if active_tab == "comp_view":
        ages = list(input.comp_age() or AGES)
    else:
        ages = list(input.age() or AGES)

    return lf.filter(
        pl.col("sex").is_in(list(input.sex() or SEXES)),
        pl.col("age_group").is_in(ages),
        pl.col("year").is_between(input.years()[0], input.years()[1]),
    )

@reactive.calc
def occ_trend_data() -> pl.DataFrame:
    occ = input.occ_select()
    if not occ:
        return pl.DataFrame()
    q = filtered_base().filter(pl.col("occupation") == occ)
    if input.level() != "Search All Levels":
        q = q.filter(pl.col("level") == input.level())

    return q.group_by("year").agg([
        pl.col("count").sum(),
        pl.col("pct_chg_1y").mean(),
        pl.col("pct_chg_3y").mean(),
        pl.col("pct_chg_5y").mean(),
        *[weighted_avg_expr(f"pctl_{k}_wavg").alias(f"pctl_{k}_wavg") for k in METRICS],
    ]).sort("year").collect()

@reactive.calc
def occ_data_latest() -> pl.DataFrame:
    df = occ_trend_data()
    return df.tail(1) if not df.is_empty() else pl.DataFrame()

@reactive.calc
def comparison_data() -> pl.DataFrame:
    occs = input.comp_occs()
    if not occs:
        return pl.DataFrame()

    q = filtered_base().filter(pl.col("occupation").is_in(occs))
    if input.comp_level() != "All Levels":
        q = q.filter(pl.col("level") == input.comp_level())

    res = q.group_by(["year", "occupation"]).agg([
        pl.col("count").sum().alias("count"),
        *[weighted_avg_expr(f"pctl_{k}_wavg").alias(f"pctl_{k}_wavg") for k in METRICS],
    ]).collect().sort("year")

    return res

@reactive.calc
def dl_filtered_lf() -> pl.LazyFrame:
    q = lf
    if input.dl_level() != "All Levels":
        q = q.filter(pl.col("level") == input.dl_level())
    if input.dl_occs():
        q = q.filter(pl.col("occupation").is_in(input.dl_occs()))
    if input.dl_sex():
        q = q.filter(pl.col("sex").is_in(list(input.dl_sex())))
    if input.dl_age():
        q = q.filter(pl.col("age_group").is_in(list(input.dl_age())))
    return q

@reactive.calc
def comp_radar_data() -> pl.DataFrame:
    occs = input.comp_occs()
    if not occs:
        return pl.DataFrame()
    yr = int(input.comp_year())

    # 1. Get filtered base for all occupations in that year to calculate employment percentiles
    base_yr = filtered_base().filter(pl.col("year") == yr)

    # 2. Total employment by occupation in that year
    all_occ_emp = (
        base_yr.group_by("occupation")
        .agg(pl.col("count").sum())
        .collect()
        .sort("count")
    )

    # Calculate employment percentile rank [0-100]
    # (rank / total) * 100
    total_occs = all_occ_emp.height
    all_occ_emp = all_occ_emp.with_columns(
        ((pl.col("count").rank() / total_occs) * 100).alias("emp_pctl"),
    )

    # 3. Get AI metrics for selected occupations
    target_occs_ai = (
        base_yr.filter(pl.col("occupation").is_in(occs))
        .group_by("occupation")
        .agg([
            *[weighted_avg_expr(f"pctl_{k}_wavg").alias(f"pctl_{k}_wavg") for k in METRICS],
        ])
        .collect()
    )

    # 4. Join employment percentile onto AI metrics
    return target_occs_ai.join(all_occ_emp.select(["occupation", "emp_pctl"]), on="occupation")

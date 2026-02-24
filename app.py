import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from shiny import reactive
from shiny.express import input, render, ui
from shinywidgets import render_widget

from setup import AGES, LEVELS, METRICS, SEXES, YEAR_MAX, YEAR_MIN, lf

# --- BRAND CONSTANTS (Matching your brand.yml) ---
BRAND = {
    "primary": "#0C0A3E",    # Blue
    "secondary": "#4D6CFA",  # Violet
    "info": "#BA274A",       # Red
    "background": "#F9F7F1", # Neutral/Cream
    "text": "#1C2826"        # Black/Dark Teal
}

SCORE_TYPES: dict[str, str] = {
    "avg": "Average",
    "wavg": "Weighted average",
    "pctl": "Percentile (weighted average)",
}

AGE_GROUPS = AGES

ui.page_opts(
    title="AI Exposure by Occupation Explorer",
    theme=ui.Theme.from_brand(__file__),
    fillable=True
)

# Custom CSS to ensure our brand colors are used in UI elements
ui.head_content(
    ui.tags.style(f"""
        .value-box {{ border: 1px solid {BRAND['primary']}22 !important; }}
        .nav-pills .nav-link.active {{ background-color: {BRAND['primary']} !important; }}
        body {{ background-color: {BRAND['background']}; }}
    """)
)

with ui.sidebar(position="left", bg="#FFFFFF"):
    ui.h5("Filters", style=f"color: {BRAND['primary']}; font-weight: 700;")
    ui.input_select("level", "SSYK level", choices=LEVELS, selected="SSYK4")
    ui.input_selectize("sex", "Sex", choices=SEXES, selected=SEXES, multiple=True)
    ui.input_selectize("age", "Age group", choices=AGE_GROUPS, selected=AGE_GROUPS, multiple=True)
    ui.input_slider("year_range", "Year range", min=YEAR_MIN, max=YEAR_MAX, value=(YEAR_MIN, YEAR_MAX), sep="")
    ui.hr()
    ui.input_select("metric", "AI capability", choices=METRICS, selected="daioe_genai")
    ui.input_radio_buttons("score_type", "Score type", choices=SCORE_TYPES, selected="wavg")
    ui.input_numeric("top_n", "Top occupations in bar chart", value=15, min=5, max=50, step=1)

@reactive.calc
def metric_col() -> str:
    base = input.metric()
    score_type = input.score_type()
    if score_type == "avg": return f"{base}_avg"
    if score_type == "wavg": return f"{base}_wavg"
    return f"pctl_{base}_wavg"

@reactive.calc
def score_axis_label() -> str:
    return SCORE_TYPES.get(input.score_type(), "Score")

@reactive.calc
def filtered_lf() -> pl.LazyFrame:
    selected_level = input.level() or "SSYK4"
    selected_sex = list(input.sex()) if input.sex() else SEXES
    selected_age = list(input.age()) if input.age() else AGE_GROUPS
    year_start, year_end = input.year_range()

    return lf.filter(
        pl.col("level") == selected_level,
        pl.col("sex").is_in(selected_sex),
        pl.col("age_group").is_in(selected_age),
        pl.col("year").is_between(year_start, year_end),
    )

@reactive.calc
def overview() -> dict[str, float | int]:
    col = metric_col()
    df = (
        filtered_lf()
        .select(
            pl.len().alias("rows"),
            pl.col("count").sum().alias("workers"),
            ((pl.col(col) * pl.col("count")).sum() / pl.col("count").sum()).alias("score"),
        )
        .collect()
    )
    
    # FIX: Safety check for empty data to prevent IndexErrors
    if df.height == 0 or df["rows"][0] == 0:
        return {"rows": 0, "workers": 0, "score": 0.0}
    
    return df.to_dicts()[0]

@reactive.calc
def trend_data() -> pl.DataFrame:
    col = metric_col()
    return (
        filtered_lf()
        .group_by("year")
        .agg(
            pl.col("count").sum().alias("workers"),
            ((pl.col(col) * pl.col("count")).sum() / pl.col("count").sum()).alias("score"),
        )
        .sort("year")
        .collect()
    )

@reactive.calc
def top_occupations() -> pl.DataFrame:
    col = metric_col()
    _, year_end = input.year_range()
    n = int(input.top_n() or 15)
    return (
        filtered_lf()
        .filter(pl.col("year") == year_end)
        .group_by(["ssyk_code", "occupation"])
        .agg(
            pl.col("count").sum().alias("workers"),
            ((pl.col(col) * pl.col("count")).sum() / pl.col("count").sum()).alias("score"),
        )
        .sort("score", descending=True)
        .head(n)
        .collect()
    )

@reactive.calc
def age_sex_data() -> pl.DataFrame:
    col = metric_col()
    return (
        filtered_lf()
        .group_by(["age_group", "sex"])
        .agg(
            pl.col("count").sum().alias("workers"),
            ((pl.col(col) * pl.col("count")).sum() / pl.col("count").sum()).alias("score"),
        )
        .collect()
    )

def apply_plot_style(fig: go.Figure) -> go.Figure:
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Nunito Sans", color=BRAND["text"]),
        hoverlabel=dict(bgcolor="white", font_size=12),
        margin=dict(l=20, r=20, t=40, b=20)
    )
    fig.update_xaxes(gridcolor="#E5E5E5", zeroline=False)
    fig.update_yaxes(gridcolor="#E5E5E5", zeroline=False)
    return fig

def empty_figure(message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=message, showarrow=False, font_size=16)
    fig.update_xaxes(visible=False); fig.update_yaxes(visible=False)
    return apply_plot_style(fig)

with ui.navset_pill(id="tab"):
    with ui.nav_panel("Visuals"):
        with ui.layout_columns(col_widths=[4, 4, 4]):
            with ui.value_box(theme="light"):
                "Filtered rows"
                @render.text
                def rows_kpi(): return f"{overview()['rows']:,}"

            with ui.value_box(theme="light"):
                "Total workers"
                @render.text
                def workers_kpi():
                    w = overview()["workers"]
                    return f"{int(w or 0):,}"

            with ui.value_box(theme="light"):
                "Average AI Exposure"
                @render.text
                def score_kpi():
                    s = overview()["score"]
                    return f"{float(s or 0):.3f}"

        with ui.layout_columns(col_widths=[7, 5]):
            with ui.card(full_screen=True):
                ui.card_header("Exposure Trend by Year")
                @render_widget
                def trend_plot():
                    df = trend_data().to_pandas()
                    if df.empty: return empty_figure("No data available")
                    fig = px.line(df, x="year", y="score", markers=True, 
                                 color_discrete_sequence=[BRAND["secondary"]])
                    fig.update_layout(yaxis_title=score_axis_label())
                    return apply_plot_style(fig)

            with ui.card(full_screen=True):
                ui.card_header("Age x Sex Distribution")
                @render_widget
                def heatmap_plot():
                    df = age_sex_data().to_pandas()
                    if df.empty: return empty_figure("No data available")
                    pivot = df.pivot(index="age_group", columns="sex", values="score")
                    fig = px.imshow(pivot, text_auto=".2f", aspect="auto",
                                   color_continuous_scale=[BRAND["background"], BRAND["info"]])
                    return apply_plot_style(fig)

        with ui.card(full_screen=True):
            ui.card_header("Highest Exposure Occupations")
            @render_widget
            def occupation_bar():
                df = top_occupations().to_pandas()
                if df.empty: return empty_figure("No data for end year")
                df["label"] = df["ssyk_code"] + " - " + df["occupation"].fillna("Unknown")
                df = df.sort_values("score")
                fig = px.bar(df, x="score", y="label", orientation="h",
                            color="score", color_continuous_scale=[BRAND["secondary"], BRAND["primary"]])
                fig.update_layout(xaxis_title=score_axis_label(), yaxis_title="")
                return apply_plot_style(fig)

    with ui.nav_panel("Data Explorer"):
        with ui.layout_columns(col_widths=[4, 8]):
            with ui.card():
                ui.card_header("Export Actions")
                @render.download(filename="ai_exposure_data.csv")
                def download_filtered_data():
                    df = filtered_lf().collect().to_pandas()
                    return df.to_csv(index=False)
            
            with ui.card():
                ui.card_header("Filtered Raw Data (Top 100)")
                @render.data_frame
                def sample_data():
                    return filtered_lf().head(100).collect().to_pandas()
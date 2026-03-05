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
    build_choices_by_level,
    first_cols,
)




ui.page_opts(
    title="Yearly AI Exposure Dashboard",
    theme=ui.Theme.from_brand(__file__),
    fillable=True,
)


with ui.sidebar(position="left"):
    ui.input_select("level", "Occupation Level 🇸🇪", choices=LEVELS, selected="SSYK4")    
    
    ui.input_selectize(
        "occupation_search",
        "Search/Select Occupation",
        choices={},
        selected=[],
        multiple=True,
        options=(
            {
                "placeholder": "Software Developers...",
                "create": False,
                "plugins": ["clear_button"],
            }
        ),
    )

    ui.input_selectize(
        "age_search",
        "Select Age Group",
        choices=AGES,
        selected= AGES[0],
        multiple=True,
    )

    ui.input_selectize(
        "sex_search",
        "Select Gender",
        choices=SEXES,
        selected=SEXES,
        multiple=True,
    )


    ui.input_slider(
        "year_range",
        "Year range",
        min=YEAR_MIN,
        max=YEAR_MAX,
        value=(YEAR_MIN, YEAR_MAX),
        step=1,
        ticks=True,
        sep="",
    )

    ui.input_select(
        "metric",
        "Sub-index",
        choices=METRICS,
        selected=next(iter(METRICS)),
    )
    


CHOICES_BY_LEVEL = build_choices_by_level(lf, LEVELS)
@reactive.effect
def _():
    ui.update_selectize(
        "occupation_search",
        choices=CHOICES_BY_LEVEL[input.level()],
        selected=[],
        server=True,   # recommended if lots of occupations
    )

@reactive.calc
def q_base() -> pl.LazyFrame:
    metric = input.metric()
    yr_min, yr_max = input.year_range()
    q = lf.filter(
        (pl.col("level") == input.level()) &
        (pl.col("year").is_between(yr_min, yr_max)) &
        (pl.col("sex").is_in(input.sex_search())) &
        (pl.col("age_group").is_in(input.age_search()))
        ).select(
        pl.col(first_cols),
        pl.col(f"^{metric}.*$")
    )
    return q.cache()  

@reactive.calc
def filtered_lf() -> pl.LazyFrame:
    occ = input.occupation_search()
    q = q_base()

    if occ:
        q = q.filter(pl.col("occupation").is_in(occ))

    return q

with ui.navset_pill(id="tab"):
    with ui.nav_panel("Visuals", value="visuals"):
        with ui.card(full_screen=True):
            ui.card_header("Search result: occupation visual")


    

    with ui.nav_panel("Dataset", value="download_view"):
        with ui.card():
            ui.card_header("Export Data")
            ui.markdown(
                """
                You can download the filtered dataset in this section. Current file options are `csv`, `parquet`, and `excel`.

                In the proceeding section, preview the dataset is displayed.
                """
                )
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
            ui.card_header("Filtered Raw Data (top 100)")

            @render.ui
            def sample_data():
                df = filtered_lf().head(100).collect().to_pandas()
                return as_great_table_html(df, METRICS)
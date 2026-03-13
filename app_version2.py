from shiny.ui._card import _full_screen_toggle
import polars as pl
import numpy as np
import plotly.graph_objects as go
from shiny import reactive
from shiny.express import input, render, ui
from shinywidgets import render_widget
import datetime

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


### ------------ APP UI SETUP --------------- ###

ui.page_opts(
    title="Yearly AI Exposure Dashboard",
    theme=ui.Theme.from_brand(__file__),
    fillable=True,
    lang="en",
    full_width=True,
)

with ui.navset_pill_list(id="tab", widths=(1,11)):
    with ui.nav_panel("Occupation View", value="occupations"):
        with ui.card():  
            #ui.card_header("Changes in Employment by Occupation")

            with ui.layout_sidebar():  
                with ui.sidebar(bg="#00000000", width=220, open="desktop", title="Filters"):
                    
                    ui.input_select(
                        "metric",
                        "Sub-index",
                        choices=METRICS,
                        selected=next(iter(METRICS)),
                    )


                    ui.input_select(
                        "level", 
                        "Level 🇸🇪", 
                        choices=LEVELS, 
                        selected="SSYK4",
                    ) 
                    
                    ui.input_selectize(
                        "occupation_search",
                        "Search/Select Occupation",
                        choices={},
                        selected=None,
                        multiple=False,
                        options={
                            "placeholder": "Accountants ...",
                            "create": False,
                            "plugins": ["clear_button"],
                        },
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

                ui.markdown("""
    ##  Changes in Employment by Occupation

    This panel presents the changes in employment over the years by occupation across 
    the **SSYK 2012** levels. We consider also an overview by age and sex.
                """)

            # with ui.card():
            #      "Card 2"
        # with ui.layout_columns():
        #     with ui.card():
        #          "Card 3"
        #     with ui.card():
        #          "Card 4"

    with ui.nav_panel("Comparison View", value="compare"):
        "Panel B content"


    with ui.nav_panel("Download", value="data_download"):
        with ui.card():
            ui.card_header("Export Data")

            with ui.layout_columns(col_widths=(4, 8)):
                with ui.card(full_screen=False):
                    ui.card_header("Filters")

                    ui.input_select(
                        "level2",
                        "Occupation Level 🇸🇪",
                        choices=LEVELS,
                        selected="SSYK4",
                    )

                    ui.input_selectize(
                        "occupation_search2",
                        "Search/Select Occupation",
                        choices={},
                        selected=[],
                        multiple=True,
                        options={
                            "placeholder": "Software Developers...",
                            "create": False,
                            "plugins": ["clear_button"],
                        },
                    )

                    with ui.layout_columns(col_widths=(6, 6)):
                        ui.input_selectize(
                            "age_search2",
                            "Select Age Group",
                            choices=AGES,
                            selected=AGES,
                            multiple=True,
                        )

                        ui.input_selectize(
                            "sex_search2",
                            "Select Gender",
                            choices=SEXES,
                            selected=SEXES,
                            multiple=True,
                        )

                    ui.input_slider(
                        "year_range2",
                        "Year range",
                        min=YEAR_MIN,
                        max=YEAR_MAX,
                        value=(YEAR_MIN, YEAR_MAX),
                        step=1,
                        ticks=True,
                        sep="",
                    )

                    ui.input_select(
                        "metric2",
                        "Sub-index",
                        choices=METRICS,
                        selected=next(iter(METRICS)),
                    )

                with ui.card(full_screen=False):
                    ui.card_header("Download")

                    ui.markdown(
                        """
                        Download the currently filtered dataset in **CSV**, **Parquet**, or **Excel** format.

                        Use the filters on the left to refine the data before exporting.
                        """
                    )

                    with ui.layout_columns(col_widths=(6, 6)):
                        ui.input_select(
                            "download_format",
                            "Download format",
                            choices={
                                "csv": "CSV",
                                "parquet": "Parquet",
                                "excel": "Excel (.xlsx)",
                            },
                            selected="csv",
                        )

                    @render.download(
                        filename=lambda: f"ai_exposure_data.{download_extension(input.download_format() or 'csv')}",
                        media_type=lambda: download_media_type(input.download_format() or "csv"),
                        label="Download filtered dataset",
                    )
                    def download_filtered_data():
                        df = filtered_lf2().collect().to_pandas()
                        return export_filtered_data(df, input.download_format() or "csv")


        with ui.card(full_screen=True):
            ui.card_header("Filtered Raw Data Preview")

            @render.ui
            def sample_data():
                df = filtered_lf2().head(20).collect().to_pandas()
                return as_great_table_html(df, METRICS)
                    
        # @render.text
        # def current_time():
        #     reactive.invalidate_later(1)
        #     return datetime.datetime.now().strftime("%H:%M:%S %p")


### ------- RE-USABLES ------------##

CHOICES_BY_LEVEL = build_choices_by_level(lf, LEVELS)


### ------------ 01. DATA SETUP (Occupation View) --------------- ###

@reactive.effect
def _():
    ui.update_selectize(
        "occupation_search",
        choices=CHOICES_BY_LEVEL[input.level()],
        selected=None,
        server=True,
    )

@reactive.calc
def q_base() -> pl.LazyFrame:
    metric = input.metric()
    yr_min, yr_max = input.year_range()
    q = lf.filter(
        (pl.col("level") == input.level()) &
        (pl.col("year").is_between(yr_min, yr_max))
        ).select(
        pl.col(first_cols),
        pl.col(f"^(pctl_)?{metric}.*$")
    )
    return q.cache() 

@reactive.calc
def filtered_lf() -> pl.LazyFrame:
    occ = input.occupation_search()
    q = q_base()

    if occ:
        q = q.filter(pl.col("occupation") == occ)

    return q

### ------------ 02. DATA SETUP (Download View) --------------- ###

@reactive.effect
def _():
    ui.update_selectize(
        "occupation_search2",
        choices=CHOICES_BY_LEVEL[input.level()],
        selected=[],
        server=True, 
    )

@reactive.calc
def q_base2() -> pl.LazyFrame:
    metric = input.metric2()
    yr_min, yr_max = input.year_range2()
    q = lf.filter(
        (pl.col("level") == input.level2()) &
        (pl.col("year").is_between(yr_min, yr_max)) &
        (pl.col("sex").is_in(input.sex_search2())) &
        (pl.col("age_group").is_in(input.age_search2()))
        ).select(
        pl.col(first_cols),
        pl.col(f"^(pctl_)?{metric}.*$")
    )
    return q.cache()  

@reactive.calc
def filtered_lf2() -> pl.LazyFrame:
    occ = input.occupation_search2()
    q = q_base2()

    if occ:
        q = q.filter(pl.col("occupation").is_in(occ))

    return q

# @render.data_frame
# def table_new1():
#     return filtered_lf().head(10).collect().to_pandas()

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

### ------------ DATA SETUP --------------- ###

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
        pl.col(f"^(pctl_)?{metric}.*$")
    )
    return q.cache()  

@reactive.calc
def filtered_lf() -> pl.LazyFrame:
    occ = input.occupation_search()
    q = q_base()

    if occ:
        q = q.filter(pl.col("occupation").is_in(occ))

    return q


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
                with ui.sidebar(bg="#00000000", width=220, open="desktop"):  
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
                        selected=[],
                        multiple=False,
                        options={
                            "placeholder": "Accountants ...",
                            "create": False,
                            "plugins": ["clear_button"],
                        },
                    )  




                ui.markdown("""
    ##  Changes in Employment by Occupation

    This panel presents the changes in employment over the years by occupation across 
    the **SSYK 2012** levels. We consider also an overview by age and sex.
                """)
    
    #     with ui.layout_columns():
    #         with ui.card():
    #             with ui.layout_sidebar(): 
    #                 with ui.sidebar():
    #                     ui.input_select(
    #                         "level", 
    #                         "Level 🇸🇪", 
    #                         choices=LEVELS, 
    #                         selected="SSYK4", 
    #                         width="50%" 
    #                     ) 

    #                     ui.input_selectize(
    #                         "occupation_search",
    #                         "Search/Select Occupation",
    #                         choices={},
    #                         selected=[],
    #                         multiple=False,
    #                         options={
    #                             "placeholder": "Software Developers...",
    #                             "create": False,
    #                             "plugins": ["clear_button"],
    #                         },
    #                         width="50%",
    #                     )
    #         ui.markdown("""
    # ## Changes in Employment by Occupation

    # This panel presents the changes in employment over the years by occupation across 
    # the **SSYK 2012** levels. We consider also an overview by age and sex.
    #             """)


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
            ui.card_header("Filtered Raw Data (Top 100)")

            @render.ui
            def sample_data():
                df = filtered_lf().head(20).collect().to_pandas()
                return as_great_table_html(df, METRICS)
        with ui.card():
            @render.data_frame
            def table_new():
                return filtered_lf().head(20).collect().to_pandas()
                    
        # @render.text
        # def current_time():
        #     reactive.invalidate_later(1)
        #     return datetime.datetime.now().strftime("%H:%M:%S %p")






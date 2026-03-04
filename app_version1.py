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

@reactive.calc
def filtered_lf():
    d_lf = lf.head(100)
    return d_lf


ui.page_opts(
    title="Yearly AI Exposure Dashboard",
    theme=ui.Theme.from_brand(__file__),
    fillable=True,
)

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
                df = filtered_lf().head(20).collect().to_pandas()
                return as_great_table_html(df, METRICS)
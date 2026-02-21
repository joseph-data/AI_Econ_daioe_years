import polars as pl

from shiny.express import ui, input, render
from shiny import reactive
from shinywidgets import render_widget

# ---------------------------------------------------
# Data Preliminaries from the setup.py script
# ---------------------------------------------------
from setup import lf

# ---------------------------------------------------
# Page Setup
# ---------------------------------------------------

ui.page_opts(
    theme=ui.Theme.from_brand(__file__)
    )

# ---------------------------------------------------
# Input Section
# ---------------------------------------------------

with ui.sidebar(position="left"):
    "Sidebar"

# ---------------------------------------------------
# UI Section
# ---------------------------------------------------
with ui.navset_pill(id = "tab"):
    with ui.nav_panel("Visuals"):
        with ui.card():
            "Here"

    with ui.nav_panel("Data"):
        with ui.card():
            @render.data_frame
            def summ_tab():
                df = lf.head(10).collect().to_pandas()
                return df

            @render.text
            def count():
                return lf.select(pl.len()).collect().item()

# ---------------------------------------------------
# Reactive calculations and effects
# ---------------------------------------------------
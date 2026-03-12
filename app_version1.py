import polars as pl
import numpy as np
import plotly.graph_objects as go
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
        selected= AGES,
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


@reactive.calc
def pyramid_counts() -> pl.DataFrame:
    df = filtered_lf().collect()

    out = (
        df.group_by(["age_group", "sex"])
          .agg(pl.col("count").sum().alias("value"))
          .with_columns(pl.col("age_group").cast(pl.Enum(list(AGES))))
          .sort("age_group")
          .with_columns(
              pl.when(pl.col("sex") == "men")
                .then(-pl.col("value"))
                .otherwise(pl.col("value"))
                .alias("value")
          )
    )
    return out


with ui.navset_pill(id="tab"):
    with ui.nav_panel("Visuals", value="visuals"):
        with ui.card(full_screen=True, height="1000px"):
            ui.card_header("Search result: occupation visual")
            
            @render_widget
            def pyramid_plot():
                df = pyramid_counts()

                men = df.filter(pl.col("sex") == "men").to_pandas()
                women = df.filter(pl.col("sex") == "women").to_pandas()

                max_abs = float(df.select(pl.col("value").abs().max()).item() or 0.0)
                if max_abs == 0:
                    max_abs = 1

                gap = max_abs * 0.1   # controls spacing between the bars

                fig = go.Figure()

                # MEN
                if len(men):
                    fig.add_bar(
                        y=men["age_group"],
                        x=men["value"],                 # negative values
                        base=np.full(len(men), -gap),   # shift starting position
                        orientation="h",
                        name="Men",
                        marker_color="#3b5bdb",
                        customdata=men["value"].abs(),  # original count
                        hovertemplate=(
                            "Men<br>"
                            "Age group: %{y}<br>"
                            "Employees: %{customdata:,}"
                            "<extra></extra>"
                        ),
                    )

                # WOMEN
                if len(women):
                    fig.add_bar(
                        y=women["age_group"],
                        x=women["value"],               # positive values
                        base=np.full(len(women), gap),  # shift starting position
                        orientation="h",
                        name="Women",
                        marker_color="#e6492d",
                        customdata=women["value"],      # original count
                        hovertemplate=(
                            "Women<br>"
                            "Age group: %{y}<br>"
                            "Employees: %{customdata:,}"
                            "<extra></extra>"
                        ),
                    )

                fig.update_layout(
                    title="Employment distribution by age and sex",
                    barmode="overlay",
                    xaxis=dict(
                        range=[-(max_abs + gap), max_abs + gap],
                        zeroline=False,
                        title="Number of employees",
                    ),
                    yaxis=dict(showticklabels=False),
                    legend_title_text="Sex",
                    margin=dict(l=40, r=40, t=50, b=40),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                )

                # center age labels
                for age in df["age_group"]:
                    fig.add_annotation(
                        x=0,
                        y=age,
                        text=age,
                        showarrow=False,
                        xanchor="center",
                        font=dict(size=12),
                    )

                return fig

    

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
            ui.card_header("Filtered Raw Data (Top 100)")

            @render.ui
            def sample_data():
                df = filtered_lf().head(20).collect().to_pandas()
                return as_great_table_html(df, METRICS)
        with ui.card():
            @render.data_frame
            def table_new():
                return filtered_lf().head(20).collect().to_pandas()
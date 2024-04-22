# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import dash_bootstrap_components as dbc
import polars as pl
from dash import Dash, Input, Output, dash_table, dcc, html

from .etl_base import EtlBase


class DataQualityDashboard(EtlBase, ABC):
    """
    Class that creates the
    """

    def __init__(
        self,
        dqd_port: int = 8050,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.dqd_port = dqd_port
        assets_folder = str(Path(__file__).parent.parent.resolve() / "assets")
        self.app = Dash(
            name=__name__,
            assets_folder=assets_folder,
            external_stylesheets=[dbc.themes.SANDSTONE],
        )

    def run(
        self,
    ):
        logging.info("Starting data quality dashboard")

        self.app.layout = self._create_layout()

        self._create_callbacks()

        self.app.run_server(host="0.0.0.0", port=self.dqd_port)

    def _create_layout(self):
        last_runs = self._get_last_runs()

        return dbc.Container(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.H2("Data Quality Dashboard", style={"color": "navy"}),
                            ],
                            width=True,
                        ),
                    ],
                    align="end",
                ),
                html.Br(),
                dbc.Row(
                    [
                        dbc.Form(
                            dbc.Row(
                                [
                                    dbc.Label(
                                        "Select data quality check run:",
                                        width="auto",
                                    ),
                                    dbc.Col(
                                        [
                                            dcc.Dropdown(
                                                id="runs-dropdown",
                                                placeholder="Select run",
                                                options=last_runs,
                                                value=last_runs[0]["value"] if last_runs else None,
                                            ),
                                        ],
                                        className="me-3",
                                    ),
                                    dbc.Col(
                                        className="me-6",
                                    ),
                                ],
                            )
                        )
                    ],
                    align="end",
                ),
                html.Br(),
                dbc.Spinner(
                    [
                        dbc.Row(
                            [
                                html.Div(id="dd-output-container"),
                                dbc.Table(
                                    [
                                        html.Thead(
                                            [
                                                html.Tr(
                                                    [
                                                        html.Th(rowSpan=2),
                                                        html.Th(
                                                            "Verification",
                                                            colSpan=4,
                                                        ),
                                                        html.Th(
                                                            "Validation",
                                                            colSpan=4,
                                                        ),
                                                        html.Th(
                                                            "Total",
                                                            colSpan=4,
                                                        ),
                                                    ],
                                                ),
                                                html.Tr(
                                                    [
                                                        html.Th("Pass"),
                                                        html.Th("Fail"),
                                                        html.Th("Total"),
                                                        html.Th("% Pass"),
                                                        html.Th("Pass"),
                                                        html.Th("Fail"),
                                                        html.Th("Total"),
                                                        html.Th("% Pass"),
                                                        html.Th("Pass"),
                                                        html.Th("Fail"),
                                                        html.Th("Total"),
                                                        html.Th("% Pass"),
                                                    ],
                                                ),
                                            ],
                                        ),
                                        html.Tbody(
                                            [
                                                html.Tr(
                                                    [
                                                        html.Th(
                                                            "Plausibility",
                                                            className="right",
                                                        ),
                                                        html.Td(id="verification_plausibility_pass"),
                                                        html.Td(id="verification_plausibility_fail"),
                                                        html.Td(id="verification_plausibility_total"),
                                                        html.Td(id="verification_plausibility_percent_pass"),
                                                        html.Td(id="validation_plausibility_pass"),
                                                        html.Td(id="validation_plausibility_fail"),
                                                        html.Td(id="validation_plausibility_total"),
                                                        html.Td(id="validation_plausibility_percent_pass"),
                                                        html.Td(id="plausibility_pass"),
                                                        html.Td(id="plausibility_fail"),
                                                        html.Td(id="plausibility_total"),
                                                        html.Td(id="plausibility_percent_pass"),
                                                    ],
                                                ),
                                                html.Tr(
                                                    [
                                                        html.Th(
                                                            "Conformance",
                                                            className="right",
                                                        ),
                                                        html.Td(id="verification_conformance_pass"),
                                                        html.Td(id="verification_conformance_fail"),
                                                        html.Td(id="verification_conformance_total"),
                                                        html.Td(id="verification_conformance_percent_pass"),
                                                        html.Td(id="validation_conformance_pass"),
                                                        html.Td(id="validation_conformance_fail"),
                                                        html.Td(id="validation_conformance_total"),
                                                        html.Td(id="validation_conformance_percent_pass"),
                                                        html.Td(id="conformance_pass"),
                                                        html.Td(id="conformance_fail"),
                                                        html.Td(id="conformance_total"),
                                                        html.Td(id="conformance_percent_pass"),
                                                    ],
                                                ),
                                                html.Tr(
                                                    [
                                                        html.Th(
                                                            "Completeness",
                                                            className="right",
                                                        ),
                                                        html.Td(id="verification_completeness_pass"),
                                                        html.Td(id="verification_completeness_fail"),
                                                        html.Td(id="verification_completeness_total"),
                                                        html.Td(id="verification_completeness_percent_pass"),
                                                        html.Td(id="validation_completeness_pass"),
                                                        html.Td(id="validation_completeness_fail"),
                                                        html.Td(id="validation_completeness_total"),
                                                        html.Td(id="validation_completeness_percent_pass"),
                                                        html.Td(id="completeness_pass"),
                                                        html.Td(id="completeness_fail"),
                                                        html.Td(id="completeness_total"),
                                                        html.Td(id="completeness_percent_pass"),
                                                    ],
                                                ),
                                                html.Tr(
                                                    [
                                                        html.Th("Total", className="right"),
                                                        html.Td(id="verification_pass"),
                                                        html.Td(id="verification_fail"),
                                                        html.Td(id="verification_total"),
                                                        html.Td(id="verification_percent_pass"),
                                                        html.Td(id="validation_pass"),
                                                        html.Td(id="validation_fail"),
                                                        html.Td(id="validation_total"),
                                                        html.Td(id="validation_percent_pass"),
                                                        html.Td(id="all_pass"),
                                                        html.Td(id="all_fail"),
                                                        html.Td(id="all_total"),
                                                        html.Td(
                                                            id="all_percent_pass",
                                                            className="total_percentage",
                                                        ),
                                                    ],
                                                ),
                                            ]
                                        ),
                                    ],
                                    className="center",
                                    bordered=True,
                                ),
                            ]
                        ),
                        html.Br(),
                        dbc.Row(
                            [
                                dash_table.DataTable(
                                    id="datatable-results",
                                    editable=False,
                                    filter_action="native",
                                    sort_action="native",
                                    sort_mode="multi",
                                    page_action="native",
                                    page_current=0,
                                    page_size=10,
                                    style_data={
                                        "whiteSpace": "normal",
                                        "height": "auto",
                                    },
                                    columns=[
                                        {"name": "Status", "id": "status"},
                                        {"name": "Table", "id": "cdm_table_name"},
                                        {"name": "Category", "id": "category"},
                                        {"name": "SubCategory", "id": "subcategory"},
                                        {"name": "Level", "id": "check_level"},
                                        {
                                            "name": "Description",
                                            "id": "check_description",
                                        },
                                        {
                                            "name": "% records",
                                            "id": "pct_violated_rows",
                                        },
                                        {"name": "SQL file", "id": "sql_file"},
                                    ],
                                    # hidden_columns=["sql_file"]
                                ),
                            ],
                            align="end",
                        ),
                        html.Br(),
                        dbc.Row(
                            [
                                dbc.Label(
                                    "Query:",
                                    width="auto",
                                ),
                                dcc.Textarea(
                                    id="textarea-query",
                                    style={"width": "100%", "height": 300},
                                ),
                            ],
                            align="end",
                        ),
                    ],
                    color="primary",
                ),
            ],
            fluid=True,
        )

    def _create_callbacks(self):
        self.app.callback(
            Output("verification_plausibility_pass", "children"),
            Output("verification_plausibility_fail", "children"),
            Output("verification_plausibility_fail", "class"),
            Output("verification_plausibility_total", "children"),
            Output("verification_plausibility_percent_pass", "children"),
            Output("verification_conformance_pass", "children"),
            Output("verification_conformance_fail", "children"),
            Output("verification_conformance_fail", "class"),
            Output("verification_conformance_total", "children"),
            Output("verification_conformance_percent_pass", "children"),
            Output("verification_completeness_pass", "children"),
            Output("verification_completeness_fail", "children"),
            Output("verification_completeness_fail", "class"),
            Output("verification_completeness_total", "children"),
            Output("verification_completeness_percent_pass", "children"),
            Output("verification_pass", "children"),
            Output("verification_fail", "children"),
            Output("verification_fail", "class"),
            Output("verification_total", "children"),
            Output("verification_percent_pass", "children"),
            Output("validation_plausibility_pass", "children"),
            Output("validation_plausibility_fail", "children"),
            Output("validation_plausibility_fail", "class"),
            Output("validation_plausibility_total", "children"),
            Output("validation_plausibility_percent_pass", "children"),
            Output("validation_conformance_pass", "children"),
            Output("validation_conformance_fail", "children"),
            Output("validation_conformance_fail", "class"),
            Output("validation_conformance_total", "children"),
            Output("validation_conformance_percent_pass", "children"),
            Output("validation_completeness_pass", "children"),
            Output("validation_completeness_fail", "children"),
            Output("validation_completeness_fail", "class"),
            Output("validation_completeness_total", "children"),
            Output("validation_completeness_percent_pass", "children"),
            Output("validation_pass", "children"),
            Output("validation_fail", "children"),
            Output("validation_fail", "class"),
            Output("validation_total", "children"),
            Output("validation_percent_pass", "children"),
            Output("plausibility_pass", "children"),
            Output("plausibility_fail", "children"),
            Output("plausibility_fail", "class"),
            Output("plausibility_total", "children"),
            Output("plausibility_percent_pass", "children"),
            Output("conformance_pass", "children"),
            Output("conformance_fail", "children"),
            Output("conformance_fail", "class"),
            Output("conformance_total", "children"),
            Output("conformance_percent_pass", "children"),
            Output("completeness_pass", "children"),
            Output("completeness_fail", "children"),
            Output("completeness_fail", "class"),
            Output("completeness_total", "children"),
            Output("completeness_percent_pass", "children"),
            Output("all_pass", "children"),
            Output("all_fail", "children"),
            Output("all_fail", "class"),
            Output("all_total", "children"),
            Output("all_percent_pass", "children"),
            Output("datatable-results", "data"),
            Input("runs-dropdown", "value"),
        )(self.run_selected)
        self.app.callback(
            Output("textarea-query", "value"),
            Input("datatable-results", "active_cell"),
            Input("datatable-results", "derived_virtual_indices"),
            Input("datatable-results", "data"),
        )(self.show_query)

    def show_query(self, active_cell, derived_virtual_indices, data):
        if not active_cell:
            return None

        full_row_index = derived_virtual_indices[active_cell["row"]]

        row = data[full_row_index]

        return row["query_text"]

    @abstractmethod
    def _get_last_runs(self) -> list[Any]:
        pass

    @abstractmethod
    def _get_run(self, id: str) -> Any:
        pass

    @abstractmethod
    def _get_results(self, run_id: str) -> pl.DataFrame:
        pass

    def run_selected(self, run_id):
        # run = self._get_run(run_id)
        df = self._get_results(run_id)
        df = df.with_columns(
            [
                pl.when((pl.col("failed") == 1)).then(pl.lit("FAILED")).otherwise(pl.lit("PASS")).alias("status"),
                pl.col("pct_violated_rows") * 100,
                pl.col("cdm_table_name").fill_null(pl.col("cdm_table_name")),
                pl.col("subcategory").fill_null(pl.lit("")),
            ]
        ).sort(
            ["status", "pct_violated_rows"],
            descending=[False, True],
        )

        verification_plausibility_pass = len(
            df.filter(
                (pl.col("failed") == 0) & (pl.col("context") == "Verification") & (pl.col("category") == "Plausibility")
            )
        )
        verification_plausibility_fail = len(
            df.filter(
                (pl.col("failed") == 1) & (pl.col("context") == "Verification") & (pl.col("category") == "Plausibility")
            )
        )
        verification_plausibility_total = len(
            df.filter((pl.col("context") == "Verification") & (pl.col("category") == "Plausibility"))
        )
        verification_plausibility_percent_pass = (
            f"{round(verification_plausibility_pass/verification_plausibility_total * 100)}%"
            if verification_plausibility_total != 0
            else "-"
        )

        verification_conformance_pass = len(
            df.filter(
                (pl.col("failed") == 0) & (pl.col("context") == "Verification") & (pl.col("category") == "Conformance")
            )
        )
        verification_conformance_fail = len(
            df.filter(
                (pl.col("failed") == 1) & (pl.col("context") == "Verification") & (pl.col("category") == "Conformance")
            )
        )
        verification_conformance_total = len(
            df.filter((pl.col("context") == "Verification") & (pl.col("category") == "Conformance"))
        )
        verification_conformance_percent_pass = (
            f"{round(verification_conformance_pass/verification_conformance_total * 100)}%"
            if verification_conformance_total != 0
            else "-"
        )

        verification_completeness_pass = len(
            df.filter(
                (pl.col("failed") == 0) & (pl.col("context") == "Verification") & (pl.col("category") == "Completeness")
            )
        )
        verification_completeness_fail = len(
            df.filter(
                (pl.col("failed") == 1) & (pl.col("context") == "Verification") & (pl.col("category") == "Completeness")
            )
        )
        verification_completeness_total = len(
            df.filter((pl.col("context") == "Verification") & (pl.col("category") == "Completeness"))
        )
        verification_completeness_percent_pass = (
            f"{round(verification_completeness_pass/verification_completeness_total * 100)}%"
            if verification_completeness_total != 0
            else "-"
        )

        verification_pass = len(df.filter((pl.col("failed") == 0) & (pl.col("context") == "Verification")))
        verification_fail = len(df.filter((pl.col("failed") == 1) & (pl.col("context") == "Verification")))
        verification_total = len(df.filter((pl.col("context") == "Verification")))
        verification_percent_pass = (
            f"{round(verification_pass/verification_total * 100)}%" if verification_total != 0 else "-"
        )

        validation_plausibility_pass = len(
            df.filter(
                (pl.col("failed") == 0) & (pl.col("context") == "Validation") & (pl.col("category") == "Plausibility")
            )
        )
        validation_plausibility_fail = len(
            df.filter(
                (pl.col("failed") == 1) & (pl.col("context") == "Validation") & (pl.col("category") == "Plausibility")
            )
        )
        validation_plausibility_total = len(
            df.filter((pl.col("context") == "Validation") & (pl.col("category") == "Plausibility"))
        )
        validation_plausibility_percent_pass = (
            f"{round(validation_plausibility_pass/validation_plausibility_total * 100)}%"
            if validation_plausibility_total != 0
            else "-"
        )

        validation_conformance_pass = len(
            df.filter(
                (pl.col("failed") == 0) & (pl.col("context") == "Validation") & (pl.col("category") == "Conformance")
            )
        )
        validation_conformance_fail = len(
            df.filter(
                (pl.col("failed") == 1) & (pl.col("context") == "Validation") & (pl.col("category") == "Conformance")
            )
        )
        validation_conformance_total = len(
            df.filter((pl.col("context") == "Validation") & (pl.col("category") == "Conformance"))
        )
        validation_conformance_percent_pass = (
            f"{round(validation_conformance_pass/validation_conformance_total * 100)}%"
            if validation_conformance_total != 0
            else "-"
        )

        validation_completeness_pass = len(
            df.filter(
                (pl.col("failed") == 0) & (pl.col("context") == "Validation") & (pl.col("category") == "Completeness")
            )
        )
        validation_completeness_fail = len(
            df.filter(
                (pl.col("failed") == 1) & (pl.col("context") == "Validation") & (pl.col("category") == "Completeness")
            )
        )
        validation_completeness_total = len(
            df.filter((pl.col("context") == "Validation") & (pl.col("category") == "Completeness"))
        )
        validation_completeness_percent_pass = (
            f"{round(validation_completeness_pass/validation_completeness_total * 100)}%"
            if validation_completeness_total != 0
            else "-"
        )

        validation_pass = len(df.filter((pl.col("failed") == 0) & (pl.col("context") == "Validation")))
        validation_fail = len(df.filter((pl.col("failed") == 1) & (pl.col("context") == "Validation")))
        validation_total = len(df.filter((pl.col("context") == "Validation")))
        validation_percent_pass = f"{round(validation_pass/validation_total * 100)}%" if validation_total != 0 else "-"

        plausibility_pass = len(df.filter((pl.col("failed") == 0) & (pl.col("category") == "Plausibility")))
        plausibility_fail = len(df.filter((pl.col("failed") == 1) & (pl.col("category") == "Plausibility")))
        plausibility_total = len(df.filter((pl.col("category") == "Plausibility")))
        plausibility_percent_pass = (
            f"{round(plausibility_pass/plausibility_total * 100)}%" if plausibility_total != 0 else "-"
        )

        conformance_pass = len(df.filter((pl.col("failed") == 0) & (pl.col("category") == "Conformance")))
        conformance_fail = len(df.filter((pl.col("failed") == 1) & (pl.col("category") == "Conformance")))
        conformance_total = len(df.filter((pl.col("category") == "Conformance")))
        conformance_percent_pass = (
            f"{round(conformance_pass/conformance_total * 100)}%" if conformance_total != 0 else "-"
        )

        completeness_pass = len(df.filter((pl.col("failed") == 0) & (pl.col("category") == "Completeness")))
        completeness_fail = len(df.filter((pl.col("failed") == 1) & (pl.col("category") == "Completeness")))
        completeness_total = len(df.filter((pl.col("category") == "Completeness")))
        completeness_percent_pass = (
            f"{round(completeness_pass/completeness_total * 100)}%" if completeness_total != 0 else "-"
        )

        all_pass = len(df.filter((pl.col("failed") == 0)))
        all_fail = len(df.filter((pl.col("failed") == 1)))
        all_total = len(df)
        all_percent_pass = f"{round(all_pass/all_total * 100)}%" if all_total != 0 else "-"

        return (
            verification_plausibility_pass,
            verification_plausibility_fail,
            "fail" if verification_plausibility_fail else None,
            verification_plausibility_total,
            verification_plausibility_percent_pass,
            verification_conformance_pass,
            verification_conformance_fail,
            "fail" if verification_conformance_fail else None,
            verification_conformance_total,
            verification_conformance_percent_pass,
            verification_completeness_pass,
            verification_completeness_fail,
            "fail" if verification_completeness_fail else None,
            verification_completeness_total,
            verification_completeness_percent_pass,
            verification_pass,
            verification_fail,
            "fail" if verification_fail else None,
            verification_total,
            verification_percent_pass,
            validation_plausibility_pass,
            validation_plausibility_fail,
            "fail" if validation_plausibility_fail else None,
            validation_plausibility_total,
            validation_plausibility_percent_pass,
            validation_conformance_pass,
            validation_conformance_fail,
            "fail" if validation_conformance_fail else None,
            validation_conformance_total,
            validation_conformance_percent_pass,
            validation_completeness_pass,
            validation_completeness_fail,
            "fail" if validation_completeness_fail else None,
            validation_completeness_total,
            validation_completeness_percent_pass,
            validation_pass,
            validation_fail,
            "fail" if validation_fail else None,
            validation_total,
            validation_percent_pass,
            plausibility_pass,
            plausibility_fail,
            "fail" if plausibility_fail else None,
            plausibility_total,
            plausibility_percent_pass,
            conformance_pass,
            conformance_fail,
            "fail" if conformance_fail else None,
            conformance_total,
            conformance_percent_pass,
            completeness_pass,
            completeness_fail,
            "fail" if completeness_fail else None,
            completeness_total,
            completeness_percent_pass,
            all_pass,
            all_fail,
            "fail" if all_fail else None,
            all_total,
            all_percent_pass,
            df.to_dicts(),
        )

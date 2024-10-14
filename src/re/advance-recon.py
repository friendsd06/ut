# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, sum as spark_sum, expr
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
import ipywidgets as widgets
from IPython.display import display, HTML, clear_output
from typing import List, Dict, Any, Tuple
from functools import partial
import re

spark = SparkSession.builder.appName("AdvancedReconciliationEngine").getOrCreate()

class DataSourceManager:
    @staticmethod
    def get_schema(source: str, is_delta: bool) -> StructType:
        try:
            if is_delta:
                return spark.read.format("delta").load(source).schema
            else:
                return spark.read.parquet(source).schema
        except Exception as e:
            raise ValueError(f"Error accessing source '{source}': {str(e)}")

    @staticmethod
    def get_data(source: str, is_delta: bool, columns: List[str]):
        try:
            if is_delta:
                return spark.read.format("delta").load(source).select(columns)
            else:
                return spark.read.parquet(source).select(columns)
        except Exception as e:
            raise ValueError(f"Error selecting columns from '{source}': {str(e)}")

class ReconciliationLogic:
    def __init__(self, prod_df, preprod_df, columns: List[str], key_columns: List[str]):
        self.prod_df = prod_df
        self.preprod_df = preprod_df
        self.columns = columns
        self.key_columns = key_columns

    def execute(self) -> Dict[str, Any]:
        return {
            'match_count': self._calculate_match_count(),
            'column_stats': self._calculate_column_stats(),
            'missing_attributes': self._find_missing_attributes(),
            'attribute_count': len(self.columns),
            'row_counts': self._get_row_counts(),
            'sample_mismatches': self._get_sample_mismatches()
        }

    def _calculate_match_count(self) -> int:
        return self.prod_df.intersect(self.preprod_df).count()

    def _calculate_column_stats(self) -> Dict[str, Dict[str, Any]]:
        def get_stats(df):
            return df.agg(*(
                    [spark_sum(when(col(c).isNotNull(), 1).otherwise(0)).alias(f"{c}_non_null_count") for c in self.columns] +
                    [spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"{c}_null_count") for c in self.columns] +
                    [count(col(c)).alias(f"{c}_total_count") for c in self.columns]
            )).collect()[0].asDict()

        prod_stats = get_stats(self.prod_df)
        preprod_stats = get_stats(self.preprod_df)

        return {
            'prod': {c: {k: prod_stats[f"{c}_{k}"] for k in ['non_null_count', 'null_count', 'total_count']} for c in self.columns},
            'preprod': {c: {k: preprod_stats[f"{c}_{k}"] for k in ['non_null_count', 'null_count', 'total_count']} for c in self.columns}
        }

    def _find_missing_attributes(self) -> Dict[str, List[str]]:
        prod_columns = set(self.prod_df.columns)
        preprod_columns = set(self.preprod_df.columns)
        return {
            'missing_in_prod': list(preprod_columns - prod_columns),
            'missing_in_preprod': list(prod_columns - preprod_columns)
        }

    def _get_row_counts(self) -> Dict[str, int]:
        return {
            'prod': self.prod_df.count(),
            'preprod': self.preprod_df.count()
        }

    def _get_sample_mismatches(self, sample_size: int = 5) -> List[Dict[str, Any]]:
        # Use window function to get a deterministic sample
        window = Window.partitionBy(lit(1)).orderBy(*self.key_columns)
        prod_with_row_num = self.prod_df.withColumn("row_num", expr("row_number() over (order by {})".format(", ".join(self.key_columns))))
        preprod_with_row_num = self.preprod_df.withColumn("row_num", expr("row_number() over (order by {})".format(", ".join(self.key_columns))))

        mismatches = prod_with_row_num.join(preprod_with_row_num, self.key_columns, "full_outer") \
            .where((prod_with_row_num.row_num != preprod_with_row_num.row_num) |
                   (prod_with_row_num.row_num.isNull()) |
                   (preprod_with_row_num.row_num.isNull()))

        return mismatches.drop("row_num").limit(sample_size).collect()

class BeautifulUI:
    def __init__(self):
        self.style = """
        <style>
            .widget-label { min-width: 20% !important; }
            .widget-text, .widget-select-multiple, .widget-dropdown { width: 80% !important; }
            .custom-button { width: 100% !important; margin-top: 10px !important; }
            .custom-output { margin-top: 20px !important; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #f2f2f2; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            .mismatch { background-color: #ffcccc; }
            .match { background-color: #ccffcc; }
        </style>
        """

    def create_text_widget(self, description: str) -> widgets.Text:
        return widgets.Text(description=description, style={'description_width': 'initial'})

    def create_dropdown_widget(self, options: List[str], description: str) -> widgets.Dropdown:
        return widgets.Dropdown(options=options, description=description, style={'description_width': 'initial'})

    def create_multiselect_widget(self, options: List[Tuple[str, str]], description: str) -> widgets.SelectMultiple:
        return widgets.SelectMultiple(options=options, description=description, style={'description_width': 'initial'})

    def create_button(self, description: str) -> widgets.Button:
        button = widgets.Button(description=description)
        button.add_class('custom-button')
        return button

    def display_output(self, content: str):
        display(HTML(self.style + content))

class ReconciliationApp:
    def __init__(self):
        self.ui = BeautifulUI()
        self.asset_classes = ['loan', 'deposit', 'commitment']
        self.asset_class_widget = self.ui.create_dropdown_widget(self.asset_classes, 'Asset Class:')
        self.source_type_widget = self.ui.create_dropdown_widget(['Delta Table', 'S3 Path'], 'Source Type:')
        self.prod_source_widget = self.ui.create_text_widget('Production Source:')
        self.preprod_source_widget = self.ui.create_text_widget('Pre-production Source:')
        self.run_button = self.ui.create_button("Run Reconciliation")
        self.output = widgets.Output()
        self.output.add_class('custom-output')

    def run(self):
        display(self.asset_class_widget, self.source_type_widget, self.prod_source_widget, self.preprod_source_widget, self.run_button, self.output)
        self.run_button.on_click(self._on_run_button_clicked)

    def _on_run_button_clicked(self, _):
        with self.output:
            clear_output()
            try:
                asset_class = self.asset_class_widget.value
                is_delta = self.source_type_widget.value == 'Delta Table'
                prod_source = self.prod_source_widget.value
                preprod_source = self.preprod_source_widget.value
                self._validate_input(asset_class, prod_source, preprod_source)

                prod_schema = DataSourceManager.get_schema(prod_source, is_delta)
                preprod_schema = DataSourceManager.get_schema(preprod_source, is_delta)

                column_widget = self.ui.create_multiselect_widget(
                    [(f.name, f.name) for f in prod_schema.fields],
                    'Select Columns:'
                )
                key_column_widget = self.ui.create_multiselect_widget(
                    [(f.name, f.name) for f in prod_schema.fields],
                    'Select Key Columns:'
                )
                confirm_button = self.ui.create_button("Confirm and Run")
                display(column_widget, key_column_widget, confirm_button)

                confirm_button.on_click(partial(self._on_confirm_button_clicked,
                                                prod_source=prod_source,
                                                preprod_source=preprod_source,
                                                is_delta=is_delta,
                                                column_widget=column_widget,
                                                key_column_widget=key_column_widget))
            except Exception as e:
                self.ui.display_output(f"<p style='color: red;'>Error: {str(e)}</p>")

    def _validate_input(self, asset_class: str, prod_source: str, preprod_source: str):
        if not asset_class or not prod_source or not preprod_source:
            raise ValueError("Please fill in all fields.")
        if not self._is_valid_source(prod_source) or not self._is_valid_source(preprod_source):
            raise ValueError("Invalid source format. Please enter a valid Delta table name or S3 path.")

    def _is_valid_source(self, source: str) -> bool:
        # Simple validation for Delta table name or S3 path
        return re.match(r'^[a-zA-Z0-9_\.]+$', source) or re.match(r'^s3://[a-zA-Z0-9_\.\-/]+$', source)

    def _on_confirm_button_clicked(self, _, prod_source: str, preprod_source: str, is_delta: bool,
                                   column_widget: widgets.SelectMultiple, key_column_widget: widgets.SelectMultiple):
        with self.output:
            clear_output()
            try:
                columns = list(column_widget.value)
                key_columns = list(key_column_widget.value)
                if not columns:
                    raise ValueError("Please select at least one column.")
                if not key_columns:
                    raise ValueError("Please select at least one key column.")

                prod_df = DataSourceManager.get_data(prod_source, is_delta, columns)
                preprod_df = DataSourceManager.get_data(preprod_source, is_delta, columns)

                engine = ReconciliationLogic(prod_df, preprod_df, columns, key_columns)
                results = engine.execute()

                self.ui.display_output(self._format_results(results))
            except Exception as e:
                self.ui.display_output(f"<p style='color: red;'>Error: {str(e)}</p>")

    def _format_results(self, results: Dict[str, Any]) -> str:
        html_output = f"""
        <h2>Reconciliation Results</h2>
        <table>
            <tr><th>Metric</th><th>Value</th></tr>
            <tr><td>Match Count</td><td>{results['match_count']}</td></tr>
            <tr><td>Attribute Count</td><td>{results['attribute_count']}</td></tr>
            <tr><td>Production Row Count</td><td>{results['row_counts']['prod']}</td></tr>
            <tr><td>Pre-production Row Count</td><td>{results['row_counts']['preprod']}</td></tr>
        </table>

        <h3>Column Statistics</h3>
        <table>
            <tr>
                <th>Column</th>
                <th>Environment</th>
                <th>Non-Null Count</th>
                <th>Null Count</th>
                <th>Total Count</th>
            </tr>
        """

        for col in results['column_stats']['prod'].keys():
            prod_stats = results['column_stats']['prod'][col]
            preprod_stats = results['column_stats']['preprod'][col]
            html_output += f"""
            <tr>
                <td rowspan="2">{col}</td>
                <td>Production</td>
                <td>{prod_stats['non_null_count']}</td>
                <td>{prod_stats['null_count']}</td>
                <td>{prod_stats['total_count']}</td>
            </tr>
            <tr>
                <td>Pre-production</td>
                <td>{preprod_stats['non_null_count']}</td>
                <td>{preprod_stats['null_count']}</td>
                <td>{preprod_stats['total_count']}</td>
            </tr>
            """

        html_output += """
        </table>

        <h3>Missing Attributes</h3>
        """

        html_output += f"<p><strong>Missing in Production:</strong> {', '.join(results['missing_attributes']['missing_in_prod']) or 'None'}</p>"
        html_output += f"<p><strong>Missing in Pre-production:</strong> {', '.join(results['missing_attributes']['missing_in_preprod']) or 'None'}</p>"

        if results['sample_mismatches']:
            html_output += """
            <h3>Sample Mismatches</h3>
            <table>
                <tr>
            """
            for col in results['sample_mismatches'][0].asDict().keys():
                html_output += f"<th>{col}</th>"
            html_output += "</tr>"

            for row in results['sample_mismatches']:
                html_output += "<tr>"
                for value in row.asDict().values():
                    html_output += f"<td>{value}</td>"
                html_output += "</tr>"

            html_output += "</table>"

        return html_output

# Main execution
ReconciliationApp().run()
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode_outer, coalesce, lit
from typing import List, Optional, Dict
from databricks.widgets import get_widget_value
import json

def load_config(config_path: str) -> Dict:
    with open(config_path, 'r') as config_file:
        return json.load(config_file)

def get_dataset_config(configs: Dict, dataset_id: str) -> Dict:
    return configs.get(dataset_id, {})

def flatten_nested_entity(df: DataFrame, nested_column: str, primary_keys: List[str]) -> DataFrame:
    exploded = df.select(*primary_keys, explode_outer(col(nested_column)).alias("_tmp"))
    flattened_cols = [col(c) for c in primary_keys] + [col("_tmp.*")]
    return exploded.select(*flattened_cols)

def reconcile_dataframes(
        source_df: DataFrame,
        target_df: DataFrame,
        join_keys: List[str],
        columns_to_compare: Optional[List[str]] = None,
        columns_to_include: Optional[List[str]] = None,
        columns_to_ignore: Optional[List[str]] = None
) -> DataFrame:
    if not isinstance(source_df, DataFrame) or not isinstance(target_df, DataFrame):
        raise ValueError("Both source_df and target_df must be Spark DataFrames.")

    all_columns = set(source_df.columns + target_df.columns)
    columns_to_process = list(all_columns - set(join_keys) - set(columns_to_ignore or []))

    if columns_to_compare:
        columns_to_process = [col for col in columns_to_process if col in columns_to_compare]

    if columns_to_include:
        columns_to_process += columns_to_include

    source_selected = source_df.select(join_keys + columns_to_process)
    target_selected = target_df.select(join_keys + columns_to_process)

    joined = source_selected.join(
        target_selected,
        on=join_keys,
        how="full_outer"
    )

    for column in columns_to_process:
        source_col = col(f"source_df.{column}")
        target_col = col(f"target_df.{column}")
        joined = joined.withColumn(
            f"{column}_match",
            coalesce(source_col, lit("NULL")) == coalesce(target_col, lit("NULL"))
        )

    return joined

def reconcile_nested_entities(
        spark: SparkSession,
        source_df: DataFrame,
        target_df: DataFrame,
        config: Dict
) -> DataFrame:
    parent_keys = config['parent_keys']
    child_keys = config['child_keys']
    nested_column = config['nested_column']

    # Reconcile parent entities
    parent_reconciled = reconcile_dataframes(
        source_df,
        target_df,
        join_keys=parent_keys,
        columns_to_compare=config.get('parent_columns_to_compare'),
        columns_to_include=config.get('parent_columns_to_include'),
        columns_to_ignore=config.get('parent_columns_to_ignore')
    )

    # Flatten and reconcile child entities
    source_flattened = flatten_nested_entity(source_df, nested_column, parent_keys)
    target_flattened = flatten_nested_entity(target_df, nested_column, parent_keys)

    child_reconciled = reconcile_dataframes(
        source_flattened,
        target_flattened,
        join_keys=parent_keys + child_keys,
        columns_to_compare=config.get('child_columns_to_compare'),
        columns_to_include=config.get('child_columns_to_include'),
        columns_to_ignore=config.get('child_columns_to_ignore')
    )

    # Combine parent and child reconciliation results
    unified_report = parent_reconciled.join(
        child_reconciled,
        on=parent_keys,
        how="left"
    )

    return unified_report

def main(spark: SparkSession, source_df: DataFrame, target_df: DataFrame, dataset_id: str, config_path: str) -> DataFrame:
    # Load configurations
    configs = load_config(config_path)

    # Get configuration for the dataset
    config = get_dataset_config(configs, dataset_id)

    if not config:
        raise ValueError(f"No configuration found for dataset ID: {dataset_id}")

    # Perform nested entity reconciliation
    result_df = reconcile_nested_entities(spark, source_df, target_df, config)

    return result_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("NestedEntityReconciliation").getOrCreate()

    # Get dataset ID and config path from Databricks widgets
    dataset_id = get_widget_value("dataset_id")
    config_path = get_widget_value("config_path")

    # In a real scenario, you would pass these DataFrames to the main function
    # Here, we're using dummy data loading for illustration
    source_df = spark.read.parquet("path/to/source/data")
    target_df = spark.read.parquet("path/to/target/data")

    result_df = main(spark, source_df, target_df, dataset_id, config_path)

    # Display or save results as needed
    result_df.show()
    # result_df.write.parquet("path/to/output")
# scripts/run_reconciliation.py

import yaml
import logging
from pyspark.sql import SparkSession
from src.reconciliation import compare_delta_tables
from src.utils import setup_logging
import os

def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("MultipleAssetsReconciliation") \
        .getOrCreate()

    # Setup Logging
    log_file = "logs/reconciliation.log"
    setup_logging(log_file)
    logger = logging.getLogger("ReconciliationLogger")

    # Load Configuration
    config_path = "config/assets_config.yaml"
    config = load_config(config_path)

    assets = config.get("assets", [])

    if not assets:
        logger.error("No assets found in configuration.")
        spark.stop()
        return

    for asset in assets:
        try:
            asset_name = asset['name']
            source_table = asset['source_table']
            target_table = asset['target_table']
            primary_key = asset['primary_key']
            columns_to_compare = asset.get('columns_to_compare')
            columns_to_include = asset.get('columns_to_include')
            columns_to_ignore = asset.get('columns_to_ignore')

            logger.info(f"Starting reconciliation for asset: {asset_name}")

            # Read Source and Target DataFrames
            source_df = spark.table(source_table)
            target_df = spark.table(target_table)

            # Perform Reconciliation
            detailed_mismatches, summary_df = compare_delta_tables(
                source_df=source_df,
                target_df=target_df,
                primary_key=primary_key,
                columns_to_compare=columns_to_compare,
                columns_to_include=columns_to_include,
                columns_to_ignore=columns_to_ignore
            )

            # Persist Results
            # Define paths
            mismatches_path = f"results/mismatches/{asset_name}_mismatches.parquet"
            summaries_path = f"results/summaries/{asset_name}_summary.parquet"

            # Write detailed mismatches
            detailed_mismatches.write.mode("overwrite").parquet(mismatches_path)
            logger.info(f"Detailed mismatches for {asset_name} saved to {mismatches_path}")

            # Write summary statistics
            summary_df.write.mode("overwrite").parquet(summaries_path)
            logger.info(f"Summary statistics for {asset_name} saved to {summaries_path}")

        except Exception as e:
            logger.error(f"Error processing asset {asset.get('name', 'Unknown')}: {str(e)}")

    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()
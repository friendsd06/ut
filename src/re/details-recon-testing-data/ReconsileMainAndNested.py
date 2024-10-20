from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Main and Nested Entity Reconciliation") \
    .getOrCreate()

# Sample main entity DataFrame
data_main = [
    (1, "Alice", 1000),
    (2, "Bob", 2000),
    (3, "Charlie", 3000)
]
schema_main = "id INT, name STRING, balance INT"
df_main = spark.createDataFrame(data_main, schema=schema_main)

# Sample nested entity DataFrame
data_nested = [
    (1, [{"type": "home", "value": "NY"}, {"type": "work", "value": "LA"}]),
    (2, [{"type": "home", "value": "SF"}]),
    (3, [{"type": "work", "value": "CHI"}])
]
schema_nested = "id INT, addresses ARRAY<STRUCT<type: STRING, value: STRING>>"
df_nested = spark.createDataFrame(data_nested, schema=schema_nested)

# Example JSON configuration
json_config = {"nested_key": "addresses"}  # Can be empty or missing

def validate_config(config: dict) -> str:
    """
    Validate the user-provided JSON configuration.
    Returns the nested key if present and valid, otherwise returns an empty string.
    """
    nested_key = config.get("nested_key", "")
    if not nested_key or not isinstance(nested_key, str):
        return ""
    return nested_key

def flatten_nested_df(df: DataFrame, nested_key: str) -> DataFrame:
    """
    Flatten the nested column in the DataFrame if the nested key is present.
    Returns the flattened DataFrame.
    """
    return df.withColumn(nested_key, explode(col(nested_key)))

def reconcile_main_only(df_main: DataFrame) -> DataFrame:
    """
    Reconcile only the main entity DataFrame.
    """
    print("\nPerforming Reconciliation on Main Entity Only:")
    return df_main

def reconcile_main_and_nested(df_main: DataFrame, df_nested: DataFrame, nested_key: str) -> DataFrame:
    """
    Flatten and join the main and nested entity DataFrames based on the nested key.
    """
    df_nested_flattened = flatten_nested_df(df_nested, nested_key)

    # Renaming nested columns to avoid ambiguity
    nested_columns = [f"{nested_key}_{col_name}" for col_name in df_nested_flattened.columns if col_name != "id"]
    df_nested_flattened = df_nested_flattened.select("id", *[col(nested_key).alias(nested_col) for nested_col in nested_columns])

    # Join main and nested DataFrames
    df_reconciled = df_main.join(df_nested_flattened, on="id", how="left")

    print("\nPerforming Reconciliation on Main and Nested Entity:")
    return df_reconciled

def reconcile_entities(df_main: DataFrame, df_nested: DataFrame, config: dict) -> DataFrame:
    """
    Main function to handle the reconciliation based on the provided configuration.
    """
    nested_key = validate_config(config)

    if not nested_key:
        # Reconcile only main entity if the nested key is missing or invalid
        return reconcile_main_only(df_main)

    # Reconcile both main and nested entities
    return reconcile_main_and_nested(df_main, df_nested, nested_key)

# Apply the reconciliation logic
df_final = reconcile_entities(df_main, df_nested, json_config)

# Show the final DataFrame
print("\nFinal Reconciled DataFrame:")
df_final.show(truncate=False)

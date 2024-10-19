from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
from pyspark.sql.functions import (
    col, coalesce, when, lit, concat, concat_ws, broadcast
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DataFrameReconciliation") \
    .config("spark.sql.shuffle.partitions", "400")  # Adjust based on your cluster
.getOrCreate()

# Define the schema for nested structures
address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zipcode", StringType(), True)
])

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("payment_date", StringType(), True),
    StructField("method", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# Create sample data for source_df
source_data = [
    {
        "parent_primary_key": "P1",
        "child_primary_key": "C1",
        "name": "Alice",
        "age": 30,
        "address": {"street": "123 Maple St", "city": "Springfield", "zipcode": "12345"},
        "orders": [
            {"order_id": "O1001", "order_date": "2023-01-10", "amount": 250.0, "status": "Shipped"},
            {"order_id": "O1002", "order_date": "2023-02-15", "amount": 150.0, "status": "Processing"}
        ],
        "payments": [
            {"payment_id": "PM2001", "payment_date": "2023-01-11", "method": "Credit Card", "amount": 250.0}
        ]
    },
    {
        "parent_primary_key": "P2",
        "child_primary_key": "C2",
        "name": "Bob",
        "age": 25,
        "address": {"street": "456 Oak St", "city": "Shelbyville", "zipcode": "67890"},
        "orders": [
            {"order_id": "O1003", "order_date": "2023-03-20", "amount": 300.0, "status": "Delivered"}
        ],
        "payments": [
            {"payment_id": "PM2002", "payment_date": "2023-03-21", "method": "PayPal", "amount": 300.0}
        ]
    }
]

# Create sample data for target_df with some differences
target_data = [
    {
        "parent_primary_key": "P1",
        "child_primary_key": "C1",
        "name": "Alice",
        "age": 31,  # Age difference
        "address": {"street": "123 Maple St", "city": "Springfield", "zipcode": "12345"},
        "orders": [
            {"order_id": "O1001", "order_date": "2023-01-10", "amount": 250.0, "status": "Shipped"},
            {"order_id": "O1002", "order_date": "2023-02-15", "amount": 175.0, "status": "Completed"}  # Amount and status difference
        ],
        "payments": [
            {"payment_id": "PM2001", "payment_date": "2023-01-11", "method": "Credit Card", "amount": 250.0}
        ]
    },
    {
        "parent_primary_key": "P2",
        "child_primary_key": "C2",
        "name": "Bob",
        "age": 25,
        "address": {"street": "456 Oak St", "city": "Shelbyville", "zipcode": "67890"},
        "orders": [
            {"order_id": "O1003", "order_date": "2023-03-20", "amount": 300.0, "status": "Delivered"}
        ],
        "payments": [
            {"payment_id": "PM2002", "payment_date": "2023-03-21", "method": "Credit Card", "amount": 300.0}  # Payment method difference
        ]
    },
    {
        "parent_primary_key": "P3",
        "child_primary_key": "C3",
        "name": "Charlie",
        "age": 28,
        "address": {"street": "789 Pine St", "city": "Capital City", "zipcode": "54321"},
        "orders": [],
        "payments": []
    }
]

# Create DataFrames
source_df = spark.createDataFrame(source_data, schema="parent_primary_key STRING, child_primary_key STRING, name STRING, age INT, address STRUCT<street: STRING, city: STRING, zipcode: STRING>, orders ARRAY<STRUCT<order_id: STRING, order_date: STRING, amount: DOUBLE, status: STRING>>, payments ARRAY<STRUCT<payment_id: STRING, payment_date: STRING, method: STRING, amount: DOUBLE>>>")

target_df = spark.createDataFrame(target_data, schema="parent_primary_key STRING, child_primary_key STRING, name STRING, age INT, address STRUCT<street: STRING, city: STRING, zipcode: STRING>, orders ARRAY<STRUCT<order_id: STRING, order_date: STRING, amount: DOUBLE, status: STRING>>, payments ARRAY<STRUCT<payment_id: STRING, payment_date: STRING, method: STRING, amount: DOUBLE>>>")

def identify_column_types(df):
    """
    Identify scalar and struct (nested) columns in the DataFrame.

    :param df: Input DataFrame
    :return: Tuple of (scalar_columns, struct_columns)
    """
    struct_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StructType)]
    scalar_cols = [field.name for field in df.schema.fields if not isinstance(field.dataType, StructType)]
    return scalar_cols, struct_cols

def add_prefix(df, prefix):
    """
    Add a prefix to all column names in the DataFrame.

    :param df: Input DataFrame
    :param prefix: Prefix string (e.g., 'source_', 'target_')
    :return: DataFrame with prefixed column names
    """
    return df.select([col(column).alias(f"{prefix}{column}") for column in df.columns])

def flatten_nested_columns(df, nested_columns):
    """
    Flatten specified nested (struct) columns by extracting their attributes one level deep.

    :param df: Input DataFrame
    :param nested_columns: List of nested column names to flatten
    :return: Flattened DataFrame
    """
    for nested_col in nested_columns:
        # Check if the nested_col is indeed a struct
        if nested_col not in df.columns:
            continue  # Skip if the column does not exist
        if not isinstance(df.schema[nested_col].dataType, StructType):
            continue  # Skip if the column is not a struct

        # Extract sub-fields from the nested struct
        sub_fields = df.select(f"{nested_col}.*").columns
        for sub_field in sub_fields:
            # Create new columns with a combined name
            df = df.withColumn(f"{nested_col}_{sub_field}", col(f"{nested_col}.{sub_field}"))
        # Drop the original nested column
        df = df.drop(nested_col)
    return df

def compare_scalars(joined_df, scalar_cols):
    """
    Compare scalar columns between source and target DataFrames and generate difference columns.

    :param joined_df: Joined DataFrame with prefixed columns
    :param scalar_cols: List of scalar column names
    :return: List of comparison Column expressions
    """
    comparisons = []
    for column in scalar_cols:
        source_col = col(f"source_{column}")
        target_col = col(f"target_{column}")
        diff_col = when(
            ~source_col.eqNullSafe(target_col),
            concat(
                lit(f"{column}: Source = "), coalesce(source_col.cast("string"), lit("null")),
                lit(", Target = "), coalesce(target_col.cast("string"), lit("null"))
            )
        ).alias(f"{column}_diff")
        comparisons.append(diff_col)
    return comparisons

def compare_flattened_nested_columns(joined_df, nested_columns):
    """
    Compare attributes of flattened nested columns and generate difference columns.

    :param joined_df: Joined DataFrame with prefixed columns
    :param nested_columns: List of nested column names that have been flattened
    :return: List of comparison Column expressions
    """
    comparisons = []
    for nested_col in nested_columns:
        # Identify all attributes for this nested_col by extracting column names that start with nested_col_
        nested_attributes = [c for c in joined_df.columns if c.startswith(f"{nested_col}_") and not c.startswith(f"source_{nested_col}_") and not c.startswith(f"target_{nested_col}_")]

        # Extract unique attribute names by removing the nested_col prefix
        attribute_names = set([attr.replace(f"{nested_col}_", "") for attr in nested_attributes])

        sub_field_comparisons = []
        for attribute in attribute_names:
            source_attr = col(f"source_{nested_col}_{attribute}")
            target_attr = col(f"target_{nested_col}_{attribute}")
            diff_expression = when(
                ~source_attr.eqNullSafe(target_attr),
                concat(
                    lit(f"{nested_col}.{attribute}: Source = "), coalesce(source_attr.cast("string"), lit("null")),
                    lit(", Target = "), coalesce(target_attr.cast("string"), lit("null"))
                )
            ).otherwise("")
            sub_field_comparisons.append(diff_expression)

        # Concatenate all differences for this nested column
        struct_diff_col = concat_ws(", ", *sub_field_comparisons).alias(f"{nested_col}_diff")
        comparisons.append(struct_diff_col)
    return comparisons

def compare_dataframes(
        source_df,
        target_df,
        primary_keys,
        nested_columns_to_flatten=[]
):
    """
    Compare two DataFrames (source and target) based on primary keys,
    including handling of nested columns by flattening them one level.

    :param source_df: Source DataFrame
    :param target_df: Target DataFrame
    :param primary_keys: List of primary key column names
    :param nested_columns_to_flatten: List of nested column names to flatten
    :return: Comparison DataFrame highlighting differences
    """

    # Identify scalar and struct columns in source DataFrame
    scalar_cols, struct_cols = identify_column_types(source_df)

    # If there are nested columns to flatten, process them
    if nested_columns_to_flatten:
        # Flatten nested columns in source and target DataFrames
        source_df = flatten_nested_columns(source_df, nested_columns_to_flatten)
        target_df = flatten_nested_columns(target_df, nested_columns_to_flatten)

        # Update scalar and struct columns after flattening
        scalar_cols, struct_cols = identify_column_types(source_df)

    # Select only necessary columns to reduce data size
    necessary_columns = primary_keys + scalar_cols + struct_cols + nested_columns_to_flatten
    # Ensure columns exist before selecting
    necessary_columns = [col for col in necessary_columns if col in source_df.columns]
    source_df = source_df.select(necessary_columns)
    target_df = target_df.select(necessary_columns)

    # Add prefixes to distinguish source and target columns
    source_df_prefixed = add_prefix(source_df, "source_")
    target_df_prefixed = add_prefix(target_df, "target_")

    # Repartition DataFrames on primary keys to optimize join
    num_partitions = 400  # Adjust based on cluster resources
    source_df_prefixed = source_df_prefixed.repartition(
        num_partitions, *[f"source_{pk}" for pk in primary_keys]
    )
    target_df_prefixed = target_df_prefixed.repartition(
        num_partitions, *[f"target_{pk}" for pk in primary_keys]
    )

    # Cache DataFrames if reused
    source_df_prefixed.cache()
    target_df_prefixed.cache()

    # Create join conditions based on primary keys using eqNullSafe
    join_conditions = [
        col(f"source_{pk}").eqNullSafe(col(f"target_{pk}")) for pk in primary_keys
    ]

    # Perform full outer join on primary keys
    joined_df = source_df_prefixed.join(target_df_prefixed, on=join_conditions, how="full_outer")

    # Compare scalar columns
    scalar_comparisons = compare_scalars(joined_df, scalar_cols)

    # Compare struct columns (if any)
    struct_comparisons = []
    if struct_cols:
        for struct_col in struct_cols:
            source_struct = col(f"source_{struct_col}")
            target_struct = col(f"target_{struct_col}")
            diff_col = when(
                ~source_struct.eqNullSafe(target_struct),
                concat(
                    lit(f"{struct_col}: Source = "), coalesce(source_struct.cast("string"), lit("null")),
                    lit(", Target = "), coalesce(target_struct.cast("string"), lit("null"))
                )
            ).alias(f"{struct_col}_diff")
            struct_comparisons.append(diff_col)

    # Compare flattened nested columns (if any)
    nested_comparisons = []
    if nested_columns_to_flatten:
        nested_comparisons = compare_flattened_nested_columns(joined_df, nested_columns_to_flatten)

    # Prepare primary key columns for output by coalescing source and target
    primary_key_cols = [
        coalesce(col(f"source_{pk}"), col(f"target_{pk}")).alias(pk) for pk in primary_keys
    ]

    # Select primary keys and all comparison results
    final_df = joined_df.select(
        *primary_key_cols,
        *scalar_comparisons,
        *struct_comparisons,
        *nested_comparisons
    )

    return final_df

# Define primary keys
primary_keys = ['parent_primary_key', 'child_primary_key']

# Define nested columns to flatten
nested_columns_to_flatten = ['orders', 'payments']

# Perform the comparison
result_df = compare_dataframes(
    source_df=source_df,
    target_df=target_df,
    primary_keys=primary_keys,
    nested_columns_to_flatten=nested_columns_to_flatten
)

# Display the comparison results
result_df.show(truncate=False)

# Optionally, write the results to a file in Parquet format
# result_df.write.mode("overwrite").parquet("path_to_output_data")

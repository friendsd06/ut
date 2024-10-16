# Import necessary PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit, sum as _sum, count as _count
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("EnhancedNestedRecon") \
    .master("local[*]") \
    .getOrCreate()

def generate_flatten_sql(df, table_name, columns_to_explode=None):
    """
    Generates a SQL query to flatten a nested DataFrame schema iteratively using a stack.

    :param df: Input DataFrame with nested schema
    :param table_name: Name of the table to generate SQL for
    :param columns_to_explode: List of column names to explode. If None, explode all array columns.
    :return: Flattened SQL query as a string
    """
    select_expressions = []
    lateral_view_clauses = []
    alias_counter = 0
    stack = [(None, field.name, field.dataType) for field in df.schema.fields]

    # If specific columns to explode are provided, mark them
    explode_columns = set(columns_to_explode) if columns_to_explode else set()

    while stack:
        parent, field, field_type = stack.pop()
        alias_counter += 1

        # Determine the full field path
        full_field = f"{parent}.{field}" if parent else field

        if isinstance(field_type, StructType):
            for subfield in field_type.fields:
                sub_full_field = f"{full_field}.{subfield.name}"
                alias_name = f"{full_field.replace('.', '_')}_{subfield.name}"
                select_expressions.append(f"{sub_full_field} AS {alias_name}")
                stack.append((full_field, subfield.name, subfield.dataType))

        elif isinstance(field_type, ArrayType):
            # Check if the current array field should be exploded
            if not columns_to_explode or field in explode_columns:
                exploded_alias = f"{field}_exploded_{alias_counter}"
                explode_clause = f"LATERAL VIEW OUTER EXPLODE({full_field}) {exploded_alias}"
                lateral_view_clauses.append(explode_clause)

                # If the elements are structs, push their fields to the stack
                if isinstance(field_type.elementType, StructType):
                    for subfield in field_type.elementType.fields:
                        sub_full_field = f"{exploded_alias}.{subfield.name}"
                        alias_name = f"{exploded_alias}_{subfield.name}"
                        select_expressions.append(f"{sub_full_field} AS {alias_name}")
                        stack.append((exploded_alias, subfield.name, subfield.dataType))
                else:
                    # For non-struct array elements, sort and coalesce
                    sort_expr = (
                        f"SORT_ARRAY(COALESCE({exploded_alias}, ARRAY())) AS {full_field.replace('.', '_')}"
                    )
                    select_expressions.append(sort_expr)
            else:
                # If not exploding, treat it as a regular column
                sort_expr = (
                    f"SORT_ARRAY(COALESCE({full_field}, ARRAY())) AS {full_field.replace('.', '_')}"
                )
                select_expressions.append(sort_expr)

        else:
            select_expr = f"{full_field} AS {full_field.replace('.', '_')}"
            select_expressions.append(select_expr)

    select_clause = ", ".join(select_expressions)
    lateral_view_clause = " ".join(lateral_view_clauses)
    sql_query = f"SELECT {select_clause} FROM {table_name} {lateral_view_clause}"

    return sql_query

def reconcile_datasets(source_df, target_df, join_keys, columns_to_explode=None):
    """
    Reconciles two datasets by comparing flattened versions and identifying discrepancies.

    :param source_df: Source DataFrame
    :param target_df: Target DataFrame
    :param join_keys: List of column names to join on
    :param columns_to_explode: List of column names to explode during flattening
    :return: Tuple containing DataFrames for match/mismatch counts and detailed mismatch reports
    """
    # Create temporary views
    source_df.createOrReplaceTempView("source_table")
    target_df.createOrReplaceTempView("target_table")

    # Generate flattened SQL queries
    source_sql = generate_flatten_sql(source_df, "source_table", columns_to_explode)
    target_sql = generate_flatten_sql(target_df, "target_table", columns_to_explode)

    print("Source Flattened SQL Query:")
    print(source_sql)
    print("\nTarget Flattened SQL Query:")
    print(target_sql)

    # Execute SQL queries to get flattened DataFrames
    flattened_source_df = spark.sql(source_sql)
    flattened_target_df = spark.sql(target_sql)

    # Perform full outer join on the specified join keys
    join_conditions = [flattened_source_df[jk] == flattened_target_df[jk] for jk in join_keys]
    joined_df = flattened_source_df.alias("src").join(
        flattened_target_df.alias("tgt"),
        on=join_conditions,
        how="full_outer"
    )

    # Initialize lists to hold comparison expressions and mismatch details
    comparison_exprs = []
    mismatch_details = []

    # Collect all columns except join keys for comparison
    comparison_columns = [c for c in flattened_source_df.columns if c not in [fk.replace('.', '_') for fk in join_keys]]

    for col_name in comparison_columns:
        src_col = f"src.{col_name}"
        tgt_col = f"tgt.{col_name}"
        diff_col = f"diff_{col_name}"

        # Comparison expression to identify mismatches
        comparison_expr = when(
            col(src_col) != col(tgt_col),
            lit(1)
        ).otherwise(lit(0)).alias(diff_col)
        comparison_exprs.append(comparison_expr)

        # Capture mismatch details
        detail_expr = when(
            col(src_col) != col(tgt_col),
            expr(f"concat('{col_name}: ', src.{col_name}, ' != ', tgt.{col_name})")
        )
        mismatch_details.append(detail_expr.alias(f"detail_{col_name}"))

    # Select key columns and comparison expressions
    select_cols = [col(f"src.{jk}").alias(jk) for jk in join_keys] + comparison_exprs + mismatch_details
    comparison_df = joined_df.select(*select_cols)

    # Aggregate to get match and mismatch counts
    # Sum all diff columns to get total mismatches
    total_mismatches_expr = _sum(
        _sum(col(f"diff_{c}") for c in comparison_columns)
    ).alias("total_mismatches")

    # Count total rows
    total_rows_expr = _count(lit(1)).alias("total_rows")

    # Sum all diff columns and calculate total matches
    # total_matches = total_columns * total_rows - total_mismatches
    counts_df = comparison_df.agg(
        _sum(*[col(f"diff_{c}") for c in comparison_columns]).alias("total_mismatches"),
        _count(lit(1)).alias("total_rows")
    )

    # Calculate total_matches as total_rows * total_columns - total_mismatches
    # Alternatively, for per-row matching, you can count rows with total_mismatches == 0
    # Here, we'll count how many rows have no mismatches
    match_count_df = comparison_df.filter(
        sum([col(f"diff_{c}") for c in comparison_columns]) == 0
    ).count()

    mismatch_count_df = counts_df.collect()[0]["total_mismatches"]

    total_rows = counts_df.collect()[0]["total_rows"]
    total_columns = len(comparison_columns)
    total_matches = total_rows * total_columns - mismatch_count_df

    # Collect mismatch details
    mismatch_columns = [f"detail_{c}" for c in comparison_columns]
    # Create a condition to check if any detail column is not null
    mismatch_condition = " OR ".join([f"`{c}` IS NOT NULL" for c in mismatch_columns])
    detailed_mismatches_df = comparison_df.select(*join_keys, *mismatch_columns) \
        .filter(expr(mismatch_condition))

    # Show counts
    print("\nReconciliation Counts:")
    counts_data = {
        "total_mismatches": mismatch_count_df,
        "total_matches": total_matches
    }
    counts_df_final = spark.createDataFrame([counts_data])
    counts_df_final.show(truncate=False)

    # Show detailed mismatches
    print("\nDetailed Mismatches:")
    detailed_mismatches_df.show(truncate=False)

    return counts_df_final, detailed_mismatches_df

# Sample complex nested data for source and target
data_source = [
    (
        1,
        {
            "name": "John Doe",
            "age": 30,
            "contact": {"email": "john@example.com", "phone": "123-456-7890"}
        },
        [
            {
                "order_id": 101,
                "order_items": [{"product": "Laptop", "quantity": 1}],
                "payment_details": {
                    "method": "Credit Card",
                    "billing_address": {"city": "New York"}
                }
            }
        ],
        [{"category": "electronics", "tags": ["tech", "gadgets"]}]
    )
]

data_target = [
    (
        1,
        {
            "name": "John Doe",
            "age": 31,  # Note the age difference here
            "contact": {"email": "john@example.com", "phone": "123-456-7890"}
        },
        [
            {
                "order_id": 101,
                "order_items": [{"product": "Laptop", "quantity": 1}],
                "payment_details": {
                    "method": "Credit Card",
                    "billing_address": {"city": "New York"}
                }
            }
        ],
        [{"category": "electronics", "tags": ["tech", "gadgets"]}]
    )
]

# Define the schema for the DataFrames
schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_info", StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("contact", StructType([
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True)
        ]), True)
    ]), True),
    StructField("orders", ArrayType(StructType([
        StructField("order_id", IntegerType(), True),
        StructField("order_items", ArrayType(StructType([
            StructField("product", StringType(), True),
            StructField("quantity", IntegerType(), True)
        ])), True),
        StructField("payment_details", StructType([
            StructField("method", StringType(), True),
            StructField("billing_address", StructType([
                StructField("city", StringType(), True)
            ]), True)
        ]), True)
    ])), True),
    StructField("preferences", ArrayType(StructType([
        StructField("category", StringType(), True),
        StructField("tags", ArrayType(StringType()), True)
    ])), True)
])

# Create DataFrames for source and target
source_df = spark.createDataFrame(data_source, schema)
target_df = spark.createDataFrame(data_target, schema)

# Specify columns to explode (e.g., 'orders', 'preferences')
columns_to_explode = ["orders", "preferences"]

# Perform reconciliation on the specified join key and columns to explode
match_counts_df, mismatch_details_df = reconcile_datasets(
    source_df,
    target_df,
    join_keys=["customer_id"],
    columns_to_explode=columns_to_explode
)

# Stop Spark session
spark.stop()

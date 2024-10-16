# Import necessary PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit
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
    :return: DataFrame containing reconciliation report with match/mismatch counts and details
    """
    # Create temporary views
    source_df.createOrReplaceTempView("source_table")
    target_df.createOrReplaceTempView("target_table")

    # Generate flattened SQL queries
    source_sql = generate_flatten_sql(source_df, "source_table", columns_to_explode)
    target_sql = generate_flatten_sql(target_df, "target_table", columns_to_explode)

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

    for col_name in flattened_source_df.columns:
        if col_name not in [fk.replace('.', '_') for fk in join_keys]:
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

    # Calculate total mismatches and matches
    total_mismatches = sum([col(f"diff_{c}") for c in flattened_source_df.columns
                            if c not in [fk.replace('.', '_') for fk in join_keys]])
    total_columns = len([c for c in flattened_source_df.columns
                         if c not in [fk.replace('.', '_') for fk in join_keys]])

    # Select key columns and comparison expressions
    select_cols = [col(f"src.{jk}").alias(jk) for jk in join_keys] + comparison_exprs + mismatch_details
    comparison_df = joined_df.select(*select_cols)

    # Aggregate to get match and mismatch counts
    agg_exprs = [
        (sum(col(f"diff_{c}") for c in flattened_source_df.columns
             if c not in [fk.replace('.', '_') for fk in join_keys])).alias("total_mismatches"),
        (count(lit(1)) - sum(col(f"diff_{c}") for c in flattened_source_df.columns
                             if c not in [fk.replace('.', '_') for fk in join_keys])).alias("total_matches")
    ]
    counts_df = comparison_df.agg(*agg_exprs)

    # Collect mismatch details
    mismatch_columns = [f"detail_{c}" for c in flattened_source_df.columns
                        if c not in [fk.replace('.', '_') for fk in join_keys]]
    detailed_mismatches_df = comparison_df.select(*join_keys, *mismatch_columns) \
        .filter(
        (col("diff_customer_info_age") == 1) |
        (col("diff_other_columns") == 1)  # Add other diff columns as needed
    )

    # Show counts
    counts_df.show(truncate=False)

    # Show detailed mismatches
    detailed_mismatches_df.show(truncate=False)

    # For better reporting, you might want to join counts and detailed mismatches into a single report
    # Here, we return both DataFrames for flexibility
    return counts_df, detailed_mismatches_df

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
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, struct, when, lit, map_from_arrays, array

def reconcile_dataframes_top_level_struct(source_df: DataFrame, target_df: DataFrame, join_columns: list) -> DataFrame:
    def get_column_types(df: DataFrame):
        return {field.name: field.dataType for field in df.schema.fields}

    def compare_scalar_columns(source_df: DataFrame, target_df: DataFrame, columns: list):
        comparison_exprs = []
        for col_name in columns:
            source_col = col(f"source.{col_name}")
            target_col = col(f"target.{col_name}")
            comparison_exprs.append(
                when(source_col != target_col,
                     struct(
                         source_col.alias("source_value"),
                         target_col.alias("target_value")
                     )
                     ).otherwise(lit(None)).alias(col_name)
            )
        return comparison_exprs

    def compare_top_level_struct_columns(source_df: DataFrame, target_df: DataFrame, struct_columns: list):
        comparison_exprs = []
        for struct_col in struct_columns:
            source_struct_col = col(f"source.{struct_col}")
            target_struct_col = col(f"target.{struct_col}")

            # Compare entire struct as a whole
            diff_expr = when(
                source_struct_col != target_struct_col,
                struct(
                    source_struct_col.alias("source_value"),
                    target_struct_col.alias("target_value")
                )
            ).otherwise(lit(None))

            comparison_exprs.append(diff_expr.alias(struct_col))

        return comparison_exprs

    # Identify scalar and top-level struct columns
    source_column_types = get_column_types(source_df)
    scalar_columns = [col for col, dtype in source_column_types.items() if not isinstance(dtype, StructType)]
    struct_columns = [col for col, dtype in source_column_types.items() if isinstance(dtype, StructType)]

    # Prepare DataFrames for comparison
    source_df = source_df.alias("source")
    target_df = target_df.alias("target")

    # Join DataFrames
    joined_df = source_df.join(target_df, join_columns, "full_outer")

    # Compare scalar columns
    scalar_comparison = compare_scalar_columns(source_df, target_df, scalar_columns)

    # Compare top-level struct columns
    struct_comparison = compare_top_level_struct_columns(source_df, target_df, struct_columns)

    # Combine results
    all_comparisons = scalar_comparison + struct_comparison

    # Create the result DataFrame
    result_df = joined_df.select(*join_columns, *all_comparisons)

    # Filter out rows with no differences
    diff_columns = [col for col in result_df.columns if col not in join_columns]
    filter_condition = " or ".join([f"{col} is not null" for col in diff_columns])
    result_df = result_df.filter(filter_condition)

    return result_df

# Example usage
spark = SparkSession.builder.appName("DataFrameReconciliation").getOrCreate()

# Define schema (same as before)
schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("is_manager", BooleanType(), True),
    StructField("personal_info", StructType([
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip", StringType(), True)
        ]), True)
    ]), True),
    StructField("work_info", StructType([
        StructField("department", StringType(), True),
        StructField("position", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("projects", StringType(), True)
    ]), True)
])

# Create source and target DataFrames (same as before)
source_data = [
    (1, "John", "Doe", 30, 75000.0, False,
     {"email": "john.doe@example.com", "phone": "123-456-7890",
      "address": {"street": "123 Main St", "city": "New York", "state": "NY", "zip": "10001"}},
     {"department": "IT", "position": "Developer", "start_date": "2020-01-15", "projects": "Project A, Project B"}),
    (2, "Jane", "Smith", 35, 90000.0, True,
     {"email": "jane.smith@example.com", "phone": "987-654-3210",
      "address": {"street": "456 Elm St", "city": "San Francisco", "state": "CA", "zip": "94105"}},
     {"department": "HR", "position": "Manager", "start_date": "2018-03-01", "projects": "Project C"}),
    (3, "Bob", "Johnson", 28, 65000.0, False,
     {"email": "bob.johnson@example.com", "phone": "555-123-4567",
      "address": {"street": "789 Oak St", "city": "Chicago", "state": "IL", "zip": "60601"}},
     {"department": "Marketing", "position": "Specialist", "start_date": "2021-07-01", "projects": "Project D, Project E"})
]

source_df = spark.createDataFrame(source_data, schema)

target_data = [
    (1, "John", "Doe", 31, 78000.0, False,
     {"email": "john.doe@example.com", "phone": "123-456-7890",
      "address": {"street": "123 Main St", "city": "New York", "state": "NY", "zip": "10001"}},
     {"department": "IT", "position": "Senior Developer", "start_date": "2020-01-15", "projects": "Project A, Project B, Project F"}),
    (2, "Jane", "Smith", 35, 95000.0, True,
     {"email": "jane.smith@newmail.com", "phone": "987-654-3210",
      "address": {"street": "789 Pine St", "city": "Los Angeles", "state": "CA", "zip": "90001"}},
     {"department": "HR", "position": "Senior Manager", "start_date": "2018-03-01", "projects": "Project C, Project G"}),
    (3, "Bob", "Johnson", 28, 65000.0, True,
     {"email": "bob.johnson@example.com", "phone": "555-999-8888",
      "address": {"street": "789 Oak St", "city": "Chicago", "state": "IL", "zip": "60601"}},
     {"department": "Marketing", "position": "Team Lead", "start_date": "2021-07-01", "projects": "Project D, Project E"})
]

target_df = spark.createDataFrame(target_data, schema)

# Perform reconciliation
join_columns = ["employee_id"]
reconciliation_result = reconcile_dataframes_top_level_struct(source_df, target_df, join_columns)

# Show results
reconciliation_result.show(truncate=False)

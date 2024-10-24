from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, struct, when, lit, explode_outer, collect_list
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType
)
from functools import reduce

def reconcile_dataframes(source_df: DataFrame, target_df: DataFrame, join_columns: list, array_child_primary_keys: dict) -> DataFrame:
    def get_column_types(df: DataFrame):
        return {field.name: field.dataType for field in df.schema.fields}

    def get_struct_and_array_columns(df: DataFrame):
        struct_columns = {}
        array_columns = {}
        for field in df.schema.fields:
            if isinstance(field.dataType, StructType):
                child_fields = field.dataType.fields
                child_column_names = [f"{field.name}.{child_field.name}" for child_field in child_fields]
                struct_columns[field.name] = child_column_names
            elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                child_fields = field.dataType.elementType.fields
                array_columns[field.name] = [child_field.name for child_field in child_fields]
        return struct_columns, array_columns

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

    def compare_struct_columns(source_df: DataFrame, target_df: DataFrame, struct_columns: dict):
        comparison_exprs = []
        for struct_col, nested_columns in struct_columns.items():
            diff_exprs = []
            any_diff_exprs = []
            for nested_col in nested_columns:
                field_name = nested_col.split('.')[-1]
                source_nested_col = col(f"source.{nested_col}")
                target_nested_col = col(f"target.{nested_col}")
                diff_expr = when(
                    source_nested_col != target_nested_col,
                    struct(
                        source_nested_col.alias("source_value"),
                        target_nested_col.alias("target_value")
                    )
                ).otherwise(lit(None)).alias(field_name)
                diff_exprs.append(diff_expr)
                any_diff_exprs.append(source_nested_col != target_nested_col)
            # Create a struct of the differences
            diff_struct = struct(*diff_exprs).alias(struct_col)
            # Include the struct only if there are differences
            any_diff = reduce(lambda x, y: x | y, any_diff_exprs)
            diff_struct_non_null = when(
                any_diff,
                diff_struct
            ).otherwise(lit(None)).alias(struct_col)
            comparison_exprs.append(diff_struct_non_null)
        return comparison_exprs

    def compare_array_columns(source_df: DataFrame, target_df: DataFrame, array_columns: dict, array_child_primary_keys: dict):
        comparison_exprs = []
        for array_col, child_field_names in array_columns.items():
            # Get child primary keys for this array column
            child_keys = array_child_primary_keys.get(array_col, [])
            # Explode arrays in source and target
            source_exploded = source_df.select(*join_columns, col(f"source.{array_col}")) \
                .withColumn("exploded", explode_outer(f"{array_col}")) \
                .select(*join_columns, *[col(f"exploded.{field}").alias(field) for field in child_field_names])
            target_exploded = target_df.select(*join_columns, col(f"target.{array_col}")) \
                .withColumn("exploded", explode_outer(f"{array_col}")) \
                .select(*join_columns, *[col(f"exploded.{field}").alias(field) for field in child_field_names])
            # Perform full outer join on join_columns + child_keys
            join_exprs = [source_exploded[col] == target_exploded[col] for col in join_columns + child_keys]
            exploded_joined = source_exploded.alias("source_arr").join(
                target_exploded.alias("target_arr"),
                on=join_exprs,
                how='full_outer'
            )
            # Compare each attribute in the structs
            diff_exprs = []
            any_diff_exprs = []
            for field_name in child_field_names:
                source_field = col(f"source_arr.{field_name}")
                target_field = col(f"target_arr.{field_name}")
                diff_expr = when(
                    source_field != target_field,
                    struct(
                        source_field.alias("source_value"),
                        target_field.alias("target_value")
                    )
                ).otherwise(lit(None)).alias(field_name)
                diff_exprs.append(diff_expr)
                any_diff_exprs.append(source_field != target_field)
            # Create a struct of differences per array element
            diff_struct = struct(*diff_exprs).alias("diff_struct")
            # Include only if there are differences
            any_diff = reduce(lambda x, y: x | y, any_diff_exprs)
            diff_row = exploded_joined.withColumn("diff_struct", when(any_diff, diff_struct).otherwise(lit(None))) \
                .where(any_diff)
            # Collect the differences back into an array
            differences = diff_row.groupBy(*join_columns).agg(collect_list("diff_struct").alias(array_col))
            # Left join the differences back to the main DataFrame
            source_df = source_df.join(differences, on=join_columns, how='left')
            comparison_exprs.append(col(array_col))
        return comparison_exprs, source_df

    # Identify scalar, struct, and array columns
    source_column_types = get_column_types(source_df)
    scalar_columns = [col_name for col_name, dtype in source_column_types.items()
                      if not isinstance(dtype, (StructType, ArrayType))]
    struct_columns, array_columns = get_struct_and_array_columns(source_df)

    # Prepare DataFrames for comparison
    source_df = source_df.alias("source")
    target_df = target_df.alias("target")

    # Compare scalar columns
    scalar_comparison = compare_scalar_columns(source_df, target_df, scalar_columns)

    # Compare struct columns (up to one level deep)
    struct_comparison = compare_struct_columns(source_df, target_df, struct_columns)

    # Compare array of struct columns with child primary keys
    array_comparison, result_df = compare_array_columns(source_df, target_df, array_columns, array_child_primary_keys)

    # Combine results
    all_comparisons = scalar_comparison + struct_comparison + array_comparison

    # Create the result DataFrame
    result_df = result_df.select(*[col(f"source.{col_name}").alias(col_name) for col_name in join_columns], *all_comparisons)

    # Filter out rows with differences
    diff_columns = [col_name for col_name in result_df.columns if col_name not in join_columns]
    filter_condition = reduce(lambda x, y: x | y, [col(col_name).isNotNull() for col_name in diff_columns])
    result_df = result_df.filter(filter_condition)

    return result_df

# Example usage
spark = SparkSession.builder.appName("DataFrameReconciliation").getOrCreate()

# Define schema with an array of structs, where elements have their own primary key
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
    StructField("projects", ArrayType(StructType([
        StructField("project_id", IntegerType(), True),  # Child primary key
        StructField("project_name", StringType(), True),
        StructField("role", StringType(), True)
    ])), True)
])

# Create source and target DataFrames
source_data = [
    (1, "John", "Doe", 30, 75000.0, False,
     {"email": "john.doe@example.com", "phone": "123-456-7890",
      "address": {"street": "123 Main St", "city": "New York", "state": "NY", "zip": "10001"}},
     [{"project_id": 1, "project_name": "Project A", "role": "Developer"},
      {"project_id": 2, "project_name": "Project B", "role": "Developer"}]),
    (2, "Jane", "Smith", 35, 90000.0, True,
     {"email": "jane.smith@example.com", "phone": "987-654-3210",
      "address": {"street": "456 Elm St", "city": "San Francisco", "state": "CA", "zip": "94105"}},
     [{"project_id": 3, "project_name": "Project C", "role": "Manager"}]),
    (3, "Bob", "Johnson", 28, 65000.0, False,
     {"email": "bob.johnson@example.com", "phone": "555-123-4567",
      "address": {"street": "789 Oak St", "city": "Chicago", "state": "IL", "zip": "60601"}},
     [{"project_id": 4, "project_name": "Project D", "role": "Specialist"},
      {"project_id": 5, "project_name": "Project E", "role": "Specialist"}])
]

source_df = spark.createDataFrame(source_data, schema)

target_data = [
    (1, "John", "Doe", 31, 78000.0, False,
     {"email": "john.doe@example.com", "phone": "123-456-7890",
      "address": {"street": "123 Main St", "city": "New York", "state": "NY", "zip": "10001"}},
     [{"project_id": 1, "project_name": "Project A", "role": "Lead Developer"},
      {"project_id": 6, "project_name": "Project F", "role": "Developer"}]),
    (2, "Jane", "Smith", 35, 95000.0, True,
     {"email": "jane.smith@newmail.com", "phone": "987-654-3210",
      "address": {"street": "789 Pine St", "city": "Los Angeles", "state": "CA", "zip": "90001"}},
     [{"project_id": 3, "project_name": "Project C", "role": "Manager"},
      {"project_id": 7, "project_name": "Project G", "role": "Manager"}]),
    (3, "Bob", "Johnson", 28, 65000.0, True,
     {"email": "bob.johnson@example.com", "phone": "555-999-8888",
      "address": {"street": "789 Oak St", "city": "Chicago", "state": "IL", "zip": "60601"}},
     [{"project_id": 4, "project_name": "Project D", "role": "Team Lead"},
      {"project_id": 5, "project_name": "Project E", "role": "Specialist"}])
]

target_df = spark.createDataFrame(target_data, schema)

# Specify child primary keys for array columns
array_child_primary_keys = {
    'projects': ['project_id']
}

# Perform reconciliation
join_columns = ["employee_id"]
reconciliation_result = reconcile_dataframes(source_df, target_df, join_columns, array_child_primary_keys)

# Show results
reconciliation_result.show(truncate=False)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, when, lit, map_from_arrays, array, explode_outer, isnull
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Initialize Spark Session
spark = SparkSession.builder.appName("ArrayOfStructWithParentIDReconciliation").getOrCreate()

def reconcile_dataframes(source_df, target_df, join_columns):
    def get_column_types(df):
        return {field.name: field.dataType for field in df.schema.fields}

    def flatten_struct_columns(schema, parent=""):
        flat_columns = []
        for field in schema.fields:
            col_name = f"{parent}.{field.name}" if parent else field.name
            if isinstance(field.dataType, StructType):
                flat_columns.extend(flatten_struct_columns(field.dataType, col_name))
            else:
                flat_columns.append(col_name)
        return flat_columns

    def compare_columns(source_col, target_col, col_name):
        """
        Generic column comparison that handles scalars, structs, nulls, and arrays of structs.
        """
        return when(
            isnull(source_col) & ~isnull(target_col),
            struct(lit(None).alias("source_value"), target_col.alias("target_value"))
        ).when(
            ~isnull(source_col) & isnull(target_col),
            struct(source_col.alias("source_value"), lit(None).alias("target_value"))
        ).when(
            source_col != target_col,
            struct(source_col.alias("source_value"), target_col.alias("target_value"))
        ).otherwise(lit(None)).alias(col_name)

    def compare_array_of_structs(source_df, target_df, array_col, nested_columns):
        """
        Compare array of structs with parent_id and id columns, showing differences only at the child level.
        """
        # Explode arrays of structs in both source and target
        exploded_source_df = source_df.withColumn("exploded_project", explode_outer(col(f"source.{array_col}")))
        exploded_target_df = target_df.withColumn("exploded_project", explode_outer(col(f"target.{array_col}")))

        # Join exploded arrays on parent_id and id using column expressions
        joined_projects_df = exploded_source_df.join(
            exploded_target_df,
            [
                col("exploded_project.parent_id") == col("exploded_project.parent_id"),
                col("exploded_project.id") == col("exploded_project.id")
            ],
            "full_outer"
        )

        # Compare nested columns within each project
        diff_keys = []
        diff_values = []
        for nested_col in nested_columns:
            source_nested_col = col(f"exploded_project.{nested_col}")
            target_nested_col = col(f"exploded_project.{nested_col}")

            diff_expr = compare_columns(source_nested_col, target_nested_col, nested_col.split('.')[-1])
            diff_keys.append(lit(nested_col.split('.')[-1]))
            diff_values.append(diff_expr)

        # Create a map of differences for the current project
        diff_map = when(
            map_from_arrays(array(*diff_keys), array(*diff_values)).isNotNull(),
            map_from_arrays(array(*diff_keys), array(*diff_values))
        ).otherwise(lit(None))

        # Only show differences for projects with differences
        result_df = joined_projects_df.filter(diff_map.isNotNull()).select(
            col("exploded_project.parent_id").alias("parent_id"),
            col("exploded_project.id").alias("id"),
            diff_map.alias(array_col)
        )

        return result_df

    # Identify column types
    source_column_types = get_column_types(source_df)
    array_struct_columns = {col: flatten_struct_columns(source_column_types[col].elementType, col)
                            for col, dtype in source_column_types.items() if isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType)}

    # Prepare DataFrames for comparison
    source_df = source_df.alias("source")
    target_df = target_df.alias("target")

    # Compare array of structs columns with parent_id and id
    comparison_dfs = []
    for array_col, nested_columns in array_struct_columns.items():
        comparison_df = compare_array_of_structs(source_df, target_df, array_col, nested_columns)
        comparison_dfs.append(comparison_df)

    # Combine results for all array of struct columns
    result_df = None
    if comparison_dfs:
        result_df = comparison_dfs[0]
        for df in comparison_dfs[1:]:
            result_df = result_df.union(df)

    return result_df

# Define schema with array of structs containing parent_id and id
schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("projects", ArrayType(
        StructType([
            StructField("parent_id", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("role", StringType(), True)
        ])
    ), True)
])

# Create source DataFrame
source_data = [
    (1, "John", "Doe",
     [{"parent_id": 1, "id": 101, "name": "Project A", "role": "Lead"},
      {"parent_id": 1, "id": 102, "name": "Project B", "role": "Member"}]
     ),
    (2, "Jane", "Smith",
     [{"parent_id": 2, "id": 201, "name": "Project C", "role": "Member"}]
     )
]

source_df = spark.createDataFrame(source_data, schema)

# Create target DataFrame with differences
target_data = [
    (1, "John", "Doe",
     [{"parent_id": 1, "id": 101, "name": "Project A", "role": "Lead"},
      {"parent_id": 1, "id": 102, "name": "Project B", "role": "Lead"}]  # Difference in role
     ),
    (2, "Jane", "Smith",
     [{"parent_id": 2, "id": 201, "name": "Project C", "role": "Lead"}]  # Difference in role
     )
]

target_df = spark.createDataFrame(target_data, schema)

# Define join columns
join_columns = ["employee_id"]

# Perform reconciliation
reconciliation_result = reconcile_dataframes(source_df, target_df, join_columns)

# Show the results
reconciliation_result.show(truncate=False)

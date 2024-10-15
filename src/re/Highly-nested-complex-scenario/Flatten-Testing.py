# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType, StructField, StringType, IntegerType, DoubleType
from typing import List, Optional

# COMMAND ----------

def generate_flatten_sql(schema: StructType, prefix: str = "", separator: str = "_") -> List[str]:
    """
    Recursively generate SQL expressions to flatten a nested schema.

    Args:
    -----
    schema : StructType
        The schema to flatten.
    prefix : str, optional
        Prefix for nested column names.
    separator : str, optional
        Separator for nested column names.

    Returns:
    --------
    List[str]
        List of SQL expressions for flattening.
    """
    expressions = []
    for field in schema.fields:
        column_name = f"{prefix}{field.name}"
        if isinstance(field.dataType, StructType):
            nested_exprs = generate_flatten_sql(field.dataType, f"{column_name}{separator}", separator)
            expressions.extend(nested_exprs)
        elif isinstance(field.dataType, ArrayType):
            if isinstance(field.dataType.elementType, StructType):
                nested_exprs = generate_flatten_sql(field.dataType.elementType, f"{column_name}{separator}", separator)
                array_exprs = [f"explode_outer({column_name}) as {column_name}"]
                array_exprs.extend([f"{expr} as {column_name}{separator}{alias.split(' as ')[1]}" for expr, alias in zip(nested_exprs, nested_exprs)])
                expressions.extend(array_exprs)
            else:
                # For scalar arrays, we keep them as-is
                expressions.append(f"{column_name} as {column_name.replace('.', separator)}")
        else:
            expressions.append(f"{column_name} as {column_name.replace('.', separator)}")
    return expressions

# COMMAND ----------

def flatten_delta_table_sql(
        df: DataFrame,
        columns_to_flatten: Optional[List[str]] = None,
        separator: str = "_"
) -> DataFrame:
    """
    Flatten a Delta table using SQL expressions.

    Args:
    -----
    df : DataFrame
        The input Delta table as a Spark DataFrame.
    columns_to_flatten : List[str], optional
        List of column names to flatten. If None, all nested columns will be flattened.
    separator : str, optional
        Separator for nested column names.

    Returns:
    --------
    DataFrame
        A new DataFrame with specified (or all) nested structures flattened.
    """
    schema = df.schema
    flat_expressions = generate_flatten_sql(schema, separator=separator)

    if columns_to_flatten:
        flat_expressions = [expr for expr in flat_expressions if any(expr.startswith(col) for col in columns_to_flatten)]

    # Create a SQL expression that selects all flattened columns
    select_expr = ", ".join(flat_expressions)

    # Create a temporary view of the original DataFrame
    view_name = "temp_view_for_flattening"
    df.createOrReplaceTempView(view_name)

    try:
        # Execute the SQL query using Spark SQL
        flattened_df = spark.sql(f"SELECT {select_expr} FROM {view_name}")
        return flattened_df
    finally:
        # Ensure the temporary view is always dropped
        spark.catalog.dropTempView(view_name)

# COMMAND ----------

# Create a sample dataset with multiple nested columns
sample_data = [
    (1, "John", {"age": 30, "city": "New York"},
     [{"type": "home", "number": "123-456-7890"}, {"type": "work", "number": "098-765-4321"}],
     {"scores": [85, 90, 78], "average": 84.3}),
    (2, "Alice", {"age": 25, "city": "San Francisco"},
     [{"type": "home", "number": "111-222-3333"}],
     {"scores": [92, 88, 95], "average": 91.7}),
    (3, "Bob", {"age": 35, "city": "Chicago"},
     [],
     {"scores": [75, 80, 82], "average": 79.0})
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("info", StructType([
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ]), True),
    StructField("phones", ArrayType(StructType([
        StructField("type", StringType(), True),
        StructField("number", StringType(), True)
    ])), True),
    StructField("grades", StructType([
        StructField("scores", ArrayType(IntegerType()), True),
        StructField("average", DoubleType(), True)
    ]), True)
])

# Create the DataFrame
df = spark.createDataFrame(sample_data, schema)

# COMMAND ----------

print("Original DataFrame:")
display(df)

# COMMAND ----------

# Flatten all nested columns
df_flat_all = flatten_delta_table_sql(df)
print("Flattened DataFrame (all nested columns):")
display(df_flat_all)

# COMMAND ----------

# Flatten specific columns
df_flat_specific = flatten_delta_table_sql(df, columns_to_flatten=["info", "phones"])
print("Flattened DataFrame (specific columns: info, phones):")
display(df_flat_specific)

# COMMAND ----------

# Show schema of flattened DataFrames
print("Schema of fully flattened DataFrame:")
df_flat_all.printSchema()

print("\nSchema of specifically flattened DataFrame:")
df_flat_specific.printSchema()
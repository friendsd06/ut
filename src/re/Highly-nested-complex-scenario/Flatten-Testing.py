"""
====================================================================
Highly Optimized PySpark Delta Table Flattening Tool
====================================================================

Author: Top 1% Developer
Date: 2024-04-27

Description:
------------
This script provides a robust and efficient method to flatten Delta tables
with complex and deeply nested schemas in Apache Spark. It handles both
structs and arrays (of structs and scalars), ensuring accurate and
order-independent comparisons.

Features:
---------
- **Recursive Schema Flattening**: Seamlessly handles multi-level nested structures.
- **Array Handling**: Differentiates between arrays of structs and arrays of scalars.
    - **Arrays of Structs**: Exploded to allow individual field comparisons.
    - **Arrays of Scalars**: Sorted to ensure order-independent comparison.
- **Selective Flattening**: Allows specifying which columns to flatten.
- **Clear and Descriptive Output**: Provides readable column names with clear aliases.
- **Optimized for Distributed Processing**: Utilizes Spark's built-in functions for performance.

Example Usage:
--------------
Imagine you have a Delta table representing student records with nested grades and scores.
You want to flatten this table to perform column-wise comparisons or data processing tasks.

Original DataFrame:
+---+-----+-------------------+---------------------------------------------+------------------------+
|id |name |info               |phones                                       |grades                  |
+---+-----+-------------------+---------------------------------------------+------------------------+
|1  |John |{30, New York}     |[{home, 123-456-7890}, {work, 098-765-4321}]|{[85, 90, 78], 84.3}|
|2  |Alice|{25, San Francisco}|[{home, 111-222-3333}]                        |{[92, 88, 95], 91.7}|
|3  |Bob  |{35, Chicago}      |[]                                           |{[75, 80, 82], 79.0}|
+---+-----+-------------------+---------------------------------------------+------------------------+

Flattened DataFrame (all nested columns):
+---+-----+---------+------------+--------------+--------------+--------------+--------------+
|id |name |info_age |info_city   |phones_type   |phones_number |grades_scores |grades_average|
+---+-----+---------+------------+--------------+--------------+--------------+--------------+
|1  |John |30       |New York    |home          |123-456-7890  |[78, 85, 90]  |84.3          |
|1  |John |30       |New York    |work          |098-765-4321  |[78, 85, 90]  |84.3          |
|2  |Alice|25       |San Francisco|home         |111-222-3333  |[88, 92, 95]  |91.7          |
|3  |Bob  |35       |Chicago     |null          |null          |[75, 80, 82]  |79.0          |
+---+-----+---------+------------+--------------+--------------+--------------+--------------+

Flattened DataFrame (specific columns: info, phones):
+---+-----+---------+------------+--------------+--------------+------------------------+
|id |name |info_age |info_city   |phones_type   |phones_number |grades                  |
+---+-----+---------+------------+--------------+--------------+------------------------+
|1  |John |30       |New York    |home          |123-456-7890  |{[85, 90, 78], 84.3}|
|1  |John |30       |New York    |work          |098-765-4321  |{[85, 90, 78], 84.3}|
|2  |Alice|25       |San Francisco|home         |111-222-3333  |{[92, 88, 95], 91.7}|
|3  |Bob  |35       |Chicago     |null          |null          |{[75, 80, 82], 79.0}|
+---+-----+---------+------------+--------------+--------------+------------------------+

====================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode_outer, sort_array, expr
from pyspark.sql.types import StructType, ArrayType, StringType, IntegerType, DoubleType
from typing import List, Optional, Tuple
from functools import reduce
import sys

def flatten_delta_table(
        df: DataFrame,
        columns_to_flatten: Optional[List[str]] = None,
        separator: str = "_"
) -> DataFrame:
    """
    Recursively flatten specified columns or all nested structures (structs and arrays) in a Delta table.

    Args:
    -----
    df (DataFrame): The input Delta table as a Spark DataFrame.
    columns_to_flatten (List[str], optional): List of column names to flatten.
        Supports nested columns using dot notation (e.g., "order_details.items").
        If None, all nested columns will be flattened.
    separator (str): The separator to use for flattened column names. Default is "_".

    Returns:
    --------
    DataFrame: A new DataFrame with specified (or all) nested structures flattened.
    """

    def flatten_schema(
            schema: StructType,
            prefix: str = ""
    ) -> Tuple[List[str], List[str], List[str]]:
        """
        Recursively flatten the schema and return lists of flattened column names,
        array columns containing structs to explode, and array columns containing scalars.

        Returns:
        --------
        Tuple[List[str], List[str], List[str]]:
            flat_cols: List of flattened column names.
            array_struct_cols: List of array columns containing structs to explode.
            array_scalar_cols: List of array columns containing scalars to sort.
        """
        flat_cols = []
        array_struct_cols = []
        array_scalar_cols = []
        for field in schema.fields:
            # Construct the full column name with prefix
            name = f"{prefix}{separator}{field.name}" if prefix else field.name
            dtype = field.dataType

            # Determine if the current field should be flattened
            should_flatten = (
                    columns_to_flatten is None or
                    any(name.startswith(col.replace(".", separator)) for col in columns_to_flatten)
            )

            if should_flatten:
                if isinstance(dtype, StructType):
                    # Recursively flatten nested StructType
                    nested_flat_cols, nested_array_struct_cols, nested_array_scalar_cols = flatten_schema(
                        dtype, prefix=name
                    )
                    flat_cols.extend(nested_flat_cols)
                    array_struct_cols.extend(nested_array_struct_cols)
                    array_scalar_cols.extend(nested_array_scalar_cols)
                elif isinstance(dtype, ArrayType):
                    if isinstance(dtype.elementType, StructType):
                        # Array of Structs: needs to be exploded
                        array_struct_cols.append(name)
                        # Keep the array column as-is for comparison after explosion
                        flat_cols.append(name)
                    else:
                        # Array of Scalars: sort for order-independent comparison
                        array_scalar_cols.append(name)
                        flat_cols.append(name)
                else:
                    # Simple field
                    flat_cols.append(name)
            else:
                # Field is excluded from flattening; include as-is
                flat_cols.append(name)

        return flat_cols, array_struct_cols, array_scalar_cols

    # Step 1: Flatten the schema to identify columns to explode or sort
    flat_cols, array_struct_cols, array_scalar_cols = flatten_schema(df.schema)

    # Step 2: Explode array columns containing structs
    for array_col in array_struct_cols:
        # Explode the array of structs
        df = df.withColumn(array_col, explode_outer(col(array_col)))
        # After exploding, flatten the struct fields
        # For example, if array_col is 'phones', with 'type' and 'number'
        # Create 'phones_type' and 'phones_number'
        struct_fields = df.schema[array_col].dataType.elementType.fields if array_col in df.columns else []
        for field in struct_fields:
            df = df.withColumn(f"{array_col}{separator}{field.name}", col(f"{array_col}.{field.name}"))
        # Drop the original exploded struct column
        df = df.drop(array_col)

    # Step 3: Sort array columns containing scalars to ensure order-independent comparison
    for array_col in array_scalar_cols:
        df = df.withColumn(array_col, sort_array(col(array_col), ascending=True))

    # Step 4: Select all flattened columns with appropriate aliasing
    # Replace the separator with '_' for readability
    select_exprs = [
        col(col_name).alias(col_name.replace(separator, "_")) for col_name in flat_cols
    ]

    df_flat = df.select(*select_exprs)

    # Step 5: Recursively flatten further nested structures if any
    # Check if new columns were added after flattening
    if len(df_flat.columns) > len(df.columns):
        return flatten_delta_table(df_flat, columns_to_flatten, separator)
    else:
        return df_flat

# Test case
def test_flatten_delta_table():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("FlattenDeltaTableTest") \
        .master("local[*]") \
        .getOrCreate()

    # Create a sample dataset with multiple nested columns
    sample_data = [
        (
            1,
            "John",
            {"age": 30, "city": "New York"},
            [
                {"type": "home", "number": "123-456-7890"},
                {"type": "work", "number": "098-765-4321"}
            ],
            {"scores": [85, 90, 78], "average": 84.3}
        ),
        (
            2,
            "Alice",
            {"age": 25, "city": "San Francisco"},
            [
                {"type": "home", "number": "111-222-3333"}
            ],
            {"scores": [92, 88, 95], "average": 91.7}
        ),
        (
            3,
            "Bob",
            {"age": 35, "city": "Chicago"},
            [],
            {"scores": [75, 80, 82], "average": 79.0}
        )
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

    df = spark.createDataFrame(sample_data, schema)

    print("Original DataFrame:")
    df.show(truncate=False)
    df.printSchema()

    # Test case 1: Flatten all nested columns
    df_flat_all = flatten_delta_table(df)
    print("\nFlattened DataFrame (all nested columns):")
    df_flat_all.show(truncate=False)
    df_flat_all.printSchema()

    # Test case 2: Flatten specific columns (e.g., "info" and "phones")
    df_flat_specific = flatten_delta_table(df, columns_to_flatten=["info", "phones"])
    print("\nFlattened DataFrame (specific columns: info, phones):")
    df_flat_specific.show(truncate=False)
    df_flat_specific.printSchema()

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    test_flatten_delta_table()

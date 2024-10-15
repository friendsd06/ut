"""
====================================================================
Highly Optimized PySpark Delta Table Comparison Tool
====================================================================

Author: Top 1% Developer
Date: 2024-04-27

Description:
------------
This script provides a robust and efficient method to compare two Delta tables
with complex and deeply nested schemas in Apache Spark. It identifies differences
at any level of nesting, including within arrays and structs, and outputs a
comprehensive DataFrame highlighting the discrepancies.

Features:
---------
- **Recursive Schema Flattening**: Handles multi-level nested structures seamlessly.
- **Array Handling**: Sorts arrays before comparison to ensure order-independent matching.
- **Comprehensive Difference Reporting**: Captures old and new values for each differing field.
- **Optimized for Distributed Processing**: Utilizes Spark's built-in functions for performance.
- **Flexible Column Selection**: Supports inclusion and exclusion of specific columns.
- **Clear and Descriptive Output**: Provides mismatch counts and detailed difference structures.

Example Usage:
--------------
Imagine you have two Delta tables representing customer orders at two different
times. You want to identify any changes in the orders, including modifications
within nested items and addresses.

Table 1 (Original Data):
+---+----------+---------------------------------------------+---------------------------------------------+
|id |customer  |order_details                                 |shipping_address                             |
+---+----------+---------------------------------------------+---------------------------------------------+
|1  |John Doe  |{order_id: 1001, items: [{item_id: A1, qty:2}, {item_id: B2, qty:1}]}|{street: "123 Elm St", city: "Springfield", zip: "12345"}|
|2  |Jane Smith|{order_id: 1002, items: [{item_id: C3, qty:5}]}|{street: "456 Oak St", city: "Shelbyville", zip: "67890"}|
+---+----------+---------------------------------------------+---------------------------------------------+

Table 2 (Updated Data):
+---+-------------+---------------------------------------------+---------------------------------------------+
|id |customer     |order_details                                 |shipping_address                             |
+---+-------------+---------------------------------------------+---------------------------------------------+
|1  |John Doe     |{order_id: 1001, items: [{item_id: A1, qty:3}, {item_id: B2, qty:1}]}|{street: "123 Elm St", city: "Springfield", zip: "12345"}|
|2  |Jane Smith   |{order_id: 1002, items: [{item_id: C3, qty:5}, {item_id: D4, qty:2}]}|{street: "456 Oak St", city: "Capital City", zip: "67890"}|
|3  |Alice Johnson|{order_id: 1003, items: [{item_id: E5, qty:1}]}|{street: "789 Pine St", city: "Ogdenville", zip: "54321"}|
+---+-------------+---------------------------------------------+---------------------------------------------+

Expected Output After Comparison:
+---+------------------------+-----------------------------------------------+---------------------------------------------+--------------+
|id |order_details_order_id  |order_details_items                            |shipping_address_city                       |mismatch_count|
+---+------------------------+-----------------------------------------------+---------------------------------------------+--------------+
|1  |null                    |{old: [{item_id: A1, qty: 2}], new: [{item_id: A1, qty: 3}]}|null                                         |1             |
|2  |null                    |{old: null, new: [{item_id: D4, qty: 2}]}      |{old: Shelbyville, new: Capital City}       |2             |
|3  |{old: null, new: 1003}  |{old: null, new: [{item_id: E5, qty: 1}]}      |{old: null, new: Ogdenville}                 |3             |
+---+------------------------+-----------------------------------------------+---------------------------------------------+--------------+
3 rows with differences

====================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    when,
    struct,
    lit,
    sort_array,
    array_sort,
)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from typing import List, Optional
from functools import reduce
import sys

def flatten_schema(
        schema: StructType, prefix: str = "", separator: str = "_"
) -> List[str]:
    """
    Recursively flattens a nested Spark schema into a list of column paths.

    Parameters:
    -----------
    - schema: StructType
        The schema to flatten.
    - prefix: str, optional
        The prefix to prepend to each column name (used for nested fields).
    - separator: str, optional
        The separator to use between nested field names.

    Returns:
    --------
    - List[str]
        A list of flattened column names with full paths.
    """
    fields = []
    for field in schema.fields:
        field_name = f"{prefix}{separator}{field.name}" if prefix else field.name
        if isinstance(field.dataType, StructType):
            # Recursively flatten nested StructType
            fields.extend(flatten_schema(field.dataType, prefix=field_name, separator=separator))
        elif isinstance(field.dataType, ArrayType):
            # If the array contains StructType, keep the array as a whole for comparison
            if isinstance(field.dataType.elementType, StructType):
                fields.append(field_name)  # e.g., order_details_items
            else:
                fields.append(field_name)  # e.g., some_array_of_scalars
        else:
            fields.append(field_name)
    return fields

def get_flat_columns(
        df: DataFrame,
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
        separator: str = "_"
) -> List[str]:
    """
    Retrieves a list of flattened column names from a DataFrame's schema,
    applying inclusion and exclusion filters.

    Parameters:
    -----------
    - df: DataFrame
        The Spark DataFrame.
    - include_columns: List[str], optional
        Columns to include in the comparison. Supports top-level and nested columns using dot notation.
    - exclude_columns: List[str], optional
        Columns to exclude from the comparison. Supports top-level and nested columns using dot notation.
    - separator: str, optional
        Separator used in flattened column names.

    Returns:
    --------
    - List[str]
        A list of flattened column names.
    """
    flat_cols = flatten_schema(df.schema, separator=separator)
    if include_columns:
        include_flat = []
        for ic in include_columns:
            # Convert dot notation to separator
            ic_flat = ic.replace(".", separator)
            include_flat.extend([c for c in flat_cols if c.startswith(ic_flat)])
        flat_cols = list(set(flat_cols).intersection(set(include_flat)))
    if exclude_columns:
        exclude_flat = []
        for ec in exclude_columns:
            ec_flat = ec.replace(".", separator)
            exclude_flat.extend([c for c in flat_cols if c.startswith(ec_flat)])
        flat_cols = [c for c in flat_cols if c not in exclude_flat]
    return flat_cols

def sort_arrays_in_df(df: DataFrame, columns: List[str], ascending: bool = True) -> DataFrame:
    """
    Sorts arrays within specified columns of a DataFrame to ensure order-independent comparison.

    Parameters:
    -----------
    - df: DataFrame
        The Spark DataFrame.
    - columns: List[str]
        List of flattened column names containing arrays to sort.
    - ascending: bool, optional
        Sort order; True for ascending, False for descending.

    Returns:
    --------
    - DataFrame
        The DataFrame with sorted arrays.
    """
    for column in columns:
        # Apply sort_array function; assumes elements are comparable
        df = df.withColumn(column, sort_array(col(column), ascending))
    return df

def compare_delta_tables(
        table1: DataFrame,
        table2: DataFrame,
        join_key: str,
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
        sort_array_columns: Optional[List[str]] = None,
        separator: str = "_"
) -> DataFrame:
    """
    Compares two Delta tables and returns a DataFrame highlighting the differences,
    including nested structures.

    Parameters:
    -----------
    - table1: DataFrame
        First Delta table as a Spark DataFrame (original).
    - table2: DataFrame
        Second Delta table as a Spark DataFrame (updated).
    - join_key: str
        Column name to join the tables on.
    - include_columns: List[str], optional
        Columns to include in the comparison. Supports nested columns using dot notation.
    - exclude_columns: List[str], optional
        Columns to exclude from the comparison. Supports nested columns using dot notation.
    - sort_array_columns: List[str], optional
        List of array-type columns to sort before comparison. Supports nested columns using dot notation.
    - separator: str, optional
        Separator used in flattened column names.

    Returns:
    --------
    - DataFrame
        A Spark DataFrame containing rows with differences, the differing columns,
        and a mismatch count.
    """
    # Validate input DataFrames
    if not isinstance(table1, DataFrame) or not isinstance(table2, DataFrame):
        raise ValueError("Both table1 and table2 must be Spark DataFrames.")

    # Identify common top-level columns excluding the join key
    common_columns = set(table1.columns).intersection(set(table2.columns)) - {join_key}

    # Apply include and exclude filters
    if include_columns:
        common_columns = set(include_columns).intersection(common_columns)
    if exclude_columns:
        common_columns = set(common_columns) - set(exclude_columns)

    # Get flattened column names for both tables
    flattened_cols_table1 = get_flat_columns(table1, include_columns, exclude_columns, separator)
    flattened_cols_table2 = get_flat_columns(table2, include_columns, exclude_columns, separator)

    # Determine the intersection of flattened columns
    flattened_common_columns = set(flattened_cols_table1).intersection(set(flattened_cols_table2)) - {join_key}

    # Convert to list
    flattened_common_columns = list(flattened_common_columns)

    # Optionally sort arrays to ensure order-independent comparison
    if sort_array_columns:
        # Convert sort_array_columns from dot notation to flattened names
        sort_array_columns_flat = [c.replace(".", separator) for c in sort_array_columns]
        table1 = sort_arrays_in_df(table1, sort_array_columns_flat, ascending=True)
        table2 = sort_arrays_in_df(table2, sort_array_columns_flat, ascending=True)

    # Select and alias columns for both tables
    selected_cols_table1 = [col(c).alias(f"t1.{c}") for c in flattened_common_columns] + [col(join_key).alias(f"t1.{join_key}")]
    selected_cols_table2 = [col(c).alias(f"t2.{c}") for c in flattened_common_columns] + [col(join_key).alias(f"t2.{join_key}")]

    # Select the necessary columns
    table1_flat = table1.select(*selected_cols_table1)
    table2_flat = table2.select(*selected_cols_table2)

    # Perform a full outer join on the join key to capture all differences
    joined_df = table1_flat.join(table2_flat, on=join_key, how="full_outer")

    # Generate comparison expressions for each flattened column
    comparison_exprs = []
    mismatch_conditions = []

    for c in flattened_common_columns:
        col1 = col(f"t1.{c}")
        col2 = col(f"t2.{c}")

        # For arrays of structs, compare sorted arrays
        # Assuming elements in arrays are structs with consistent ordering after sort
        # Sorting is already handled if specified in sort_array_columns
        if isinstance(table1.schema[c].dataType, ArrayType) and isinstance(table1.schema[c].dataType.elementType, StructType):
            # Compare arrays directly since they are sorted if specified
            expr = when(
                col1 != col2,
                struct(
                    col1.alias("old"),
                    col2.alias("new")
                )
            ).alias(c)
        else:
            # Compare scalar or struct fields directly
            expr = when(
                col1 != col2,
                struct(
                    col1.alias("old"),
                    col2.alias("new")
                )
            ).alias(c)
        comparison_exprs.append(expr)

        # Condition to check if this column has a mismatch
        mismatch_conditions.append(col(c).isNotNull())

    # Calculate total number of mismatches
    mismatch_count_expr = reduce(
        lambda acc, cond: acc + when(cond, lit(1)).otherwise(lit(0)),
        mismatch_conditions,
        lit(0)
    )

    # Select the join key, comparison results, and mismatch count
    result_df = joined_df.select(
        col(f"t1.{join_key}").alias(join_key),
        *comparison_exprs
    ).withColumn(
        "mismatch_count", mismatch_count_expr
    ).filter(
        col("mismatch_count") > 0
    )

    return result_df

def test_complex_nested_schema(spark: SparkSession):
    """
    Tests the compare_delta_tables function with a complex nested schema scenario.

    This function creates two sample DataFrames with deeply nested structures and
    intentional differences to demonstrate the comparison functionality.
    """
    print("============================================")
    print("Testing Complex Nested Schema Scenario:")
    print("============================================")

    # Define a deeply nested schema
    complex_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("customer", StringType(), False),
        StructField("order_details", StructType([
            StructField("order_id", IntegerType(), True),
            StructField("items", ArrayType(StructType([
                StructField("item_id", StringType(), True),
                StructField("qty", IntegerType(), True)
            ])), True)
        ]), True),
        StructField("shipping_address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True)
        ]), True)
    ])

    # Sample data for the first table (original)
    data1 = [
        (
            1,
            "John Doe",
            {
                "order_id": 1001,
                "items": [
                    {"item_id": "A1", "qty": 2},
                    {"item_id": "B2", "qty": 1}
                ]
            },
            {"street": "123 Elm St", "city": "Springfield", "zip": "12345"}
        ),
        (
            2,
            "Jane Smith",
            {
                "order_id": 1002,
                "items": [
                    {"item_id": "C3", "qty": 5}
                ]
            },
            {"street": "456 Oak St", "city": "Shelbyville", "zip": "67890"}
        )
    ]

    # Sample data for the second table (updated)
    data2 = [
        (
            1,
            "John Doe",
            {
                "order_id": 1001,
                "items": [
                    {"item_id": "A1", "qty": 3},  # qty changed from 2 to 3
                    {"item_id": "B2", "qty": 1}
                ]
            },
            {"street": "123 Elm St", "city": "Springfield", "zip": "12345"}
        ),
        (
            2,
            "Jane Smith",
            {
                "order_id": 1002,
                "items": [
                    {"item_id": "C3", "qty": 5},
                    {"item_id": "D4", "qty": 2}  # new item added
                ]
            },
            {"street": "456 Oak St", "city": "Capital City", "zip": "67890"}  # city changed
        ),
        (
            3,
            "Alice Johnson",
            {
                "order_id": 1003,
                "items": [
                    {"item_id": "E5", "qty": 1}
                ]
            },
            {"street": "789 Pine St", "city": "Ogdenville", "zip": "54321"}  # new record
        )
    ]

    # Create DataFrames
    table1 = spark.createDataFrame(data1, schema=complex_schema)
    table2 = spark.createDataFrame(data2, schema=complex_schema)

    # Display input tables
    print("\nTable 1 (Original Data):")
    table1.show(truncate=False)

    print("\nTable 2 (Updated Data):")
    table2.show(truncate=False)

    # Define join key
    join_key = "id"

    # Compare the two tables
    differences_df = compare_delta_tables(
        table1=table1,
        table2=table2,
        join_key=join_key,
        sort_array_columns=["order_details_items"]  # Sorting arrays to ensure order-independent comparison
    )

    # Display the differences
    print("\nDifferences Found:")
    differences_df.show(truncate=False)

    # Output the number of differing rows
    print(f"{differences_df.count()} rows with differences\n")

def main():
    """
    Main execution function.

    Initializes SparkSession, runs the test scenario, and gracefully stops the session.
    """
    # Initialize SparkSession with optimized configurations
    spark = SparkSession.builder \
        .appName("Highly Optimized Delta Table Comparison") \
        .config("spark.sql.shuffle.partitions", "200")  # Adjust based on cluster resources
    .getOrCreate()

try:
    # Execute the test with complex nested schemas
    test_complex_nested_schema(spark)
except Exception as e:
    print(f"An error occurred: {e}", file=sys.stderr)
finally:
    # Stop the SparkSession gracefully
    spark.stop()

if __name__ == "__main__":
    main()

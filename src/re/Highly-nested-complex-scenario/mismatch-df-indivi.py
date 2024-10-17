from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType,
    MapType, DoubleType
)
from pyspark.sql.functions import (
    explode_outer, col, when, struct, lit, array, explode, filter, expr
)
from functools import reduce
from typing import List, Optional

def compare_delta_tables(
        table1: DataFrame,
        table2: DataFrame,
        join_key: str,
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Compare two Delta tables and return a DataFrame with the differences.
    Shows only the columns that have mismatches along with mismatch and match counts.
    Works for both nested and non-nested structures.

    Args:
        table1 (DataFrame): The first Delta table.
        table2 (DataFrame): The second Delta table.
        join_key (str): The primary key column to join on.
        include_columns (Optional[List[str]]): Specific columns to include in the comparison.
        exclude_columns (Optional[List[str]]): Specific columns to exclude from the comparison.

    Returns:
        DataFrame: A DataFrame highlighting the differences with mismatched columns,
                   their old and new values, and counts of mismatches and matches per row.
    """
    # Ensure the inputs are DataFrames
    if not isinstance(table1, DataFrame) or not isinstance(table2, DataFrame):
        raise ValueError("Both table1 and table2 must be Spark DataFrames.")

    # Determine columns for comparison
    all_columns = set(table1.columns) & set(table2.columns) - {join_key}

    # Include columns logic
    if include_columns:
        compare_columns = list(set(include_columns) & all_columns)
    else:
        compare_columns = list(all_columns)

    # Exclude columns logic
    if exclude_columns:
        compare_columns = [c for c in compare_columns if c not in exclude_columns]

    # Calculate total number of columns being compared
    total_compare_columns = len(compare_columns)

    if total_compare_columns == 0:
        raise ValueError("No columns available for comparison after applying include/exclude filters.")

    # Perform a full outer join
    joined_df = table1.alias("t1").join(table2.alias("t2"), on=join_key, how="full_outer")

    # Create comparison expressions
    # For each column, create a struct of (old, new) if values differ, else null
    compare_exprs = [
        when(col(f"t1.{c}") != col(f"t2.{c}"),
             struct(col(f"t1.{c}").alias("old"), col(f"t2.{c}").alias("new"))
             ).alias(c)
        for c in compare_columns
    ]

    # Select join_key and comparison expressions
    comparison_df = joined_df.select(
        col(f"t1.{join_key}").alias(join_key),
        *compare_exprs
    )

    # Create an array of mismatches with column name, old value, and new value
    mismatches_expr = filter(
        array(
            *[
                when(col(c).isNotNull(), struct(
                    lit(c).alias("column"),
                    col(f"{c}.old").alias("old_value"),
                    col(f"{c}.new").alias("new_value")
                ))
                for c in compare_columns
            ]
        ),
        lambda x: x.isNotNull()
    )

    # Add mismatches array column
    comparison_df = comparison_df.withColumn("mismatches", mismatches_expr)

    # Calculate mismatch_count and match_count
    comparison_df = comparison_df.withColumn(
        "mismatch_count",
        expr("size(mismatches)")
    ).withColumn(
        "match_count",
        lit(total_compare_columns) - col("mismatch_count")
    )

    # Explode mismatches to have one row per mismatch
    # Include mismatch_count and match_count in each row
    result_df = comparison_df.withColumn("mismatch", explode("mismatches")) \
        .select(
        join_key,
        col("mismatch.column").alias("column"),
        col("mismatch.old_value").alias("old_value"),
        col("mismatch.new_value").alias("new_value"),
        col("mismatch_count"),
        col("match_count")
    )

    return result_df

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("DeltaTableComparison") \
        .getOrCreate()

    # ===========================
    # Example 1: Simple Flat DataFrames
    # ===========================

    # Define schema for Table1
    schema_table1 = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("address", StringType(), True),
        StructField("salary", DoubleType(), True)
    ])

    # Create sample data for Table1
    data_table1 = [
        (1, "John Doe", 30, "123 Elm St", 70000.0),
        (2, "Jane Smith", 25, "456 Oak Ave", 80000.0),
        (3, "Alice Johnson", 28, "789 Pine Rd", 75000.0)
    ]

    # Create DataFrame for Table1
    table1 = spark.createDataFrame(data_table1, schema_table1)

    # Define schema for Table2
    schema_table2 = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("address", StringType(), True),
        StructField("salary", DoubleType(), True)
    ])

    # Create sample data for Table2
    data_table2 = [
        (1, "John Doe", 31, "123 Elm St", 70000.0),          # age mismatch
        (2, "Jane Smith", 25, "456 Oak Avenue", 82000.0),    # address and salary mismatch
        (4, "Bob Brown", 40, "321 Maple St", 90000.0)        # New record not in table1
    ]

    # Create DataFrame for Table2
    table2 = spark.createDataFrame(data_table2, schema_table2)

    # Define the join key
    join_key = "id"

    # Compare the tables
    differences_df = compare_delta_tables(
        table1=table1,
        table2=table2,
        join_key=join_key,
        include_columns=["name", "age", "address", "salary"],
        exclude_columns=["name"]  # Example: exclude 'name' from comparison
    )

    # Show the differences with mismatch and match counts
    print("=== Example 1: Simple Flat DataFrames ===")
    differences_df.show(truncate=False)

    # ===========================
    # Example 2: Nested DataFrames
    # ===========================

    # Define schema for Table1 with nested structure
    schema_table1_nested = StructType([
        StructField("id", IntegerType(), True),
        StructField("personal_info", StructType([
            StructField("name", StructType([
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True)
            ]), True),
            StructField("contact", StructType([
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True)
            ]), True)
        ]), True)
    ])

    # Create sample data for Table1 Nested
    data_table1_nested = [
        (1, {"name": {"first_name": "John", "last_name": "Doe"},
             "contact": {"email": "john.doe@example.com", "phone": "555-1234"}}),
        (2, {"name": {"first_name": "Jane", "last_name": "Smith"},
             "contact": {"email": "jane.smith@example.com", "phone": "555-5678"}})
    ]

    # Create DataFrame for Table1 Nested
    table1_nested = spark.createDataFrame(data_table1_nested, schema_table1_nested)

    # Define schema for Table2 with nested structure
    schema_table2_nested = StructType([
        StructField("id", IntegerType(), True),
        StructField("personal_info", StructType([
            StructField("name", StructType([
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True)
            ]), True),
            StructField("contact", StructType([
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True)
            ]), True)
        ]), True)
    ])

    # Create sample data for Table2 Nested
    data_table2_nested = [
        (1, {"name": {"first_name": "John", "last_name": "Doe"},
             "contact": {"email": "john.doe@example.com", "phone": "555-9999"}}),  # phone mismatch
        (3, {"name": {"first_name": "Alice", "last_name": "Johnson"},
             "contact": {"email": "alice.johnson@example.com", "phone": "555-0000"}})   # New record
    ]

    # Create DataFrame for Table2 Nested
    table2_nested = spark.createDataFrame(data_table2_nested, schema_table2_nested)

    # Compare nested tables
    differences_nested_df = compare_delta_tables(
        table1=table1_nested,
        table2=table2_nested,
        join_key="id",
        include_columns=["personal_info"],
        exclude_columns=[]
    )

    # Show mismatches with counts
    print("\n=== Example 2: Nested DataFrames ===")
    differences_nested_df.show(truncate=False)

    # ===========================
    # Example 3: Complex Nested Structures
    # ===========================

    # Define schema for Complex Nested Example
    schema_complex_nested = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_info", StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("personal_details", StructType([
                StructField("name", StructType([
                    StructField("first_name", StringType(), True),
                    StructField("last_name", StringType(), True)
                ]), True),
                StructField("contact", MapType(StringType(), StringType()), True)
            ]), True)
        ]), True),
        StructField("order_details", StructType([
            StructField("items", ArrayType(StructType([
                StructField("item_id", IntegerType(), True),
                StructField("description", StringType(), True),
                StructField("specs", StructType([
                    StructField("color", StringType(), True),
                    StructField("dimensions", StructType([
                        StructField("length", DoubleType(), True),
                        StructField("width", DoubleType(), True)
                    ]), True)
                ]), True)
            ])), True),
            StructField("payment_info", StructType([
                StructField("method", StringType(), True),
                StructField("transaction_info", StructType([
                    StructField("transaction_id", StringType(), True),
                    StructField("status", StringType(), True)
                ]), True)
            ]), True)
        ]), True)
    ])

    # Create sample data for Complex Nested Example
    data_complex_nested = [
        (
            5001,
            {
                "customer_id": 601,
                "personal_details": {
                    "name": {
                        "first_name": "David",
                        "last_name": "Wilson"
                    },
                    "contact": {
                        "email": "david.wilson@example.com",
                        "phone": "555-0707"
                    }
                }
            },
            {
                "items": [
                    {
                        "item_id": 701,
                        "description": "Gaming Laptop",
                        "specs": {
                            "color": "Black",
                            "dimensions": {
                                "length": 35.5,
                                "width": 25.0
                            }
                        }
                    },
                    {
                        "item_id": 702,
                        "description": "Wireless Mouse",
                        "specs": {
                            "color": "Red",
                            "dimensions": {
                                "length": 10.0,
                                "width": 5.0
                            }
                        }
                    }
                ],
                "payment_info": {
                    "method": "Credit Card",
                    "transaction_info": {
                        "transaction_id": "CC789456123",
                        "status": "Completed"
                    }
                }
            }
        ),
        (
            5002,
            {
                "customer_id": 602,
                "personal_details": {
                    "name": {
                        "first_name": "Emma",
                        "last_name": "Davis"
                    },
                    "contact": {
                        "email": "emma.davis@example.com",
                        "phone": "555-0808"
                    }
                }
            },
            {
                "items": [
                    {
                        "item_id": 703,
                        "description": "Smartphone",
                        "specs": {
                            "color": "Blue",
                            "dimensions": {
                                "length": 15.0,
                                "width": 7.0
                            }
                        }
                    }
                ],
                "payment_info": {
                    "method": "PayPal",
                    "transaction_info": {
                        "transaction_id": "PP123789456",
                        "status": "Pending"
                    }
                }
            }
        )
    ]

    # Create DataFrame for Complex Nested Example
    complex_nested_df1 = spark.createDataFrame(data_complex_nested, schema_complex_nested)

    # Create another sample data with some mismatches
    data_complex_nested2 = [
        (
            5001,
            {
                "customer_id": 601,
                "personal_details": {
                    "name": {
                        "first_name": "David",
                        "last_name": "Wilson"
                    },
                    "contact": {
                        "email": "david.wilson@example.com",
                        "phone": "555-0711"  # phone mismatch
                    }
                }
            },
            {
                "items": [
                    {
                        "item_id": 701,
                        "description": "Gaming Laptop Pro",  # description mismatch
                        "specs": {
                            "color": "Black",
                            "dimensions": {
                                "length": 35.5,
                                "width": 25.0
                            }
                        }
                    },
                    {
                        "item_id": 702,
                        "description": "Wireless Mouse",
                        "specs": {
                            "color": "Blue",  # color mismatch
                            "dimensions": {
                                "length": 10.0,
                                "width": 5.0
                            }
                        }
                    }
                ],
                "payment_info": {
                    "method": "Credit Card",
                    "transaction_info": {
                        "transaction_id": "CC789456123",
                        "status": "Completed"
                    }
                }
            }
        ),
        (
            5003,
            {
                "customer_id": 603,
                "personal_details": {
                    "name": {
                        "first_name": "Frank",
                        "last_name": "Miller"
                    },
                    "contact": {
                        "email": "frank.miller@example.com",
                        "phone": "555-0909"
                    }
                }
            },
            {
                "items": [
                    {
                        "item_id": 704,
                        "description": "Tablet",
                        "specs": {
                            "color": "White",
                            "dimensions": {
                                "length": 20.0,
                                "width": 12.0
                            }
                        }
                    }
                ],
                "payment_info": {
                    "method": "Debit Card",
                    "transaction_info": {
                        "transaction_id": "DC456123789",
                        "status": "Completed"
                    }
                }
            }
        )
    ]

    # Create DataFrame for Complex Nested Example (Second Table)
    complex_nested_df2 = spark.createDataFrame(data_complex_nested2, schema_complex_nested)

    # Compare complex nested tables
    differences_complex_nested_df = compare_delta_tables(
        table1=complex_nested_df1,
        table2=complex_nested_df2,
        join_key="order_id",
        include_columns=["customer_info", "order_details"],
        exclude_columns=[]
    )

    # Show mismatches with counts
    print("\n=== Example 3: Complex Nested DataFrames ===")
    differences_complex_nested_df.show(truncate=False)

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main()

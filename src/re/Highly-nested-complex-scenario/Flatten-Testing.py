from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode_outer, expr
from pyspark.sql.types import StructType, ArrayType, StringType, IntegerType, DoubleType
from typing import List, Optional, Tuple

def flatten_delta_table(df: DataFrame, columns_to_flatten: Optional[List[str]] = None, separator: str = "_") -> DataFrame:
    """
    Recursively flatten specified columns or all nested structures (structs and arrays) in a Delta table.

    Args:
    df (DataFrame): The input Delta table as a Spark DataFrame.
    columns_to_flatten (List[str], optional): List of column names to flatten. If None, all nested columns will be flattened.
    separator (str): The separator to use for flattened column names. Default is "_".

    Returns:
    DataFrame: A new DataFrame with specified (or all) nested structures flattened.
    """

    def flatten_schema(schema: StructType, prefix: str = "") -> Tuple[List[str], List[str]]:
        """
        Recursively flatten the schema and return lists of flattened column expressions and array columns to explode.
        """
        fields = []
        array_columns = []
        for field in schema.fields:
            name = prefix + field.name if prefix else field.name
            dtype = field.dataType

            if columns_to_flatten is None or name in columns_to_flatten:
                if isinstance(dtype, StructType):
                    nested_fields, nested_arrays = flatten_schema(dtype, f"{name}{separator}")
                    fields.extend(nested_fields)
                    array_columns.extend(nested_arrays)
                elif isinstance(dtype, ArrayType):
                    array_columns.append(name)
                    if isinstance(dtype.elementType, StructType):
                        nested_fields, _ = flatten_schema(dtype.elementType, f"{name}{separator}")
                        fields.extend(nested_fields)
                    else:
                        fields.append(f"{name} as {name}")
                else:
                    fields.append(name)
            else:
                fields.append(name)

        return fields, array_columns

    flat_cols, array_cols = flatten_schema(df.schema)

    # First, explode all array columns
    for array_col in array_cols:
        df = df.withColumn(array_col, explode_outer(col(array_col)))

    # Then, select all flattened columns
    df_flat = df.select([expr(col).alias(col.split(".")[-1].replace(separator, "_")) for col in flat_cols])

    # If any columns were flattened, recursively call the function again
    if len(df_flat.columns) > len(df.columns) - len(array_cols):
        return flatten_delta_table(df_flat, columns_to_flatten, separator)
    else:
        return df_flat

# Test case
def test_flatten_delta_table():
    # Create a SparkSession
    spark = SparkSession.builder.appName("FlattenDeltaTableTest").getOrCreate()

    # Create a sample dataset with multiple nested columns
    sample_data = [
        (1, "John", {"age": 30, "city": "New York"}, [{"type": "home", "number": "123-456-7890"}, {"type": "work", "number": "098-765-4321"}], {"scores": [85, 90, 78], "average": 84.3}),
        (2, "Alice", {"age": 25, "city": "San Francisco"}, [{"type": "home", "number": "111-222-3333"}], {"scores": [92, 88, 95], "average": 91.7}),
        (3, "Bob", {"age": 35, "city": "Chicago"}, [], {"scores": [75, 80, 82], "average": 79.0})
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

    # Test case 2: Flatten specific columns
    df_flat_specific = flatten_delta_table(df, columns_to_flatten=["info", "phones"])
    print("\nFlattened DataFrame (specific columns: info, phones):")
    df_flat_specific.show(truncate=False)
    df_flat_specific.printSchema()

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    test_flatten_delta_table()
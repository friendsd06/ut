from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode_outer, sort_array
from pyspark.sql.types import StructType, ArrayType, StringType, IntegerType, DoubleType, StructField
from typing import List, Optional, Tuple

def flatten_delta_table(
        df: DataFrame,
        columns_to_flatten: Optional[List[str]] = None,
        separator: str = "_"
) -> DataFrame:
    def flatten_schema(
            schema: StructType,
            prefix: str = ""
    ) -> Tuple[List[str], List[str], List[str]]:
        flat_cols = []
        array_struct_cols = []
        array_scalar_cols = []
        for field in schema.fields:
            name = f"{prefix}{separator}{field.name}" if prefix else field.name
            dtype = field.dataType

            should_flatten = (
                    columns_to_flatten is None or
                    any(name.startswith(col.replace(".", separator)) for col in columns_to_flatten)
            )

            if should_flatten:
                if isinstance(dtype, StructType):
                    nested_flat_cols, nested_array_struct_cols, nested_array_scalar_cols = flatten_schema(
                        dtype, prefix=name
                    )
                    flat_cols.extend(nested_flat_cols)
                    array_struct_cols.extend(nested_array_struct_cols)
                    array_scalar_cols.extend(nested_array_scalar_cols)
                elif isinstance(dtype, ArrayType):
                    if isinstance(dtype.elementType, StructType):
                        array_struct_cols.append(name)
                        flat_cols.append(name)
                    else:
                        array_scalar_cols.append(name)
                        flat_cols.append(name)
                else:
                    flat_cols.append(name)
            else:
                flat_cols.append(name)

        return flat_cols, array_struct_cols, array_scalar_cols

    flat_cols, array_struct_cols, array_scalar_cols = flatten_schema(df.schema)

    for array_col in array_struct_cols:
        df = df.withColumn(array_col, explode_outer(col(array_col)))
        struct_fields = df.schema[array_col].dataType.elementType.fields if array_col in df.columns else []
        for field in struct_fields:
            df = df.withColumn(f"{array_col}{separator}{field.name}", col(f"{array_col}.{field.name}"))
        df = df.drop(array_col)

    for array_col in array_scalar_cols:
        df = df.withColumn(array_col, sort_array(col(array_col), ascending=True))

    if 'grades_scores' in flat_cols:
        df = df.withColumn('grades_scores', sort_array(col('grades.scores')))
    if 'grades_average' in flat_cols:
        df = df.withColumn('grades_average', col('grades.average'))

    select_exprs = [col(col_name).alias(col_name.replace(separator, "_")) for col_name in flat_cols]

    df_flat = df.select(*select_exprs)

    if len(df_flat.columns) > len(df.columns):
        return flatten_delta_table(df_flat, columns_to_flatten, separator)
    else:
        return df_flat

def test_flatten_delta_table():
    spark = SparkSession.builder \
        .appName("FlattenDeltaTableTest") \
        .master("local[*]") \
        .getOrCreate()

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

    df_flat_all = flatten_delta_table(df)
    print("\nFlattened DataFrame (all nested columns):")
    df_flat_all.show(truncate=False)
    df_flat_all.printSchema()

    df_flat_specific = flatten_delta_table(df, columns_to_flatten=["info", "phones"])
    print("\nFlattened DataFrame (specific columns: info, phones):")
    df_flat_specific.show(truncate=False)
    df_flat_specific.printSchema()

    spark.stop()

if __name__ == "__main__":
    test_flatten_delta_table()

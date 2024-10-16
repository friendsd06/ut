from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import struct, array, map_from_arrays, col

# Initialize Spark Session
spark = SparkSession.builder.appName("ComplexFlattenScenarios").getOrCreate()

# Scenario 1: Deeply Nested Structure with All Data Types
schema1 = StructType([
    StructField("id", StringType(), True),
    StructField("nested_data", StructType([
        StructField("array_of_structs", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("flag", BooleanType(), True),
            StructField("nested_array", ArrayType(IntegerType()), True)
        ])), True),
        StructField("map_of_arrays", MapType(StringType(), ArrayType(StringType())), True),
        StructField("struct_with_map", StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("complex_map", MapType(StringType(), StructType([
                StructField("key1", LongType(), True),
                StructField("key2", ArrayType(FloatType()), True)
            ])), True)
        ]), True)
    ]), True)
])

data1 = [
    ("ID1", {
        "array_of_structs": [
            {"name": "Item1", "value": 10.5, "flag": True, "nested_array": [1, 2, 3]},
            {"name": "Item2", "value": 20.7, "flag": False, "nested_array": [4, 5, 6]}
        ],
        "map_of_arrays": {"key1": ["a", "b", "c"], "key2": ["d", "e", "f"]},
        "struct_with_map": {
            "timestamp": "2023-01-01 12:00:00",
            "complex_map": {
                "mapKey1": {"key1": 100, "key2": [1.1, 2.2, 3.3]},
                "mapKey2": {"key1": 200, "key2": [4.4, 5.5, 6.6]}
            }
        }
    })
]

df1 = spark.createDataFrame(data1, schema1)

# Scenario 2: Array of Arrays with Mixed Types
schema2 = StructType([
    StructField("id", StringType(), True),
    StructField("mixed_array", ArrayType(ArrayType(StringType())), True),
    StructField("complex_array", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("values", ArrayType(ArrayType(IntegerType())), True)
    ])), True)
])

data2 = [
    ("ID2",
     [["a", "b"], ["c", "d", "e"], ["f"]],
     [
         {"name": "Group1", "values": [[1, 2], [3, 4, 5]]},
         {"name": "Group2", "values": [[6, 7, 8], [9, 10]]}
     ]
     )
]

df2 = spark.createDataFrame(data2, schema2)

# Scenario 3: Deeply Nested Maps and Structs
schema3 = StructType([
    StructField("id", StringType(), True),
    StructField("nested_map", MapType(StringType(), MapType(StringType(), StructType([
        StructField("field1", IntegerType(), True),
        StructField("field2", ArrayType(MapType(StringType(), StringType())), True)
    ]))), True)
])

data3 = [
    ("ID3", {
        "outer_key1": {
            "inner_key1": {"field1": 100, "field2": [{"a": "1"}, {"b": "2"}]},
            "inner_key2": {"field1": 200, "field2": [{"c": "3"}, {"d": "4"}]}
        },
        "outer_key2": {
            "inner_key3": {"field1": 300, "field2": [{"e": "5"}, {"f": "6"}]},
            "inner_key4": {"field1": 400, "field2": [{"g": "7"}, {"h": "8"}]}
        }
    })
]

df3 = spark.createDataFrame(data3, schema3)

# Scenario 4: Complex Struct with Nullable Fields and Empty Arrays
schema4 = StructType([
    StructField("id", StringType(), True),
    StructField("nullable_struct", StructType([
        StructField("field1", StringType(), True),
        StructField("field2", IntegerType(), True),
        StructField("nullable_array", ArrayType(StringType()), True),
        StructField("empty_array", ArrayType(IntegerType()), True),
        StructField("nullable_map", MapType(StringType(), StringType()), True)
    ]), True),
    StructField("array_with_nulls", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True)
    ])), True)
])

data4 = [
    ("ID4",
     {"field1": "value1", "field2": None, "nullable_array": None, "empty_array": [], "nullable_map": None},
     [{"name": "Item1", "value": 10.5}, {"name": None, "value": None}, {"name": "Item3", "value": 30.7}]
     )
]

df4 = spark.createDataFrame(data4, schema4)

# Scenario 5: Extremely Nested Structure with All Types
schema5 = StructType([
    StructField("id", StringType(), True),
    StructField("level1", StructType([
        StructField("array1", ArrayType(StructType([
            StructField("map1", MapType(StringType(), ArrayType(StructType([
                StructField("field1", IntegerType(), True),
                StructField("field2", ArrayType(MapType(StringType(), StringType())), True)
            ]))))
        ])), True),
        StructField("struct1", StructType([
            StructField("array2", ArrayType(MapType(StringType(), StructType([
                StructField("nested_array", ArrayType(StructType([
                    StructField("name", StringType(), True),
                    StructField("value", DoubleType(), True)
                ]))),
                StructField("nested_map", MapType(StringType(), ArrayType(IntegerType())))
            ]))), True)
        ]), True)
    ]), True)
])

data5 = [
    ("ID5", {
        "array1": [
            {"map1": {"key1": [{"field1": 1, "field2": [{"a": "1"}, {"b": "2"}]}],
                      "key2": [{"field1": 2, "field2": [{"c": "3"}, {"d": "4"}]}]}}
        ],
        "struct1": {
            "array2": [
                {"key3": {"nested_array": [{"name": "Item1", "value": 10.5}, {"name": "Item2", "value": 20.7}],
                          "nested_map": {"m1": [1, 2, 3], "m2": [4, 5, 6]}}},
                {"key4": {"nested_array": [{"name": "Item3", "value": 30.9}, {"name": "Item4", "value": 40.1}],
                          "nested_map": {"m3": [7, 8, 9], "m4": [10, 11, 12]}}}
            ]
        }
    })
]

df5 = spark.createDataFrame(data5, schema5)

# Function to recursively flatten nested structures
from pyspark.sql.functions import explode, col

def flatten_df(nested_df):
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()

        for column_name, column_type in df.dtypes:
            if column_type.startswith("struct"):
                projected_df = df.select(col("{}.*".format(column_name)))
                stack.append((parents + (column_name,), projected_df))
            elif column_type.startswith("array"):
                projected_df = df.select(explode(col(column_name)).alias(column_name))
                stack.append((parents + (column_name,), projected_df))
            else:
                columns.append(col(".".join(parents + (column_name,))).alias("_".join(parents + (column_name,))))

    return nested_df.select(columns)

# Example usage:
# flat_df1 = flatten_df(df1)
# flat_df1.show(truncate=False)
# flat_df1.printSchema()

# Repeat for other dataframes (df2, df3, df4, df5)

# Don't forget to stop the Spark session when you're done
# spark.stop()
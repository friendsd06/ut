from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType

# Initialize Spark Session
spark = SparkSession.builder.appName("FlattenNestedDataFrameScenarios").getOrCreate()

# Scenario 1: Simple Nested Structure
schema1 = StructType([
    StructField("name", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True)
    ]), True)
])

data1 = [("John Doe", {"street": "123 Main St", "city": "Anytown"})]
df1 = spark.createDataFrame(data1, schema1)

# Scenario 2: Array of Primitives
schema2 = StructType([
    StructField("name", StringType(), True),
    StructField("hobbies", ArrayType(StringType()), True)
])

data2 = [("Jane Smith", ["reading", "swimming", "hiking"])]
df2 = spark.createDataFrame(data2, schema2)

# Scenario 3: Nested Array of Structs
schema3 = StructType([
    StructField("name", StringType(), True),
    StructField("orders", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("item", StringType(), True),
        StructField("price", DoubleType(), True)
    ])), True)
])

data3 = [("Alice Johnson", [{"id": 1, "item": "Book", "price": 15.99}, {"id": 2, "item": "Pen", "price": 1.99}])]
df3 = spark.createDataFrame(data3, schema3)

# Scenario 4: Deep Nesting
schema4 = StructType([
    StructField("user", StructType([
        StructField("personal", StructType([
            StructField("name", StructType([
                StructField("first", StringType(), True),
                StructField("last", StringType(), True)
            ]), True)
        ]), True),
        StructField("professional", StructType([
            StructField("company", StructType([
                StructField("name", StringType(), True),
                StructField("position", StringType(), True)
            ]), True)
        ]), True)
    ]), True)
])

data4 = [({"personal": {"name": {"first": "Bob", "last": "Brown"}}, "professional": {"company": {"name": "Tech Corp", "position": "Developer"}}})]
df4 = spark.createDataFrame(data4, schema4)

# Scenario 5: Array of Arrays
schema5 = StructType([
    StructField("name", StringType(), True),
    StructField("matrix", ArrayType(ArrayType(IntegerType())), True)
])

data5 = [("Charlie Davis", [[1, 2], [3, 4], [5, 6]])]
df5 = spark.createDataFrame(data5, schema5)

# Scenario 6: Mixed Nested Types
schema6 = StructType([
    StructField("name", StringType(), True),
    StructField("details", StructType([
        StructField("age", IntegerType(), True),
        StructField("contacts", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("value", StringType(), True)
        ])), True),
        StructField("address", StructType([
            StructField("home", StructType([
                StructField("street", StringType(), True),
                StructField("city", StringType(), True)
            ]), True),
            StructField("work", StructType([
                StructField("street", StringType(), True),
                StructField("city", StringType(), True)
            ]), True)
        ]), True)
    ]), True)
])

data6 = [("David Evans", {
    "age": 30,
    "contacts": [{"type": "email", "value": "david@example.com"}, {"type": "phone", "value": "123-456-7890"}],
    "address": {
        "home": {"street": "456 Elm St", "city": "Sometown"},
        "work": {"street": "789 Oak Rd", "city": "Othertown"}
    }
})]
df6 = spark.createDataFrame(data6, schema6)

# Scenario 7: Nested Maps
schema7 = StructType([
    StructField("name", StringType(), True),
    StructField("preferences", StructType([
        StructField("colors", MapType(StringType(), StringType()), True),
        StructField("sizes", MapType(StringType(), StringType()), True)
    ]), True)
])

data7 = [("Eva Fischer", {
    "colors": {"primary": "blue", "secondary": "green"},
    "sizes": {"shirt": "M", "shoes": "8"}
})]
df7 = spark.createDataFrame(data7, schema7)

# Scenario 8: Array with Mixed Types
# Note: PySpark doesn't support arrays with mixed types directly, so we'll use string representation
schema8 = StructType([
    StructField("name", StringType(), True),
    StructField("data", ArrayType(StringType()), True)
])

data8 = [("Frank Garcia", ["42", "string", '{"key": "value"}', "[1, 2, 3]"])]
df8 = spark.createDataFrame(data8, schema8)

# Scenario 9: Deeply Nested Array of Structs
schema9 = StructType([
    StructField("name", StringType(), True),
    StructField("family", StructType([
        StructField("parents", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("occupation", StructType([
                StructField("title", StringType(), True),
                StructField("speciality", StringType(), True),
                StructField("experience", ArrayType(StructType([
                    StructField("year", IntegerType(), True),
                    StructField("position", StringType(), True)
                ])), True)
            ]), True)
        ])), True)
    ]), True)
])

data9 = [("Grace Holmes", {
    "parents": [
        {
            "name": "Emma",
            "occupation": {
                "title": "Doctor",
                "speciality": "Pediatrics",
                "experience": [
                    {"year": 2010, "position": "Resident"},
                    {"year": 2015, "position": "Attending"}
                ]
            }
        },
        {
            "name": "James",
            "occupation": {
                "title": "Engineer",
                "speciality": "Software",
                "experience": [
                    {"year": 2005, "position": "Junior Developer"},
                    {"year": 2010, "position": "Senior Developer"}
                ]
            }
        }
    ]
})]
df9 = spark.createDataFrame(data9, schema9)

# Scenario 10: Complex Nested Structure with Multiple Array and Map Levels
schema10 = StructType([
    StructField("company", StringType(), True),
    StructField("departments", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("teams", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("projects", ArrayType(StructType([
                StructField("name", StringType(), True),
                StructField("tasks", ArrayType(StructType([
                    StructField("id", IntegerType(), True),
                    StructField("description", StringType(), True),
                    StructField("status", StringType(), True)
                ])), True),
                StructField("resources", StructType([
                    StructField("budget", IntegerType(), True),
                    StructField("team_members", ArrayType(StringType()), True),
                    StructField("tools", MapType(StringType(), ArrayType(StringType())), True)
                ]), True)
            ])), True)
        ])), True)
    ])), True)
])

data10 = [("Tech Innovations Inc.", [
    {
        "name": "Engineering",
        "teams": [
            {
                "name": "Frontend",
                "projects": [
                    {
                        "name": "Website Redesign",
                        "tasks": [
                            {"id": 1, "description": "Mock-up designs", "status": "completed"},
                            {"id": 2, "description": "Implement responsive layout", "status": "in-progress"}
                        ],
                        "resources": {
                            "budget": 50000,
                            "team_members": ["Alice", "Bob", "Charlie"],
                            "tools": {
                                "design": ["Figma", "Sketch"],
                                "development": ["React", "TypeScript"]
                            }
                        }
                    }
                ]
            },
            {
                "name": "Backend",
                "projects": [
                    {
                        "name": "API Optimization",
                        "tasks": [
                            {"id": 3, "description": "Analyze current performance", "status": "completed"},
                            {"id": 4, "description": "Implement caching", "status": "pending"}
                        ],
                        "resources": {
                            "budget": 30000,
                            "team_members": ["David", "Eva"],
                            "tools": {
                                "development": ["Node.js", "MongoDB"],
                                "monitoring": ["Grafana", "Prometheus"]
                            }
                        }
                    }
                ]
            }
        ]
    }
])]
df10 = spark.createDataFrame(data10, schema10)

# Function to flatten nested DataFrame
def flatten_nested_dataframe(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df

# Example usage:
# flat_df1 = flatten_nested_dataframe(df1)
# flat_df1.show(truncate=False)
# flat_df1.printSchema()

# Repeat for other dataframes (df2, df3, ..., df10)

# Don't forget to stop the Spark session when you're done
# spark.stop()
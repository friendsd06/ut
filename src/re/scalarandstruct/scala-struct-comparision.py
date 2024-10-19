from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("Scalar and Struct Comparison").getOrCreate()

# Sample data with scalar and struct types
source_data = [
    (1, 'John', 5000, ('US', 'New York', 'East'), ('Developer', 'IT', 'Junior'), ('Single', 'Male', '25')),
    (2, 'Alice', 7000, ('Canada', 'Toronto', 'North'), ('Analyst', 'Finance', 'Senior'), ('Married', 'Female', '30')),
    (3, 'Bob', 6000, ('UK', 'London', 'South'), ('Manager', 'HR', 'Lead'), ('Single', 'Male', '35'))
]

target_data = [
    (1, 'John', 5500, ('US', 'New York', 'East'), ('Developer', 'IT', 'Junior'), ('Single', 'Male', '26')),  # Salary and age difference
    (2, 'Alice', 7000, ('Canada', 'Vancouver', 'West'), ('Analyst', 'Finance', 'Senior'), ('Married', 'Female', '30')),  # City difference
    (3, 'Bob', 6000, ('UK', 'London', 'South'), ('Manager', 'HR', 'Lead'), ('Single', 'Male', '35'))  # No difference
]

# Define schema with scalar and struct fields
schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),  # Scalar column
    StructField('salary', IntegerType(), True),  # Scalar column
    StructField('location', StructType([  # Struct column
        StructField('country', StringType(), True),
        StructField('city', StringType(), True),
        StructField('region', StringType(), True)
    ]), True),
    StructField('job', StructType([  # Struct column
        StructField('title', StringType(), True),
        StructField('department', StringType(), True),
        StructField('level', StringType(), True)
    ]), True),
    StructField('personal_info', StructType([  # Struct column
        StructField('marital_status', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('age', StringType(), True)
    ]), True)
])

# Create DataFrames for source and target
source_df = spark.createDataFrame(source_data, schema=schema).alias("source")
target_df = spark.createDataFrame(target_data, schema=schema).alias("target")

# Join source and target on the primary key (id)
joined_df = source_df.join(target_df, on="id", how="inner")

# Function to consolidate differences for struct fields
def consolidate_struct_diff(parent_field, sub_fields):
    """
    Consolidate the differences for struct fields into one column.
    :param parent_field: The parent field (struct column)
    :param sub_fields: The list of sub-fields within the struct column
    :return: A column with consolidated differences for the struct field
    """
    sub_field_comparisons = []
    for sub_field in sub_fields:
        sub_field_comparisons.append(
            F.when(
                F.col(f"source.{parent_field}.{sub_field}") != F.col(f"target.{parent_field}.{sub_field}"),
                F.concat(
                    F.lit(f"{sub_field}: Source = "), F.col(f"source.{parent_field}.{sub_field}"),
                    F.lit(", Target = "), F.col(f"target.{parent_field}.{sub_field}")
                )
            ).otherwise("")
        )

    # Combine all differences into one column using concat_ws
    return F.concat_ws(", ", *sub_field_comparisons).alias(f"{parent_field}_diff")

# Scalar fields comparison (2 scalar fields: name, salary)
scalar_comparisons = [
    F.when(
        F.col(f"source.{field}") != F.col(f"target.{field}"),
        F.concat(F.lit(f"Source = "), F.col(f"source.{field}"), F.lit(", Target = "), F.col(f"target.{field}"))
    ).alias(f"{field}_diff")
    for field in ['name', 'salary']
]

# Struct fields comparison (3 struct fields: location, job, personal_info)
struct_fields = {
    'location': ['country', 'city', 'region'],
    'job': ['title', 'department', 'level'],
    'personal_info': ['marital_status', 'gender', 'age']
}


# Generate comparison for struct fields
struct_comparisons = [consolidate_struct_diff(field, sub_fields) for field, sub_fields in struct_fields.items()]

# Select the primary key and all comparison columns (scalar and struct)
result_df = joined_df.select(
    'id',  # Primary key
    *scalar_comparisons,
    *struct_comparisons
)

# Show the final result DataFrame
result_df.show(truncate=False)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("Filter Columns with Differences").getOrCreate()

# Sample data with scalar and struct fields (as before)
source_data = [
    (1, 'John', 5000, 'IT', 'New York', 'M', 'Single', 'USA', 'NY', 25,
     ('US', 'East', 'NY'), ('Developer', 'Junior', 'IT'), ('Married', 'Male', '30'), ('Car', 'Toyota', 'Blue'), ('A', 'B', 'C')),
    (2, 'Alice', 7000, 'Finance', 'Toronto', 'F', 'Married', 'Canada', 'ON', 30,
     ('Canada', 'North', 'Toronto'), ('Analyst', 'Senior', 'Finance'), ('Single', 'Female', '28'), ('Bike', 'Honda', 'Red'), ('D', 'E', 'F')),
    (3, 'Bob', 6000, 'HR', 'London', 'M', 'Single', 'UK', 'LDN', 35,
     ('UK', 'South', 'London'), ('Manager', 'Lead', 'HR'), ('Married', 'Male', '40'), ('Bus', 'Volvo', 'Green'), ('G', 'H', 'I'))
]

target_data = [
    (1, 'John', 5500, 'IT', 'New York', 'M', 'Single', 'USA', 'NY', 26,
     ('US', 'East', 'NY'), ('Developer', 'Junior', 'IT'), ('Married', 'Male', '30'), ('Car', 'Toyota', 'Blue'), ('A', 'B', 'C')),
    (2, 'Alice', 7000, 'Finance', 'Vancouver', 'F', 'Married', 'Canada', 'BC', 30,
     ('Canada', 'West', 'Vancouver'), ('Analyst', 'Senior', 'Finance'), ('Single', 'Female', '28'), ('Bike', 'Honda', 'Red'), ('D', 'E', 'G')),
    (3, 'Bob', 6000, 'HR', 'London', 'M', 'Single', 'UK', 'LDN', 35,
     ('UK', 'South', 'London'), ('Manager', 'Lead', 'HR'), ('Married', 'Male', '40'), ('Bus', 'Volvo', 'Green'), ('G', 'H', 'I'))
]

# Define schema with scalar and struct fields
schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('salary', IntegerType(), True),
    StructField('department', StringType(), True),
    StructField('city', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('marital_status', StringType(), True),
    StructField('country', StringType(), True),
    StructField('state', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('location', StructType([
        StructField('country', StringType(), True),
        StructField('region', StringType(), True),
        StructField('state', StringType(), True)
    ]), True),
    StructField('job', StructType([
        StructField('title', StringType(), True),
        StructField('level', StringType(), True),
        StructField('department', StringType(), True)
    ]), True),
    StructField('personal_info', StructType([
        StructField('marital_status', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('age', StringType(), True)
    ]), True),
    StructField('vehicle', StructType([
        StructField('type', StringType(), True),
        StructField('brand', StringType(), True),
        StructField('color', StringType(), True)
    ]), True),
    StructField('other_info', StructType([
        StructField('field1', StringType(), True),
        StructField('field2', StringType(), True),
        StructField('field3', StringType(), True)
    ]), True)
])

# Create DataFrames for source and target
source_df = spark.createDataFrame(source_data, schema=schema).alias("source")
target_df = spark.createDataFrame(target_data, schema=schema).alias("target")

# Join source and target on the primary key (id)
joined_df = source_df.join(target_df, on="id", how="inner")

# 1. Identify Struct Columns Automatically
struct_columns = [field.name for field in schema.fields if isinstance(field.dataType, StructType)]

# 2. Extract Scalar Columns
scalar_columns = [field.name for field in schema.fields if field.name not in struct_columns]

# 3. Process Scalar Columns Separately
scalar_comparisons = joined_df.select(
    'id',
    *[
        F.when(
            F.col(f"source.{field}") != F.col(f"target.{field}"),
            F.concat(F.lit(f"Source = "), F.col(f"source.{field}"), F.lit(", Target = "), F.col(f"target.{field}"))
        ).alias(f"{field}_diff")
        for field in scalar_columns
    ]
)

# 4. Process Struct Columns Separately
def consolidate_struct_diff(parent_field, sub_fields):
    """
    Consolidate the differences for struct fields into one column.
    :param parent_field: The parent field (struct column)
    :param sub_fields: The list of sub-fields within the struct column
    :return: A column with consolidated differences for the struct field
    """
    sub_field_comparisons = [
        F.when(
            F.col(f"source.{parent_field}.{sub_field}") != F.col(f"target.{parent_field}.{sub_field}"),
            F.concat(
                F.lit(f"{sub_field}: Source = "), F.col(f"source.{parent_field}.{sub_field}"),
                F.lit(", Target = "), F.col(f"target.{parent_field}.{sub_field}")
            )
        ).otherwise("")
        for sub_field in sub_fields
    ]

    return F.concat_ws(", ", *sub_field_comparisons).alias(f"{parent_field}_diff")

struct_comparisons = joined_df.select(
    'id',
    *[consolidate_struct_diff(field, schema[field].dataType.names) for field in struct_columns]
)

# 5. Join the results from scalar and struct comparisons
final_result_df = scalar_comparisons.join(struct_comparisons, on='id', how='inner')

# 6. Filter only columns with non-empty differences
diff_columns = [col for col in final_result_df.columns if col != 'id']

# Create a filter expression to retain only non-empty difference columns
filter_expr = F.greatest(*[F.col(col) != '' for col in diff_columns])

# Filter the DataFrame to show only rows with differences and non-empty difference columns
filtered_result_df = final_result_df.filter(filter_expr)

# Select only non-empty difference columns
non_empty_columns = ['id'] + [col for col in diff_columns if filtered_result_df.select(col).filter(F.col(col) != '').count() > 0]
filtered_result_df = filtered_result_df.select(non_empty_columns)

# Show the filtered DataFrame with only columns that have differences
filtered_result_df.show(truncate=False)

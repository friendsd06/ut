from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a Spark session
spark = SparkSession.builder.appName("Reconciliation Example").getOrCreate()

# Sample source and target DataFrames
source_data = [
    ('1', 'John', '5000'),
    ('2', 'Alice', '7000'),
    ('3', 'Bob', '6000'),
    ('4', 'Charlie', '8000')
]

target_data = [
    ('1', 'John', '5500'),   # Difference in salary
    ('2', 'Alice', '7000'),  # No difference
    ('3', 'Bob', '6500'),    # Difference in salary
    ('4', 'Charlie', None)   # Difference in salary (NULL)
]

columns = ['id', 'name', 'salary']

# Create DataFrames
source_df = spark.createDataFrame(source_data, schema=columns).alias("source")
target_df = spark.createDataFrame(target_data, schema=columns).alias("target")

# List of columns to compare (except primary key column 'id')
columns_to_compare_final = ['name', 'salary']

# Join source and target on the primary key (id)
value_mismatches = source_df.join(target_df, on="id", how="inner")

# Create the reconciliation DataFrame with descriptive messages
corrected_detailed_mismatches = value_mismatches.select(
    'id',  # Primary key column
    *[
        F.concat(
            F.lit(f"Source = "),
            F.when(F.col(f"source.{c}").isNull(), "NULL").otherwise(F.col(f"source.{c}").cast("string")),
            F.lit(", Target = "),
            F.when(F.col(f"target.{c}").isNull(), "NULL").otherwise(F.col(f"target.{c}").cast("string"))
        ).alias(c)
        for c in columns_to_compare_final
    ]
).filter(
    " OR ".join([f"source.{c} != target.{c} OR (source.{c} IS NULL AND target.{c} IS NOT NULL) OR (source.{c} IS NOT NULL AND target.{c} IS NULL)" for c in columns_to_compare_final])
)

# Show the output
corrected_detailed_mismatches.show(truncate=False)


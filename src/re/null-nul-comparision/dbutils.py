# Define source and target paths
source_path = "s3a://your-bucket/path/to/data/partition43543/"
target_path = "s3a://your-bucket/path/to/data/partition45654/"

# Use dbutils.fs.cp to copy files from source to target
dbutils.fs.cp(source_path, target_path, recurse=True)


def clean_redundant_nulls(df, columns):
    for column in columns:
        # Replace multiple ", , " with a single empty string
        df = df.withColumn(column, regexp_replace(col(column), r"(, )+", ""))
        # Remove leading or trailing commas
        df = df.withColumn(column, regexp_replace(col(column), r"^,|,$", ""))

    return df

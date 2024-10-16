from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, col, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, MapType,
    TimestampType, DecimalType, BinaryType
)
from datetime import datetime
import logging
from decimal import Decimal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_nested_columns(schema, prefix=''):
    """
    Recursively retrieves all nested columns (StructType, ArrayType, MapType).

    :param schema: StructType schema of the DataFrame.
    :param prefix: Current prefix for nested fields.
    :return: List of nested column paths.
    """
    nested_cols = []

    # Ensure the schema is a StructType
    if not isinstance(schema, StructType):
        logger.error("Provided schema is not a StructType.")
        return nested_cols

    for field in schema.fields:
        field_name = f"{prefix}.{field.name}" if prefix else field.name
        if isinstance(field.dataType, (StructType, ArrayType, MapType)):
            nested_cols.append(field_name)
            # Recurse only if the dataType is complex
            if isinstance(field.dataType, StructType):
                nested_cols += get_nested_columns(field.dataType, prefix=field_name)
            elif isinstance(field.dataType, ArrayType):
                element_type = field.dataType.elementType
                if isinstance(element_type, (StructType, ArrayType, MapType)):
                    # For ArrayType of complex types, recurse accordingly
                    if isinstance(element_type, StructType):
                        nested_cols += get_nested_columns(element_type, prefix=field_name)
                    elif isinstance(element_type, ArrayType):
                        nested_cols += get_nested_columns(element_type, prefix=field_name)
                    elif isinstance(element_type, MapType):
                        nested_cols += get_nested_columns(element_type.valueType, prefix=field_name)
            elif isinstance(field.dataType, MapType):
                value_type = field.dataType.valueType
                if isinstance(value_type, (StructType, ArrayType, MapType)):
                    # For MapType of complex types, recurse accordingly
                    if isinstance(value_type, StructType):
                        nested_cols += get_nested_columns(value_type, prefix=field_name)
                    elif isinstance(value_type, ArrayType):
                        nested_cols += get_nested_columns(value_type, prefix=field_name)
                    elif isinstance(value_type, MapType):
                        nested_cols += get_nested_columns(value_type.valueType, prefix=field_name)
    return nested_cols

def flatten_map(df, map_column, prefix=None):
    """
    Flattens a MapType column into separate key and value columns.

    :param df: Input DataFrame.
    :param map_column: Name of the MapType column to flatten.
    :param prefix: Optional prefix for new column names.
    :return: DataFrame with flattened MapType column.
    """
    exploded_map = f"{map_column}_exploded"
    df = df.withColumn(exploded_map, explode_outer(col(map_column)))
    df = df.drop(map_column)

    key_col = f"{prefix}_key" if prefix else "key"
    value_col = f"{prefix}_value" if prefix else "value"

    df = df.withColumn(key_col, col(f"{exploded_map}.key")) \
        .withColumn(value_col, col(f"{exploded_map}.value")) \
        .drop(exploded_map)

    logger.info(f"Flattened MapType column: '{map_column}' into '{key_col}' and '{value_col}'")
    return df

def flatten_dataframe_recursive(df, columns_to_explode=None):
    """
    Recursively flattens a nested DataFrame until no ArrayType, StructType, or MapType columns remain.
    Allows selective exploding of specified ArrayType columns.

    :param df: Input DataFrame.
    :param columns_to_explode: List of column paths (dot notation) to explode. If None, all ArrayType columns are exploded.
    :return: Fully flattened DataFrame.
    """
    if columns_to_explode is not None:
        # Normalize column paths
        columns_to_explode = set(columns_to_explode)
        logger.info(f"Columns to explode: {columns_to_explode}")
    else:
        logger.info("No specific columns to explode provided. All ArrayType columns will be exploded.")

    nested_cols = get_nested_columns(df.schema)
    iteration = 1

    while nested_cols:
        logger.info(f"Flattening Iteration: {iteration}")
        for column in nested_cols:
            parts = column.split('.')
            current_field = df.schema
            try:
                for part in parts:
                    current_field = current_field[part].dataType
            except Exception as e:
                logger.error(f"Skipping invalid column path: '{column}'. Error: {e}")
                continue

            # Handling ArrayType
            if isinstance(current_field, ArrayType):
                should_explode = False
                if columns_to_explode:
                    if column in columns_to_explode:
                        should_explode = True
                    else:
                        logger.info(f"Skipping exploding ArrayType column: '{column}' as it's not specified in 'columns_to_explode'")

                if not columns_to_explode or should_explode:
                    exploded_alias = f"{parts[-1]}_exploded"
                    logger.info(f"Exploding ArrayType column: '{column}' as '{exploded_alias}'")
                    df = df.withColumn(exploded_alias, explode_outer(col(column)))
                    df = df.drop(column)

                    element_type = current_field.elementType

                    if isinstance(element_type, StructType):
                        # Flatten StructType elements within ArrayType
                        for subfield in element_type.fields:
                            subfield_name = subfield.name
                            new_col_name = f"{parts[-1]}_{subfield_name}_exploded"
                            if new_col_name in df.columns:
                                logger.warning(f"Column '{new_col_name}' already exists. Skipping to prevent collision.")
                                continue
                            df = df.withColumn(new_col_name, col(f"{exploded_alias}.{subfield_name}"))
                            logger.info(f"Added column: '{new_col_name}' from '{exploded_alias}.{subfield_name}'")
                        df = df.drop(exploded_alias)
                    elif isinstance(element_type, MapType):
                        # Flatten MapType elements within ArrayType
                        df = flatten_map(df, exploded_alias, prefix=parts[-1])
                    elif isinstance(element_type, ArrayType):
                        # Handle nested ArrayType (Arrays within Arrays)
                        logger.info(f"Handling nested ArrayType column: '{exploded_alias}'")
                        df = df.withColumn(exploded_alias, explode_outer(col(exploded_alias)))
                        # The nested Array will be handled in the next iterations
                    else:
                        # Rename primitive exploded column
                        new_col_name = f"{parts[-1]}_exploded"
                        if new_col_name in df.columns:
                            logger.warning(f"Column '{new_col_name}' already exists. Skipping to prevent collision.")
                            continue
                        df = df.withColumnRenamed(exploded_alias, new_col_name)
                        logger.info(f"Renamed column: '{exploded_alias}' to '{new_col_name}'")
                else:
                    # If not exploding, decide how to handle the ArrayType column.
                    # Optionally, you can flatten the struct inside without exploding.
                    # For now, we'll skip flattening ArrayType columns not specified to explode.
                    logger.info(f"Skipping ArrayType column: '{column}' as it's not specified to explode.")

            # Handling StructType
            elif isinstance(current_field, StructType):
                logger.info(f"Flattening StructType column: '{column}'")
                for subfield in current_field.fields:
                    subfield_name = subfield.name
                    new_col_name = f"{parts[-1]}_{subfield_name}"

                    # Prevent column name collisions
                    if new_col_name in df.columns:
                        logger.warning(f"Column '{new_col_name}' already exists. Skipping to prevent collision.")
                        continue

                    df = df.withColumn(new_col_name, col(f"{column}.{subfield_name}"))
                    logger.info(f"Added column: '{new_col_name}' from '{column}.{subfield_name}'")
                df = df.drop(column)

            # Handling MapType
            elif isinstance(current_field, MapType):
                logger.info(f"Flattening MapType column: '{column}'")
                df = flatten_map(df, column, prefix=parts[-1])

            else:
                # Handling other complex types like BinaryType, TimestampType, etc.
                logger.info(f"Column '{column}' is a primitive type or unsupported complex type. Skipping.")

        # Update nested columns for the next iteration
        nested_cols = get_nested_columns(df.schema)
        logger.info(f"Nested columns remaining: {nested_cols}")
        iteration += 1

    logger.info("Completed flattening the DataFrame.")
    return df

def define_complex_schema() -> StructType:
    """
    Defines a complex schema with various data types, including DecimalType.

    :return: StructType representing the schema.
    """
    return StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("contact_details", StructType([
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True)
        ]), True),
        StructField("addresses", ArrayType(StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True)
        ])), True),
        StructField("preferences", MapType(StringType(), StringType()), True),
        StructField("last_order_timestamp", TimestampType(), True),
        StructField("account_balance", DecimalType(10, 2), True),
        StructField("binary_data", BinaryType(), True),
        StructField("status", StringType(), True)
    ])

def create_complex_sample_data(schema: StructType):
    """
    Creates sample data for source and target DataFrames based on the provided schema.

    :param schema: StructType schema for the DataFrames.
    :return: Tuple of (source_df, target_df)
    """
    source_data = [
        (
            1,
            "John Doe",
            {
                "email": "john.doe@example.com",
                "phone": "555-1234"
            },
            [
                {
                    "street": "123 Main St",
                    "city": "New York",
                    "zip": "10001"
                },
                {
                    "street": "456 Elm St",
                    "city": "Los Angeles",
                    "zip": "90001"
                }
            ],
            {
                "newsletter": "subscribed",
                "sms_alerts": "enabled"
            },
            datetime(2023, 5, 21, 15, 30),
            Decimal("1500.75"),
            b'\x00\x01\x02',
            "active"
        ),
        (
            2,
            "Jane Smith",
            {
                "email": "jane.smith@example.com",
                "phone": "555-5678"
            },
            [
                {
                    "street": "789 Broadway",
                    "city": "Chicago",
                    "zip": "60601"
                }
            ],
            {
                "newsletter": "unsubscribed",
                "sms_alerts": "disabled"
            },
            datetime(2023, 6, 10, 10, 15),
            Decimal("250.00"),
            b'\x03\x04\x05',
            "inactive"
        )
    ]

    target_data = [
        (
            1,
            "John Doe",
            {
                "email": "john.doe@example.com",
                "phone": "555-1234"
            },
            [
                {
                    "street": "123 Main St",
                    "city": "New York",
                    "zip": "10001"
                },
                {
                    "street": "456 Elm St",
                    "city": "Los Angeles",
                    "zip": "90001"
                }
            ],
            {
                "newsletter": "subscribed",
                "sms_alerts": "enabled"
            },
            datetime(2023, 5, 21, 15, 30),
            Decimal("1500.75"),
            b'\x00\x01\x02',
            "active"
        ),
        (
            2,
            "Jane Smith",
            {
                "email": "jane.smith@example.com",
                "phone": "555-5678"
            },
            [
                {
                    "street": "789 Broadway",
                    "city": "Chicago",
                    "zip": "60601"
                }
            ],
            {
                "newsletter": "unsubscribed",
                "sms_alerts": "disabled"
            },
            datetime(2023, 6, 10, 10, 15),
            Decimal("250.00"),
            b'\x03\x04\x05',
            "inactive"
        )
    ]

    # Create DataFrames
    source_df = spark.createDataFrame(source_data, schema)
    target_df = spark.createDataFrame(target_data, schema)

    return source_df, target_df

def compare_counts(source_df, target_df):
    """
    Compares the record counts of source and target DataFrames.

    :param source_df: Source DataFrame.
    :param target_df: Target DataFrame.
    """
    source_count = source_df.count()
    target_count = target_df.count()

    logger.info(f"Source DataFrame Count: {source_count}")
    logger.info(f"Target DataFrame Count: {target_count}")

    if source_count == target_count:
        logger.info("Record counts match.")
    else:
        logger.warning("Record counts do NOT match.")

def compare_dataframes(source_df, target_df):
    """
    Compares the content of source and target DataFrames.

    :param source_df: Source DataFrame.
    :param target_df: Target DataFrame.
    """
    # Add unique identifiers
    source_df_with_id = source_df.withColumn("source_id", lit("source"))
    target_df_with_id = target_df.withColumn("target_id", lit("target"))

    # Perform a full outer join on all columns except the unique identifiers
    join_columns = [col_name for col_name in source_df.columns]

    comparison_df = source_df_with_id.join(
        target_df_with_id,
        on=join_columns,
        how="outer"
    )

    # Identify discrepancies
    discrepancies = comparison_df.filter(
        (col("source_id").isNull()) | (col("target_id").isNull())
    )

    if discrepancies.count() == 0:
        logger.info("All records match between Source and Target DataFrames.")
    else:
        logger.warning("Discrepancies found:")
        discrepancies.show(truncate=False)

def main_example():
    """
    Main function to execute the flattening and reconciliation process.
    """
    global spark  # To make it accessible in create_complex_sample_data
    spark = SparkSession.builder \
        .appName("AdvancedFlatteningExample") \
        .master("local[*]") \
        .getOrCreate()

    # Define the complex schema
    schema_complex = define_complex_schema()

    # Create source and target DataFrames
    source_df_complex, target_df_complex = create_complex_sample_data(schema_complex)

    # Display the source DataFrame
    logger.info("Source DataFrame (Complex Schema):")
    source_df_complex.show(truncate=False)
    source_df_complex.printSchema()

    # Display the target DataFrame
    logger.info("Target DataFrame (Complex Schema):")
    target_df_complex.show(truncate=False)
    target_df_complex.printSchema()

    # Specify columns to explode
    columns_to_explode = ["addresses"]  # Example: only explode 'addresses' column

    # Apply the recursive flattening function with selective exploding
    flattened_source_complex = flatten_dataframe_recursive(source_df_complex, columns_to_explode=columns_to_explode)
    flattened_target_complex = flatten_dataframe_recursive(target_df_complex, columns_to_explode=columns_to_explode)

    # Display the flattened source DataFrame
    logger.info("Flattened Source DataFrame (Complex Schema):")
    flattened_source_complex.show(truncate=False)
    flattened_source_complex.printSchema()

    # Display the flattened target DataFrame
    logger.info("Flattened Target DataFrame (Complex Schema):")
    flattened_target_complex.show(truncate=False)
    flattened_target_complex.printSchema()

    # Perform Reconciliation Testing
    # Compare Counts
    compare_counts(flattened_source_complex, flattened_target_complex)

    # Compare DataFrame Content
    compare_dataframes(flattened_source_complex, flattened_target_complex)

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main_example()

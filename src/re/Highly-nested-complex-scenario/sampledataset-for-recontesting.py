def create_sample_data(schema: StructType):
    """
    Creates and returns two DataFrames (source and target) with sample data for recon testing.

    :param schema: StructType defining the schema of the DataFrames.
    :return: Tuple containing (source_df, target_df).
    """
    # Sample data for source DataFrame
    source_data = [
        (
            1,
            "John Doe",
            {
                "email": "john.doe@example.com",
                "phone": "555-1234"
            },
            28
        ),
        (
            2,
            "Jane Smith",
            {
                "email": "jane.smith@example.com",
                "phone": "555-5678"
            },
            34
        )
    ]

    # Sample data for target DataFrame
    # For recon testing, you can make target identical or introduce differences
    target_data = [
        (
            1,
            "John Doe",
            {
                "email": "john.doe@example.com",
                "phone": "555-1234"
            },
            28
        ),
        (
            2,
            "Jane Smith",
            {
                "email": "jane.smith@example.com",
                "phone": "555-5678"
            },
            34
        )
    ]

    # Create DataFrames
    source_df = spark.createDataFrame(source_data, schema)
    target_df = spark.createDataFrame(target_data, schema)

    return source_df, target_df


def define_schema() -> StructType:
    """
    Defines and returns the schema for the reconciliation test DataFrames.

    :return: StructType representing the schema.
    """
    return StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("contact_details", StructType([
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True)
        ]), True),
        StructField("age", IntegerType(), True)
    ])


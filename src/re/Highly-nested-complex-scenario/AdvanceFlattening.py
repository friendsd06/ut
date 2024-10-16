def flatten_dataframe_recursive(df):
    """
    Recursively flattens a nested DataFrame until no ArrayType, StructType, or MapType columns remain.

    :param df: Input DataFrame.
    :return: Fully flattened DataFrame.
    """
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
                    df = df.withColumnRenamed(exploded_alias, new_col_name)
                    logger.info(f"Renamed column: '{exploded_alias}' to '{new_col_name}'")

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

def remove_all_array_columns(df: DataFrame) -> DataFrame:
    """
    Removes all ArrayType columns from the DataFrame, including nested ArrayType fields within StructType columns.

    Parameters:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The DataFrame without any ArrayType columns.
    """
    def drop_arrays(schema, prefix=''):
        """
        Recursively traverses the schema to identify and remove ArrayType fields.

        Parameters:
            schema (StructType): The schema of the DataFrame or nested StructType.
            prefix (str): The prefix for nested fields.

        Returns:
            list: A list of columns to retain.
        """
        selected_cols = []
        for field in schema.fields:
            field_name = field.name
            full_name = f"{prefix}.{field_name}" if prefix else field_name

            if isinstance(field.dataType, ArrayType):
                print(f"Dropping ArrayType column: {full_name}")
                continue  # Skip ArrayType columns
            elif isinstance(field.dataType, StructType):
                # Recursively process nested StructType
                nested_cols = drop_arrays(field.dataType, prefix=full_name)
                if nested_cols:
                    # Recreate the StructType without ArrayType fields
                    selected_cols.append(struct(*nested_cols).alias(field_name))
                else:
                    # If all nested fields are ArrayType and removed, drop the StructType
                    print(f"Dropping StructType column as all nested fields are ArrayType: {full_name}")
                    continue
            else:
                # Retain non-ArrayType columns
                selected_cols.append(col(full_name))
        return selected_cols

    # Get the list of columns without ArrayType
    new_columns = drop_arrays(df.schema)

    # Select the columns to form the new DataFrame
    return df.select(*new_columns)
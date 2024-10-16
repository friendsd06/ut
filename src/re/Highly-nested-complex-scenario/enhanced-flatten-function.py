from pyspark.sql.functions import explode_outer, col
from pyspark.sql.types import ArrayType, StructType

def flatten_dataframe(df, columns_to_flatten):
    """
    Flattens a nested DataFrame by selectively exploding and flattening specified columns.

    :param df: Input DataFrame with nested schema.
    :param columns_to_flatten: List of column names to flatten (dot notation for nested fields).
    :return: Flattened DataFrame.
    """
    for column in columns_to_flatten:
        # Split the column path to identify parent and field
        parts = column.split('.')
        parent = '.'.join(parts[:-1]) if len(parts) > 1 else parts[0]
        field = parts[-1]

        # Retrieve the data type of the column
        current_field = df.schema
        for part in parts:
            current_field = current_field[part].dataType

        if isinstance(current_field, ArrayType):
            # If the field is an ArrayType, explode it
            exploded_alias = f"{field}_exploded"
            print(f"Exploding ArrayType column: '{column}' as '{exploded_alias}'")
            df = df.withColumn(exploded_alias, explode_outer(col(column)))
            df = df.drop(column)

            # Determine the data type of the exploded column
            exploded_field = current_field.elementType

            if isinstance(exploded_field, StructType):
                # If exploded column is StructType, flatten its fields
                for subfield in exploded_field.fields:
                    subfield_name = subfield.name
                    new_col_name = f"{field}_{subfield_name}_exploded"
                    df = df.withColumn(new_col_name, col(f"{exploded_alias}.{subfield_name}"))
                    print(f"Added column: '{new_col_name}' from '{exploded_alias}.{subfield_name}'")
                df = df.drop(exploded_alias)
            else:
                # If exploded column is primitive, rename it
                new_col_name = f"{field}_exploded"
                df = df.withColumnRenamed(exploded_alias, new_col_name)
                print(f"Renamed column: '{exploded_alias}' to '{new_col_name}'")

        elif isinstance(current_field, StructType):
            # If the field is StructType, flatten its fields without exploding
            print(f"Flattening StructType column: '{column}'")
            for subfield in current_field.fields:
                subfield_name = subfield.name
                new_col_name = f"{field}_{subfield_name}"
                df = df.withColumn(new_col_name, col(f"{column}.{subfield_name}"))
                print(f"Added column: '{new_col_name}' from '{column}.{subfield_name}'")
            df = df.drop(column)

        else:
            # If the field is neither ArrayType nor StructType, skip or handle accordingly
            print(f"Column '{column}' is neither ArrayType nor StructType. Skipping.")

    return df
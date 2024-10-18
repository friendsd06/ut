from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType

def flatten_column(df: DataFrame, col_name: str, prefix_sep: str='_', drop_original: bool=False) -> DataFrame:
    """
    Flattens a specified column in a PySpark DataFrame that contains nested structs or arrays of structs.

    Parameters:
    - df: The PySpark DataFrame.
    - col_name: The name of the column to flatten.
    - prefix_sep: Separator to use between the original column name and the nested field names.
    - drop_original: Whether to drop the original column after flattening.

    Returns:
    - A PySpark DataFrame with the specified column flattened.
    """
    # Check if the column exists
    if col_name not in df.columns:
        raise ValueError(f"Column '{col_name}' does not exist in the DataFrame.")

    # Get the schema of the specified column
    col_field = df.schema[col_name]

    # If the column is an array, explode it
    if isinstance(col_field.dataType, ArrayType):
        df = df.withColumn(col_name, explode_outer(col(col_name)))

    # If the column is a struct or array of structs, flatten it
    if isinstance(col_field.dataType, StructType) or \
            (isinstance(col_field.dataType, ArrayType) and isinstance(col_field.dataType.elementType, StructType)):

        # Define a recursive function to flatten structs
        def flatten_struct(struct_col_name, struct_schema, prefix=''):
            fields = []
            for field in struct_schema.fields:
                field_name = field.name
                field_type = field.dataType
                # Build the full field name with prefix
                full_name = f"{prefix}{prefix_sep}{field_name}" if prefix else field_name

                if isinstance(field_type, StructType):
                    # Recursively flatten nested structs
                    nested_struct_col = f"{struct_col_name}.{field_name}"
                    fields.extend(flatten_struct(nested_struct_col, field_type, full_name))
                else:
                    # Add the field to the list
                    fields.append(col(f"{struct_col_name}.{field_name}").alias(full_name))
            return fields

        # Get all columns except the one to flatten
        other_columns = [col(c) for c in df.columns if c != col_name]
        # Flatten the struct fields
        flattened_fields = flatten_struct(col_name, df.schema[col_name].dataType, col_name)
        # Select all columns together
        df = df.select(*other_columns, *flattened_fields)

        # Drop the original column if specified
        if drop_original:
            df = df.drop(col_name)

    return df

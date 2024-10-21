# Function to get scalar columns (excluding nested columns)
def get_scalar_columns(df):
    return [f.name for f in df.schema.fields if not isinstance(f.dataType, (ArrayType, StructType))]

Function to drop nested columns for scalar processing
    def process_scalar(df):
        scalar_cols = get_scalar_columns(df)
        return df.select(scalar_cols)
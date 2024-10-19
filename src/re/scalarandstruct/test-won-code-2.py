from pyspark.sql.types import StructType
from pyspark.sql.functions import (
    col, coalesce, when, lit, concat, eqNullSafe, concat_ws
)

# Function to identify struct and scalar columns dynamically
def identify_column_types(df):
    struct_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StructType)]
    scalar_cols = [field.name for field in df.schema.fields if not isinstance(field.dataType, StructType)]
    return scalar_cols, struct_cols

# Function to add prefixes for source and target columns
def add_prefix(df, prefix):
    return df.select([col(column).alias(f"{prefix}{column}") for column in df.columns])

# Function to compare scalar columns
# Function to compare scalar columns
def compare_scalars(joined_df, scalar_cols):
    comparisons = []
    for column in scalar_cols:
        source_col = col(f"source_{column}")
        target_col = col(f"target_{column}")
        diff_col = when(
            ~source_col.eqNullSafe(target_col),
            concat(
                lit(f"{column}: Source = "), coalesce(source_col, lit("null")),
                lit(", Target = "), coalesce(target_col, lit("null"))
            )
        ).alias(f"{column}_diff")
        comparisons.append(diff_col)
    return comparisons

# Function to compare struct columns
def compare_structs(joined_df, struct_cols):
    struct_comparisons = []
    for struct_col in struct_cols:
        sub_fields = joined_df.select(f"source_{struct_col}.*").columns
        sub_field_comparisons = []
        for sub_field in sub_fields:
            source_sub_col = col(f"source_{struct_col}.{sub_field}")
            target_sub_col = col(f"target_{struct_col}.{sub_field}")
            diff_expression = when(
                ~source_sub_col.eqNullSafe(target_sub_col),
                concat(
                    lit(f"{struct_col}.{sub_field}: Source = "),
                    coalesce(source_sub_col, lit("null")),
                    lit(", Target = "),
                    coalesce(target_sub_col, lit("null"))
                )
            ).otherwise("")
            sub_field_comparisons.append(diff_expression)
        struct_diff_col = concat_ws(", ", *sub_field_comparisons).alias(f"{struct_col}_diff")
        struct_comparisons.append(struct_diff_col)
    return struct_comparisons


# Main function to perform dataframe comparison
def compare_dataframes(source_df, target_df, primary_keys):
    # Add prefixes to distinguish source and target columns
    source_df_prefixed = add_prefix(source_df, "source_")
    target_df_prefixed = add_prefix(target_df, "target_")

    # Join dataframes on primary keys using eqNullSafe to handle nulls
    join_conditions = [
        eqNullSafe(col(f"source_{pk}"), col(f"target_{pk}")) for pk in primary_keys
    ]
    joined_df = source_df_prefixed.join(target_df_prefixed, on=join_conditions, how="full_outer")

    # Identify scalar and struct columns in source dataframe
    scalar_cols, struct_cols = identify_column_types(source_df)

    # Compare scalar and struct columns
    scalar_comparisons = compare_scalars(joined_df, scalar_cols)
    struct_comparisons = compare_structs(joined_df, struct_cols)

    # Prepare primary key columns for output
    primary_key_cols = [
        coalesce(col(f"source_{pk}"), col(f"target_{pk}")).alias(pk) for pk in primary_keys
    ]

    # Select primary keys and comparison results
    final_df = joined_df.select(*primary_key_cols, *scalar_comparisons, *struct_comparisons)

    return final_df

# Example usage
primary_keys = ['parent_primary_key', 'child_primary_key']
result_df = compare_dataframes(source_df, target_df, primary_keys)

# Display the comparison results
result_df.show(truncate=False)

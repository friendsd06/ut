from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, concat, lit, coalesce, concat_ws
)
from pyspark.sql.types import BooleanType
from functools import reduce

# Function to filter dataframes based on a condition
def filter_dataframes(source_df: DataFrame, target_df: DataFrame, filter_condition):
    filtered_source_df = source_df.filter(filter_condition)
    filtered_target_df = target_df.filter(filter_condition)
    return filtered_source_df, filtered_target_df

# Function to compare struct columns
def compare_structs(joined_df: DataFrame, struct_cols: list):
    struct_comparisons = []
    struct_mismatch_conditions = []

    for struct_col in struct_cols:
        sub_fields = joined_df.select(f"source_{struct_col}.*").columns
        sub_field_comparisons = []
        sub_field_mismatch_conditions = []

        for sub_field in sub_fields:
            source_sub_col = col(f"source_{struct_col}.{sub_field}")
            target_sub_col = col(f"target_{struct_col}.{sub_field}")

            # Define the difference expression
            diff_expression = when(
                ~source_sub_col.eqNullSafe(target_sub_col),
                concat(
                    lit(f"{struct_col}.{sub_field}: Source = "),
                    coalesce(source_sub_col.cast("string"), lit("null")),
                    lit(", Target = "),
                    coalesce(target_sub_col.cast("string"), lit("null"))
                )
            )
            sub_field_comparisons.append(diff_expression)

            # Mismatch condition for this sub-field
            mismatch_condition = ~source_sub_col.eqNullSafe(target_sub_col)
            sub_field_mismatch_conditions.append(mismatch_condition)

        # Concatenate all sub-field differences for the struct column, ignoring nulls
        struct_diff_col = concat_ws(
            "; ", *[diff for diff in sub_field_comparisons if diff is not None]
        ).alias(f"{struct_col}_diff")

        struct_comparisons.append(struct_diff_col)

        # Combine mismatch conditions for sub-fields
        struct_mismatch_condition = reduce(lambda a, b: a | b, sub_field_mismatch_conditions)
        struct_mismatch_conditions.append(struct_mismatch_condition)

    # Overall struct mismatch column
    if struct_mismatch_conditions:
        struct_mismatch_col = reduce(lambda a, b: a | b, struct_mismatch_conditions).alias("struct_mismatch")
    else:
        struct_mismatch_col = lit(False).alias("struct_mismatch")

    return struct_comparisons, struct_mismatch_col

# Function to compare scalar columns
def compare_scalars(joined_df: DataFrame, scalar_cols: list):
    scalar_comparisons = []
    scalar_mismatch_conditions = []

    for scalar_col in scalar_cols:
        source_col = col(f"source_{scalar_col}")
        target_col = col(f"target_{scalar_col}")

        diff_col = when(
            ~source_col.eqNullSafe(target_col),
            concat(
                lit(f"{scalar_col}: Source = "),
                coalesce(source_col.cast("string"), lit("null")),
                lit(", Target = "),
                coalesce(target_col.cast("string"), lit("null"))
            )
        ).alias(f"{scalar_col}_diff")

        scalar_comparisons.append(diff_col)

        # Mismatch condition for this scalar column
        mismatch_condition = ~source_col.eqNullSafe(target_col)
        scalar_mismatch_conditions.append(mismatch_condition)

    # Overall scalar mismatch column
    if scalar_mismatch_conditions:
        scalar_mismatch_col = reduce(lambda a, b: a | b, scalar_mismatch_conditions).alias("scalar_mismatch")
    else:
        scalar_mismatch_col = lit(False).alias("scalar_mismatch")

    return scalar_comparisons, scalar_mismatch_col

# Main function to compare dataframes
def compare_dataframes(
        source_df: DataFrame,
        target_df: DataFrame,
        key_cols: list,
        scalar_cols: list,
        struct_cols: list,
        filter_condition=None,
        filter_mismatches=False
):
    # Step 1: Filter both dataframes if a filter condition is provided
    if filter_condition:
        filtered_source_df, filtered_target_df = filter_dataframes(
            source_df, target_df, filter_condition
        )
    else:
        filtered_source_df = source_df
        filtered_target_df = target_df

    # Step 2: Prefix columns to distinguish source and target
    prefixed_source_df = filtered_source_df.select(
        [col(c).alias(f"source_{c}") for c in filtered_source_df.columns]
    )
    prefixed_target_df = filtered_target_df.select(
        [col(c).alias(f"target_{c}") for c in filtered_target_df.columns]
    )

    # Step 3: Prepare join condition
    join_conditions = [
        prefixed_source_df[f"source_{k}"].eqNullSafe(prefixed_target_df[f"target_{k}"]) for k in key_cols
    ]

    # Step 4: Join the dataframes
    joined_df = prefixed_source_df.join(
        prefixed_target_df,
        on=join_conditions,
        how="full_outer"
    )

    # Step 5: Compare struct columns
    struct_diff_cols, struct_mismatch_col = compare_structs(joined_df, struct_cols)

    # Step 6: Compare scalar columns
    scalar_diff_cols, scalar_mismatch_col = compare_scalars(joined_df, scalar_cols)

    # Step 7: Collect all difference columns
    all_diff_cols = struct_diff_cols + scalar_diff_cols

    # Step 8: Introduce a mismatch column
    mismatch_col = (col("struct_mismatch") | col("scalar_mismatch")).alias("mismatch")

    # Step 9: Select relevant columns
    result_df = joined_df.select(
        [f"source_{k}" for k in key_cols] +  # Key columns
        all_diff_cols +                      # Difference columns
        [struct_mismatch_col, scalar_mismatch_col, mismatch_col]  # Mismatch columns
    )

    # Step 10: Filter mismatches if required
    if filter_mismatches:
        result_df = result_df.filter(col("mismatch") == True)

    return result_df

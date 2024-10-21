# Function to perform reconciliation
def reconcile_main_entity(source_df, target_df, primary_keys):
    # Get the list of scalar and struct columns
    scalar_cols, struct_cols = identify_column_types(source_df)

    # Add prefix to the source and target columns
    source_df_prefixed = add_prefix(source_df, "source")
    target_df_prefixed = add_prefix(target_df, "target")

    # Join on primary keys
    join_conditions = [col(f"source_{pk}").eqNullSafe(col(f"target_{pk}")) for pk in primary_keys]
    joined_df = source_df_prefixed.join(target_df_prefixed, on=join_conditions, how="full_outer")

    # Compare scalar and struct columns
    scalar_comparisons = compare_scalars(joined_df, scalar_cols)
    struct_comparisons = compare_structs(joined_df, struct_cols)

    # Create primary key columns
    primary_key_cols = [coalesce(col(f"source_{pk}"), col(f"target_{pk}")).alias(pk) for pk in primary_keys]

    # Select all columns for final DataFrame
    final_df = joined_df.select(*primary_key_cols, *scalar_comparisons, *struct_comparisons)

    # Clean redundant "null, null" values
    comparison_columns = [c for c in final_df.columns if "_diff" in c]
    final_df = clean_redundant_nulls(final_df, comparison_columns)

    return final_df

# Function to clean redundant nulls in comparison columns
def clean_redundant_nulls(df, columns):
    for column in columns:
        # Replace multiple "null, null" with a single "null"
        df = df.withColumn(column, regexp_replace(col(column), r"(null,\s*)+null", "null"))

    return df
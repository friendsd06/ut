# Cast both fields to string to handle data type mismatches
source_value = col(source_field).cast("string")
target_value = col(target_field).cast("string")


# Correctly chain the when clauses using otherwise
diff_expr = when(
    source_value.isNull() & target_value.isNotNull(),
    concat(lit(f"{field}: Source = null"), lit(", Target = "), target_value)
).otherwise(
    when(
        source_value.isNotNull() & target_value.isNull(),
        concat(lit(f"{field}: Source = "), source_value, lit(", Target = null"))
    ).otherwise(
        when(
            ~source_value.eqNullSafe(target_value),
            concat(
                lit(f"{field}: Source = "), coalesce(source_value, lit("null")),
                lit(", Target = "), coalesce(target_value, lit("null"))
            )
        ).otherwise(None)
    )
)


if source_field in joined_df.columns and target_field in joined_df.columns:
    # Get data types of the source and target columns
    source_dtype = dict(joined_df.dtypes).get(source_field)
    target_dtype = dict(joined_df.dtypes).get(target_field)

    if source_dtype == target_dtype:
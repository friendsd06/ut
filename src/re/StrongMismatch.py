
# Step 8: Introduce a mismatch column
mismatch_expression = reduce(
    lambda acc, c: acc | (c.isNotNull()),
    [col(diff_col.alias) for diff_col in all_diff_cols],
    lit(False)
)
mismatch_col = mismatch_expression.cast(BooleanType()).alias("mismatch")

# Step 9: Select relevant columns
result_df = joined_df.select(
    [f"source_{k}" for k in key_cols] +  # Key columns
    all_diff_cols +                      # Difference columns
    [mismatch_col]                       # Mismatch column
)

# Step 10: Filter mismatches if required
if filter_mismatches:
    result_df = result_df.filter(col("mismatch") == True)
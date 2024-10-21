# Suppose df6 has a column named 'orders_differences' that conflicts with unified_diff_df
# Rename the conflicting column in df6
df6 = df6.withColumnRenamed('orders_differences', 'df6_orders_differences')

# Now perform the join as before
final_df = unified_diff_df.join(
    df6,
    on=global_primary_keys,
    how='full_outer'
)
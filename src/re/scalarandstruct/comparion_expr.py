# Step 2: Prepare the comparison expressions
comparison_exprs = []
for c in columns_to_compare:
    source_col = col(f"source.{c}")
    target_col = col(f"target.{c}")

    comparison = when(
        source_col.eqNullSafe(target_col),
        lit(None)
    ).otherwise(
        struct(
            source_col.alias("source"),
            target_col.alias("target")
        )
    ).alias(c)

    comparison_exprs.append(comparison)


    # Step 4: Apply comparisons and filter in one step
    result_df = joined_df.select(
        col(f"source.{primary_key}").alias(primary_key),
        *comparison_exprs
    ).filter(
        reduce(or_, [col(c).isNotNull() for c in columns_to_compare])
    )
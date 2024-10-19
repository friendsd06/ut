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


    def create_comparison_conditions(columns_to_compare):
        comparison_conditions = []
    for c in columns_to_compare:
        source_col = col(f"source.{c}")
        target_col = col(f"target.{c}")

        condition = (
            # Compare non-null values
            when(~isnull(source_col) & ~isnull(target_col),
                 source_col.cast("string") != target_col.cast("string"))
                # Check if one is null and the other isn't
                .when(isnull(source_col) != isnull(target_col), lit(True))
                # Both null or equal
                .otherwise(lit(False))
        )

        comparison_conditions.append(condition.alias(f"diff_{c}"))

    return comparison_conditions


    # Structure the output
    result_df = filtered_df.select(
        *[col(k) for k in key_columns],
        *[when(col(f"diff_{c}"),
               struct(col(f"source_{c}").alias("source"),
                      col(f"target_{c}").alias("target")))
              .otherwise(lit(None)).alias(c)
          for c in columns_to_compare]
    )
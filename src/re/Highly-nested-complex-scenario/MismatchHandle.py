def safe_cast_to_string(column):
    return when(column.isNull(), lit("NULL")).otherwise(
        when(col(column).cast(StringType()).isNull(), to_json(column))
            .otherwise(col(column).cast(StringType()))
    )

detailed_mismatches = value_mismatches.select(
    "*",
    "mismatch_type",
    array(*[
        when(
            (safe_cast_to_string(col(f"source_{c}")) != safe_cast_to_string(col(f"target_{c}"))) |
            (col(f"source_{c}").isNull() != col(f"target_{c}").isNull()),
            concat(
                lit(f"{c}: source="),
                safe_cast_to_string(col(f"source_{c}")),
                lit(", target="),
                safe_cast_to_string(col(f"target_{c}"))
            )
        ).alias(f"{c}_diff")
        for c in columns_to_compare_final
    ]).alias("diff_array")
)

detailed_mismatches = detailed_mismatches.withColumn(
    "differences",
    concat_ws(", ", *[col(f"{c}_diff") for c in columns_to_compare_final])
).filter(size("diff_array") > 0)

# Select only the required columns
detailed_mismatches = detailed_mismatches.select(
    parent_primary_key,
    "mismatch_type",
    "differences"
)
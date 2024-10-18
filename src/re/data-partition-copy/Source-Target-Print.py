from pyspark.sql import functions as F

corrected_detailed_mismatches = value_mismatches.select(
    parent_primary_key,
    *[
        F.struct(
            F.when(F.col(f"source_{c}").isNull(), "NULL").otherwise(F.col(f"source_{c}").cast("string")).alias("source_value"),
            F.when(F.col(f"target_{c}").isNull(), "NULL").otherwise(F.col(f"target_{c}").cast("string")).alias("target_value")
        ).alias(c)
        for c in columns_to_compare_final
    ]
).filter(
    " OR ".join([f"source_{c} IS NOT NULL AND target_{c} IS NOT NULL AND source_{c} != target_{c}" for c in columns_to_compare_final])
)

corrected_detailed_mismatches.show(truncate=False)

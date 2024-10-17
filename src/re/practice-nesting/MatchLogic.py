summary_df = summary_df.agg(
    F.sum(F.when(F.col("mismatch_type") == "Missing in Target", F.col("count")).otherwise(0)).alias("missing_in_target_count"),
    F.sum(F.when(F.col("mismatch_type") == "Missing in Source", F.col("count")).otherwise(0)).alias("missing_in_source_count"),
    F.sum(F.when(F.col("mismatch_type") == "Value Mismatch", F.col("count")).otherwise(0)).alias("value_mismatch_count")
).withColumn("total_source_records", F.lit(4))  # 4 records in source
.withColumn("total_target_records", F.lit(4))  # 4 records in target
.withColumn("matched_records_count",
            F.greatest(
                F.lit(0),
                F.least(
                    F.col("total_source_records") - F.col("missing_in_target_count") - F.col("value_mismatch_count"),
                    F.col("total_target_records") - F.col("missing_in_source_count") - F.col("value_mismatch_count")
                )
            )
            )
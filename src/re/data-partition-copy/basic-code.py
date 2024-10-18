parent_mismatches_df = parent_mismatches_df.withColumn('child_primary_key', lit(None).cast("string"))

parent_mismatches_df = parent_mismatches_df.withColumnRenamed('primary_key', 'parent_primary_key')
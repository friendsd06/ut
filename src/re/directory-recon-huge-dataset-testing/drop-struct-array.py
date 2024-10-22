non_struct_array_cols = [field.name for field in df.schema.fields
                         if not isinstance(field.dataType, (StructType, ArrayType))]

df_new = df.select(*non_struct_array_cols)
df_new.printSchema()



if nested_mismatch_col is not None:
    difference_results[array_col] = difference_results[array_col].withColumn(
        "nested_mismatch_col", nested_mismatch_col
    )


    # === Handle the nested_mismatch_col if it exists in the mismatch dictionary ===
    if array_col in mismatch:
        nested_mismatch_col = mismatch[array_col]
        # Add the nested_mismatch_col to the difference DataFrame
        diff_df = diff_df.withColumn("nested_mismatch_col", nested_mismatch_col)
non_struct_array_cols = [field.name for field in df.schema.fields
                         if not isinstance(field.dataType, (StructType, ArrayType))]

df_new = df.select(*non_struct_array_cols)
df_new.printSchema()



if nested_mismatch_col is not None:
    difference_results[array_col] = difference_results[array_col].withColumn(
        "nested_mismatch_col", nested_mismatch_col
    )
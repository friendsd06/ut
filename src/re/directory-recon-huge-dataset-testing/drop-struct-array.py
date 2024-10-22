non_struct_array_cols = [field.name for field in df.schema.fields
                         if not isinstance(field.dataType, (StructType, ArrayType))]

df_new = df.select(*non_struct_array_cols)
df_new.printSchema()
# Identify struct columns without prefix "source_"
struct_columns = [
    field.name.replace('source_', '')
    for field in schema.fields
    if isinstance(field.dataType, StructType)
]


# Filter out primary keys in a separate pass
filtered_struct_columns = [col for col in struct_columns if col not in primary_keys]

if field.name.replace('source_', '') not in struct_columns
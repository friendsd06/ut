# Identify struct columns without prefix "source_"
struct_columns = [
    field.name.replace('source_', '')
    for field in schema.fields
    if isinstance(field.dataType, StructType)
]
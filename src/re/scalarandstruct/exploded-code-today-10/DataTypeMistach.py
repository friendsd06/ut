# Cast both fields to string to handle data type mismatches
source_value = col(source_field).cast("string")
target_value = col(target_field).cast("string")
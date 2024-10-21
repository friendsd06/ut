# Filter out rows where all comparison columns are null
comparison_columns = [col for col in final_df.columns if col not in primary_keys]
non_null_condition = reduce(lambda x, y: x | y, [col(c).isNotNull() for c in comparison_columns])
final_df = final_df.filter(non_null_condition)
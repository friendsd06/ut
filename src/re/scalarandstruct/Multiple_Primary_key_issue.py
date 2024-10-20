def join_exploded_dfs(source_dfs, target_dfs, array_cols, primary_keys):
    from functools import reduce
    from pyspark.sql.functions import col

    joined_dfs = {}
    for array_column in array_cols:
        source_df = source_dfs[array_column]
        target_df = target_dfs[array_column]

        # Rename primary keys in target_df to avoid ambiguity
        for key in primary_keys:
            target_df = target_df.withColumnRenamed(key, f"{key}_target")

        # Identify unique identifier columns (fields ending with '_id')
        struct_fields = [col_name for col_name in source_df.columns if col_name not in primary_keys]
        id_fields = [col_name for col_name in struct_fields if col_name.endswith('_id')]

        # Prepare source and target keys for join
        source_keys = primary_keys + id_fields
        target_keys = [f"{key}_target" if key in primary_keys else key for key in source_keys]

        # Build join conditions
        join_conditions = [col(s_key).eqNullSafe(col(t_key)) for s_key, t_key in zip(source_keys, target_keys)]
        join_condition = reduce(lambda x, y: x & y, join_conditions)

        # Perform the join
        joined_df = source_df.join(target_df, on=join_condition, how="full_outer")
        joined_dfs[array_column] = joined_df

    return joined_dfs

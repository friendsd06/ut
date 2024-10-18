from pyspark.sql.functions import col, coalesce, lit, concat

def compare_df(df1, df2, natural_key, return_all_rows=None):
    # Convert natural key columns to uppercase
    natural_key_upper = [key.upper() for key in natural_key]

    # Identify measure columns (columns not in natural key)
    measure_columns = [col_name for col_name in df1.columns if col_name.upper() not in natural_key_upper]

    # Join df1 and df2 on the natural key columns
    comparison = df1.alias('df1').join(df2.alias('df2'), on=natural_key_upper, how='left')

    # Create boolean columns to compare measure columns
    boolean_columns = []
    for col_name in measure_columns:
        bool_col = coalesce(
            col(f"df1.{col_name}") == col(f"df2.{col_name}"),
            lit(False)
        ).alias(f"boolean_{col_name}")
        boolean_columns.append(bool_col)

    # Select necessary columns
    select_columns = [
        *[col(f"df1.{key}").alias(f"KEY_{key}") for key in natural_key_upper],
        *boolean_columns,
        *[col(f"df1.{col_name}").alias(f"df1_{col_name}") for col_name in measure_columns],
        *[col(f"df2.{col_name}").alias(f"df2_{col_name}") for col_name in measure_columns]
    ]

    result_df = comparison.select(*select_columns)

    # Apply filters based on return_all_rows parameter
    if return_all_rows == True:
        # Keep rows where all measure columns match
        filter_condition = ' AND '.join([f"boolean_{col_name} = true" for col_name in measure_columns])
        result_df = result_df.filter(filter_condition)
        print("Returning only matched rows")
    elif return_all_rows == False:
        # Keep rows where any measure column does not match
        filter_condition = ' OR '.join([f"boolean_{col_name} = false" for col_name in measure_columns])
        result_df = result_df.filter(filter_condition)
        print("Returning only mismatched rows")
    else:
        # Keep all rows
        print("Returning all rows")

    # Create comparison columns showing df1 and df2 values
    comparison_columns = []
    for idx, col_name in enumerate(measure_columns, start=1):
        comp_col = concat(
            coalesce(col(f"df1_{col_name}").cast('string'), lit('null')),
            lit("="),
            coalesce(col(f"df2_{col_name}").cast('string'), lit('null'))
        ).alias(f"{idx:02d}_{col_name}")
        match_col = col(f"boolean_{col_name}").alias(f"df1_{idx:02d}_{col_name}_matches_df2_{col_name}?")
        comparison_columns.extend([comp_col, match_col])

    # Final selection of columns
    final_columns = [
        *[col(f"KEY_{key}") for key in natural_key_upper],
        *comparison_columns
    ]

    result_df = result_df.select(*final_columns)

    return result_df

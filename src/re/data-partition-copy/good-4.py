from pyspark.sql.functions import col, coalesce, lit, concat

def compare_df(df1, df2, natural_key, return_all_rows=None):
    # Convert all natural key columns to uppercase
    natural_key = [col_name.upper() for col_name in natural_key]

    # Identify the measure columns (non-key columns)
    measure_columns = [col_name for col_name in df1.columns if col_name not in natural_key]

    # Join df1 and df2 on natural key columns
    comparison = df1.alias('df1').join(df2.alias('df2'), on=natural_key, how='left')

    # Handle the case where only matched rows should be returned
    if return_all_rows == True:
        result_df = comparison.select(
            *[col(f"df1.{col_name}").alias(f"KEY_{col_name}") for col_name in natural_key],
            *[
                (coalesce(
                    (col(f"df1.{col_name}") == col(f"df2.{col_name}")) |
                    (col(f"df1.{col_name}").isNull() & col(f"df2.{col_name}").isNotNull()),
                    lit(False))
                ).alias(f"boolean_{col_name}")
                for col_name in measure_columns
            ],
            *[col(f"df1.{col_name}") for col_name in measure_columns],
            *[col(f"df2.{col_name}") for col_name in measure_columns]
        )

        # Filter matched rows
        result_df = result_df.filter(
            ' and '.join([f'boolean_{col_name} = true' for col_name in measure_columns])
        )

        print("Returning only matched rows")
        return result_df

    # Handle the case where only mismatched rows should be returned
    elif return_all_rows == False:
        result_df = comparison.select(
            *[col(f"df1.{col_name}").alias(f"KEY_{col_name}") for col_name in natural_key],
            *[
                (coalesce(
                    (col(f"df1.{col_name}") != col(f"df2.{col_name}")) |
                    (col(f"df1.{col_name}").isNotNull() & col(f"df2.{col_name}").isNull()),
                    lit(False))
                ).alias(f"boolean_{col_name}")
                for col_name in measure_columns
            ],
            *[col(f"df1.{col_name}") for col_name in measure_columns],
            *[col(f"df2.{col_name}") for col_name in measure_columns]
        )

        # Filter mismatched rows
        result_df = result_df.filter(
            ' or '.join([f'boolean_{col_name} = false' for col_name in measure_columns])
        )

        print("Returning only mismatched rows")
        return result_df

    # Handle the case where all rows should be returned
    else:
        result_df = comparison.select(
            *[col(f"df1.{col_name}").alias(f"KEY_{col_name}") for col_name in natural_key],
            *[
                (coalesce(
                    (col(f"df1.{col_name}") == col(f"df2.{col_name}")) |
                    (col(f"df1.{col_name}").isNull() & col(f"df2.{col_name}").isNotNull()),
                    lit(False))
                ).alias(f"boolean_{col_name}")
                for col_name in measure_columns
            ],
            *[col(f"df1.{col_name}") for col_name in measure_columns],
            *[col(f"df2.{col_name}") for col_name in measure_columns]
        )

        print("Returning all rows")
        return result_df
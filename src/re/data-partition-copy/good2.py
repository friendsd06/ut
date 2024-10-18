from pyspark.sql.functions import *
import inspect

def compare_df(df1,df2,natural_key,retrun_all_rows=None):
    natural_key = [ele.upper() for ele in natural_key]
    measure_columns = [col_name for col_name in df1.columns if col_name not in natural_key]
    comparison = df1.alias('df1').join(df2.alias('df2'), on=natural_key,how='left')

    params = inspect.signature(compare_df).parameters
    param_names = [p for p in params]
    print(param_names)

    if retrun_all_rows == True:
        allmatches = comparison.select(
            *[ col(f"df1.{col_name}").alias(f"KEY_{col_name}") for col_name in natural_key],
            *[
                (
                    coalesce(
                        (col(f"df1.{col_name}") == col(f"df2.{col_name}")) |
                        (col(f"df1.{col_name}").isNull() & col(f"df2.{col_name}").isNotNull())
                        ,lit(False))
                ).alias(f"boolean_{col_name}")
                for col_name in measure_columns
            ],
            *[ col(f"df1.{col_name}") for col_name in measure_columns],
            *[ col(f"df2.{col_name}") for col_name in measure_columns]
        )

        boolean_columns = ["boolean_"+col_name for col_name in measure_columns]
        condition = ' and '.join([f'{col} = true' for col in boolean_columns])
        allmatches = allmatches.filter(condition)
        filtered_columns = [ele[8:] for ele in boolean_columns ]
        l1 = [ (concat(
            coalesce(col(f"df1.{col_name}").cast('string'),lit('null')),
            lit("="),
            coalesce(col(f"df2.{col_name}").cast('string'),lit('null'))
        ).alias(f"0{index+1}_{col_name}")) for index,col_name in enumerate(filtered_columns)]
        l2 = [ col(f"boolean_{col_name}").alias(f"df1_0{index+1}_{col_name}_match_with_df2_{col_name}?") for index,col_name in enumerate(filtered_columns)]
        result = []
        for a, b in zip(l1, l2):
            result.extend([a, b])

        allmatches = allmatches.select(*[ col(f"KEY_{col_name}").alias(f"KEY_{col_name}") for col_name in natural_key]  ,  *result)
        print("Returning Only Matched Rows")
        return allmatches
    elif retrun_all_rows == False:
        mismatches = comparison.select(
            *[ col(f"df1.{col_name}").alias(f"KEY_{col_name}") for col_name in natural_key],
            *[
                (
                    coalesce(
                        (col(f"df1.{col_name}") == col(f"df2.{col_name}")) |
                        (col(f"df1.{col_name}").isNotNull() & col(f"df2.{col_name}").isNull())
                        ,lit(False))
                ).alias(f"boolean_{col_name}")
                for col_name in measure_columns
            ],
            *[ col(f"df1.{col_name}") for col_name in measure_columns],
            *[ col(f"df2.{col_name}") for col_name in measure_columns]
        )

        boolean_columns = ["boolean_"+col_name for col_name in measure_columns]
        condition = ' or '.join([f'{col} = false' for col in boolean_columns])
        mismatches = mismatches.filter(condition)
        filtered_columns = [ele[8:] for ele in boolean_columns ]
        l1 = [ (concat(
            coalesce(col(f"df1.{col_name}").cast('string'),lit('null')),
            lit("="),
            coalesce(col(f"df2.{col_name}").cast('string'),lit('null'))
        ).alias(f"0{index+1}_{col_name}")) for index,col_name in enumerate(filtered_columns)]
        l2 = [ col(f"boolean_{col_name}").alias(f"df1_0{index+1}_{col_name}_match_with_df2_{col_name}?") for index,col_name in enumerate(filtered_columns)]
        result = []
        for a, b in zip(l1, l2):
            result.extend([a, b])

        mismatches = mismatches.select(*[ col(f"KEY_{col_name}").alias(f"KEY_{col_name}") for col_name in natural_key]  ,  *result)
        print("Returning Only Mismatched Rows")
        return mismatches

    elif retrun_all_rows == None:
        allmatches = comparison.select(
            *[ col(f"df1.{col_name}").alias(f"KEY_{col_name}") for col_name in natural_key],
            *[
                (
                    coalesce(
                        (col(f"df1.{col_name}").isNull() & col(f"df2.{col_name}").isNotNull()) |
                        (col(f"df1.{col_name}") == col(f"df2.{col_name}"))
                        ,lit(False))
                ).alias(f"boolean_{col_name}")
                for col_name in measure_columns
            ],
            *[ col(f"df1.{col_name}") for col_name in measure_columns],
            *[ col(f"df2.{col_name}") for col_name in measure_columns]
        )
        #display(allmatches)
        boolean_columns = ["boolean_"+col_name for col_name in measure_columns]

        filtered_columns = [ele[8:] for ele in boolean_columns ]
        l1 = [ (concat(
            coalesce(col(f"df1.{col_name}").cast('string'),lit('null')),
            lit("="),
            coalesce(col(f"df2.{col_name}").cast('string'),lit('null'))
        ).alias(f"0{index+1}_{col_name}")) for index,col_name in enumerate(filtered_columns)]
        l2 = [ col(f"boolean_{col_name}").alias(f"df1_0{index+1}_{col_name}_match_with_df2_{col_name}?") for index,col_name in enumerate(filtered_columns)]
        result = []
        for a, b in zip(l1, l2):
            result.extend([a, b])

        allmatches = allmatches.select(*[ col(f"KEY_{col_name}").alias(f"KEY_{col_name}") for col_name in natural_key]  ,  *result)
        print("Returning All Rows")
        return allmatches
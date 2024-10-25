from pyspark.sql.functions import *
from pyspark.sql.types import StructType, ArrayType
import inspect

def flatten_df(df, parent_keys):
    # List to hold the selected columns
    cols = []
    for field in df.schema.fields:
        field_name = field.name
        field_type = field.dataType
        if field_name in parent_keys:
            # Include parent key fields as they are
            cols.append(col(field_name))
        elif isinstance(field_type, StructType):
            # If the field is a struct, flatten its immediate fields
            for nested_field in field_type.fields:
                nested_name = nested_field.name
                nested_type = nested_field.dataType
                full_name = f"{field_name}_{nested_name}"
                if nested_name in parent_keys:
                    # If nested field is a foreign key, include it
                    cols.append(col(f"{field_name}.{nested_name}").alias(full_name))
                elif isinstance(nested_type, (StructType, ArrayType)):
                    # If nested field is a struct or array, we drop it
                    continue
                else:
                    # Include the field
                    cols.append(col(f"{field_name}.{nested_name}").alias(full_name))
        elif isinstance(field_type, ArrayType):
            # If it's an array, check if it's an array of structs
            element_type = field_type.elementType
            if isinstance(element_type, StructType):
                # Array of structs, we drop it
                continue
            else:
                # Include the array as is
                cols.append(col(field_name))
        else:
            # Include the field as is
            cols.append(col(field_name))
    # Return the DataFrame with selected columns
    return df.select(*cols)

def compare_df(df1, df2, natural_key, parent_keys, retrun_all_rows=None):
    # Combine parent keys to create a combined key
    combined_key_col = 'combined_key'
    df1 = df1.withColumn(combined_key_col, concat_ws('_', *[col(k) for k in parent_keys]))
    df2 = df2.withColumn(combined_key_col, concat_ws('_', *[col(k) for k in parent_keys]))

    # Flatten the DataFrames
    df1 = flatten_df(df1, parent_keys)
    df2 = flatten_df(df2, parent_keys)

    # Adjust natural_key to match the flattened column names
    natural_key = [col_name.replace('.', '_') for col_name in natural_key]
    natural_key_upper = [ele.upper() for ele in natural_key]

    # Get the measure_columns
    measure_columns = [col_name for col_name in df1.columns if col_name.upper() not in natural_key_upper + [combined_key_col.upper()]]

    # Adjust the natural key to include the combined key
    join_keys = [combined_key_col] + [k for k in natural_key if k != combined_key_col]

    # Perform the join
    comparison = df1.alias('df1').join(df2.alias('df2'), on=join_keys, how='left')

    params = inspect.signature(compare_df).parameters
    param_names = [p for p in params]
    print(param_names)

    if retrun_all_rows == True:
        allmatches = comparison.select(
            *[col(f"df1.{col_name}").alias(f"KEY_{col_name}") for col_name in join_keys],
            *[
                (
                    coalesce(
                        (col(f"df1.{col_name}") == col(f"df2.{col_name}")) |
                        (col(f"df1.{col_name}").isNull() & col(f"df2.{col_name}").isNotNull()),
                        lit(False)
                    )
                ).alias(f"boolean_{col_name}")
                for col_name in measure_columns
            ],
            *[col(f"df1.{col_name}") for col_name in measure_columns],
            *[col(f"df2.{col_name}") for col_name in measure_columns]
        )

        boolean_columns = ["boolean_" + col_name for col_name in measure_columns]
        condition = ' and '.join([f'{col} = true' for col in boolean_columns])
        allmatches = allmatches.filter(condition)
        filtered_columns = [ele[len('boolean_'):] for ele in boolean_columns]
        l1 = [
            (
                concat(
                    coalesce(col(f"df1.{col_name}").cast('string'), lit('null')),
                    lit("="),
                    coalesce(col(f"df2.{col_name}").cast('string'), lit('null'))
                ).alias(f"0{index+1}_{col_name}")
            )
            for index, col_name in enumerate(filtered_columns)
        ]
        l2 = [
            col(f"boolean_{col_name}").alias(f"df1_0{index+1}_{col_name}_match_with_df2_{col_name}?")
            for index, col_name in enumerate(filtered_columns)
        ]
        result = []
        for a, b in zip(l1, l2):
            result.extend([a, b])

        allmatches = allmatches.select(
            *[col(f"KEY_{col_name}").alias(f"KEY_{col_name}") for col_name in join_keys],
            *result
        )
        print("Returning Only Matched Rows")
        return allmatches

    elif retrun_all_rows == False:
        mismatches = comparison.select(
            *[col(f"df1.{col_name}").alias(f"KEY_{col_name}") for col_name in join_keys],
            *[
                (
                    coalesce(
                        (col(f"df1.{col_name}") == col(f"df2.{col_name}")) |
                        (col(f"df1.{col_name}").isNotNull() & col(f"df2.{col_name}").isNull()),
                        lit(False)
                    )
                ).alias(f"boolean_{col_name}")
                for col_name in measure_columns
            ],
            *[col(f"df1.{col_name}") for col_name in measure_columns],
            *[col(f"df2.{col_name}") for col_name in measure_columns]
        )

        boolean_columns = ["boolean_" + col_name for col_name in measure_columns]
        condition = ' or '.join([f'{col} = false' for col in boolean_columns])
        mismatches = mismatches.filter(condition)
        filtered_columns = [ele[len('boolean_'):] for ele in boolean_columns]
        l1 = [
            (
                concat(
                    coalesce(col(f"df1.{col_name}").cast('string'), lit('null')),
                    lit("="),
                    coalesce(col(f"df2.{col_name}").cast('string'), lit('null'))
                ).alias(f"0{index+1}_{col_name}")
            )
            for index, col_name in enumerate(filtered_columns)
        ]
        l2 = [
            col(f"boolean_{col_name}").alias(f"df1_0{index+1}_{col_name}_match_with_df2_{col_name}?")
            for index, col_name in enumerate(filtered_columns)
        ]
        result = []
        for a, b in zip(l1, l2):
            result.extend([a, b])

        mismatches = mismatches.select(
            *[col(f"KEY_{col_name}").alias(f"KEY_{col_name}") for col_name in join_keys],
            *result
        )
        print("Returning Only Mismatched Rows")
        return mismatches

    elif retrun_all_rows == None:
        allmatches = comparison.select(
            *[col(f"df1.{col_name}").alias(f"KEY_{col_name}") for col_name in join_keys],
            *[
                (
                    coalesce(
                        (col(f"df1.{col_name}").isNull() & col(f"df2.{col_name}").isNotNull()) |
                        (col(f"df1.{col_name}") == col(f"df2.{col_name}")),
                        lit(False)
                    )
                ).alias(f"boolean_{col_name}")
                for col_name in measure_columns
            ],
            *[col(f"df1.{col_name}") for col_name in measure_columns],
            *[col(f"df2.{col_name}") for col_name in measure_columns]
        )

        boolean_columns = ["boolean_" + col_name for col_name in measure_columns]
        filtered_columns = [ele[len('boolean_'):] for ele in boolean_columns]
        l1 = [
            (
                concat(
                    coalesce(col(f"df1.{col_name}").cast('string'), lit('null')),
                    lit("="),
                    coalesce(col(f"df2.{col_name}").cast('string'), lit('null'))
                ).alias(f"0{index+1}_{col_name}")
            )
            for index, col_name in enumerate(filtered_columns)
        ]
        l2 = [
            col(f"boolean_{col_name}").alias(f"df1_0{index+1}_{col_name}_match_with_df2_{col_name}?")
            for index, col_name in enumerate(filtered_columns)
        ]
        result = []
        for a, b in zip(l1, l2):
            result.extend([a, b])

        allmatches = allmatches.select(
            *[col(f"KEY_{col_name}").alias(f"KEY_{col_name}") for col_name in join_keys],
            *result
        )
        print("Returning All Rows")
        return allmatches

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce

def dynamic_full_outer_join(source_df: DataFrame, target_df: DataFrame, join_columns: list, columns_to_compare: list) -> DataFrame:
    # Step 1: Create the join condition dynamically based on the list of join columns
    join_condition = [source_df[col] == target_df[col] for col in join_columns]

    # Step 2: Perform full outer join between source and target DataFrames
    joined_df = source_df.join(target_df, join_condition, how="full_outer")

    # Step 3: Create coalesced primary key column
    coalesced_columns = [
        coalesce(source_df[col], target_df[col]).alias(col) for col in join_columns
    ]

    # Step 4: Select columns from source and target with aliases
    source_columns = [source_df[f"source_{c}"].alias(f"source_{c}") for c in columns_to_compare]
    target_columns = [target_df[f"target_{c}"].alias(f"target_{c}") for c in columns_to_compare]

    # Step 5: Combine coalesced primary keys and comparison columns
    selected_columns = coalesced_columns + source_columns + target_columns

    # Step 6: Select and return the result DataFrame
    result_df = joined_df.select(*selected_columns)
    return result_df

# Example usage
# source_df: Source DataFrame
# target_df: Target DataFrame
# join_columns: List of primary key columns for joining (e.g., ['id', 'date'])
# columns_to_compare: List of columns to compare (e.g., ['name', 'age', 'salary'])
# result_df = dynamic_full_outer_join(source_df, target_df, join_columns, columns_to_compare)
# result_df.show()

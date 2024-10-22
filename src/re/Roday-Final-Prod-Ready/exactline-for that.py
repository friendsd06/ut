def compare_and_combine_differences(
        joined_df, compare_fields, source_prefix, target_prefix,
        global_primary_keys, array_pks, result_col_name
):
    # If compare_fields is None or empty, compare all fields except primary and foreign keys
    if not compare_fields:
        # Get all columns from joined_df
        all_columns = joined_df.columns

        # Exclude primary keys and foreign keys
        exclude_fields = global_primary_keys + array_pks + [f"fk_{pk}" for pk in global_primary_keys]
        exclude_fields += [f"{source_prefix}{pk}" for pk in array_pks]
        exclude_fields += [f"{target_prefix}{pk}" for pk in array_pks]

        # Fields to compare
        compare_fields = []
        for col_name in all_columns:
            if col_name.startswith(source_prefix):
                field = col_name[len(source_prefix):]
                source_field = f"{source_prefix}{field}"
                target_field = f"{target_prefix}{field}"
                if (
                        source_field in all_columns and
                        target_field in all_columns and
                        field not in exclude_fields and
                        source_field not in exclude_fields and
                        target_field not in exclude_fields
                ):
                    compare_fields.append(field)

    difference_expressions = []
    for field in compare_fields:
        source_field = f"{source_prefix}{field}"
        target_field = f"{target_prefix}{field}"

        # Build the difference expression
        diff_expr = when(
            col(source_field).isNull() & col(target_field).isNotNull(),
            concat(lit(f"{field}: Source = null, Target = "), col(target_field).cast("string"))
        ).when(
            col(source_field).isNotNull() & col(target_field).isNull(),
            concat(lit(f"{field}: Source = "), col(source_field).cast("string"), lit(", Target = null"))
        ).when(
            ~col(source_field).eqNullSafe(col(target_field)),
            concat(
                lit(f"{field}: Source = "), coalesce(col(source_field).cast("string"), lit("null")),
                lit(", Target = "), coalesce(col(target_field).cast("string"), lit("null"))
            )
        )
        difference_expressions.append(diff_expr)

    if difference_expressions:
        # === Changes Start Here ===

        # Lines 43-61: Added code to include array primary keys in the differences column
        # Include array primary keys in the difference description
        array_key_expressions = []
        for pk in array_pks:
            source_pk = col(pk).cast("string")
            target_pk = col(f"{target_prefix}{pk}").cast("string")
            pk_expr = when(
                source_pk.eqNullSafe(target_pk),
                concat(lit(f"{pk}: "), coalesce(source_pk, lit("null")))
            ).otherwise(
                concat(
                    lit(f"{pk}: Source = "), coalesce(source_pk, lit("null")),
                    lit(", Target = "), coalesce(target_pk, lit("null"))
                )
            )
            array_key_expressions.append(pk_expr)

        # Combine array keys and field differences into a single expression
        all_differences = array_key_expressions + difference_expressions
        combined_differences = concat_ws("; ", array(*all_differences))

        # Select parent primary keys and combined differences
        selected_columns = (
                [col(pk) for pk in global_primary_keys] +
                [combined_differences.alias(result_col_name)]
        )
        # === Changes End Here ===

        result_df = joined_df.select(*selected_columns).filter(col(result_col_name).isNotNull())
    else:
        # If no differences found, return an empty DataFrame with the required columns
        result_schema = StructType([
                                       StructField(pk, StringType(), True) for pk in global_primary_keys
                                   ] + [StructField(result_col_name, StringType(), True)])
        result_df = spark.createDataFrame([], result_schema)

    return result_df

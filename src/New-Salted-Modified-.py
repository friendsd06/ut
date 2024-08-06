
def salt_dataframe(df, key_column, num_salts):
    return df.withColumn("salt", (rand() * num_salts).cast("int")) \
        .withColumn("salted_key", concat(col(key_column), lit("_"), col("salt").cast("string")))

# Apply salting
num_salts = 10
salted_transactions = salt_dataframe(transactions, "customer_id", num_salts)
salted_customer_data = salt_dataframe(customer_data, "customer_id", num_salts)

print("\nPerforming join with additional operations...")
result = (salted_transactions.join(salted_customer_data, "salted_key")
          .groupBy("customer_id", "category")
          .agg(sum("value").alias("sum_value")))

# Join result
print("Join result distribution:")
join_distribution = result.groupBy("category").agg(sum("sum_value").alias("total_value"))
join_distribution.show()
from common_utils import create_spark_session, generate_loan_data
from delta.tables import DeltaTable
from pyspark.sql.functions import col
import time

def generate_update_data(spark, path, num_updates):
    existing_data = spark.read.format("delta").load(path)
    max_id = existing_data.agg({"loan_id": "max"}).collect()[0][0]
    max_id = int(max_id.split("-")[1])

    update_data = generate_loan_data(spark, num_updates, start_id=max_id-num_updates+1)
    return update_data.withColumn("loan_amount", col("loan_amount") * 1.1)

def perform_merge(spark, target_path, update_data):
    start_time = time.time()

    delta_table = DeltaTable.forPath(spark, target_path)
    delta_table.alias("target").merge(
        update_data.alias("updates"),
        "target.loan_id = updates.loan_id"
    ).whenMatchedUpdate(set={
        "loan_amount": "updates.loan_amount",
        "loan_status": "updates.loan_status",
        "remaining_balance": "updates.remaining_balance"
    }).whenNotMatchedInsert(values={
        col("updates." + c): col("updates." + c) for c in update_data.columns
    }).execute()

    end_time = time.time()
    return end_time - start_time

def analyze_table(spark, path):
    metrics = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
    partition_stats = spark.sql(f"""
        SELECT cob_date, slice, COUNT(*) as record_count, COUNT(DISTINCT input_file_name()) as file_count
        FROM delta.`{path}`
        GROUP BY cob_date, slice
        ORDER BY cob_date, slice
    """)
    return metrics, partition_stats

def run_merge_scenario(spark, target_path, update_size):
    print(f"\nRunning merge scenario with {update_size} updates")
    update_data = generate_update_data(spark, target_path, update_size)

    print("Analyzing table before merge...")
    before_metrics, before_stats = analyze_table(spark, target_path)

    print("Performing merge operation...")
    merge_time = perform_merge(spark, target_path, update_data)
    print(f"Merge completed in {merge_time:.2f} seconds")

    print("Analyzing table after merge...")
    after_metrics, after_stats = analyze_table(spark, target_path)

    print("\nMerge Performance Statistics:")
    print(f"Update Size: {update_size}")
    print(f"Merge Time: {merge_time:.2f} seconds")
    print(f"Files Before: {before_metrics['numFiles']}, After: {after_metrics['numFiles']}")
    print(f"Size Before: {before_metrics['sizeInBytes']}, After: {after_metrics['sizeInBytes']}")

    return merge_time, before_metrics, after_metrics

def main():
    spark = create_spark_session()
    base_path = "/path/to/your/delta/table"
    target_path = f"{base_path}/loan_data"

    update_sizes = {
        "small": 1_000,
        "medium": 100_000,
        "large": 1_000_000,
        "extra_large": 5_000_000
    }

    results = {}
    for size_name, size in update_sizes.items():
        results[size_name] = run_merge_scenario(spark, target_path, size)

    print("\nSummary of Merge Scenarios:")
    for size_name, (merge_time, before, after) in results.items():
        print(f"{size_name.capitalize()} Update:")
        print(f"  Merge Time: {merge_time:.2f} seconds")
        print(f"  Files: {before['numFiles']} -> {after['numFiles']}")
        print(f"  Size: {before['sizeInBytes']} -> {after['sizeInBytes']}")

    spark.stop()

if __name__ == "__main__":
    main()
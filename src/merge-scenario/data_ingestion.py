from common_utils import create_spark_session, generate_loan_data
import time

def write_data_to_delta(spark, data, path):
    start_time = time.time()
    data.write.format("delta") \
        .partitionBy("cob_date", "slice") \
        .mode("overwrite") \
        .option("maxRecordsPerFile", "0") \
        .save(path)
    end_time = time.time()
    print(f"Data writing completed in {end_time - start_time:.2f} seconds")

def main():
    spark = create_spark_session()
    base_path = "/path/to/your/delta/table"
    target_path = f"{base_path}/loan_data"

    print("Generating 150 million records...")
    initial_data = generate_loan_data(spark, 150_000_000)
    print("Writing initial data to Delta table...")
    write_data_to_delta(spark, initial_data, target_path)

    print("Data ingestion completed.")
    spark.stop()

if __name__ == "__main__":
    main()
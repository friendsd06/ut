# synthetic_data_generator.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType, DateType, TimestampType
import dbldatagen as dg
import sys

class SyntheticDataGenerator:
    def __init__(self, spark, input_data=None, input_schema=None, config=None, rows=10000):
        """
        Initialize the SyntheticDataGenerator.

        Parameters:
        - spark: SparkSession object
        - input_data: Spark DataFrame containing the input data
        - input_schema: StructType schema if input_data is not provided
        - config: Dictionary containing column-specific configurations
        - rows: Number of rows to generate
        """
        self.spark = spark
        self.input_data = input_data
        self.input_schema = input_schema if input_schema else input_data.schema if input_data else None
        self.config = config if config else {}
        self.rows = rows
        self.string_columns = []
        self.data_generator = None

        if not self.input_schema:
            raise ValueError("Either input_data or input_schema must be provided.")

        self._identify_string_columns()
        self._initialize_data_generator()

    def _identify_string_columns(self):
        """Identify string columns in the schema."""
        self.string_columns = [field.name for field in self.input_schema.fields if isinstance(field.dataType, StringType)]

    def _initialize_data_generator(self):
        """Initialize the DataGenerator with the input schema."""
        self.data_generator = dg.DataGenerator(
            sparkSession=self.spark,
            name="synthetic_data",
            rows=self.rows,
            schema=self.input_schema
        )

    def _configure_string_columns(self):
        """Configure data generation for string columns."""
        if not self.input_data:
            # If no input data is provided, generate random string values
            for col_name in self.string_columns:
                col_config = self.config.get(col_name, {})
                self.data_generator = self.data_generator.withColumnSpec(
                    col_name,
                    **col_config.get('generation_params', {'prefix': f"{col_name}_", 'random': True})
                )
            return

        for col_name in self.string_columns:
            col_config = self.config.get(col_name, {})
            # Use actual values from input data
            value_counts = self.input_data.groupBy(col_name).count()
            total_count = self.input_data.count()

            value_prob_df = value_counts.withColumn('probability', col('count') / total_count)
            value_prob_list = value_prob_df.select(col_name, 'probability').collect()

            values_list = []
            weights_list = []
            for row in value_prob_list:
                value = row[col_name]
                probability = row['probability']
                if value is not None:
                    values_list.append(value)
                    weights_list.append(probability)

            if values_list:
                self.data_generator = self.data_generator.withColumnSpec(
                    col_name,
                    values=values_list,
                    weights=weights_list,
                    random=True,
                    **col_config.get('generation_params', {})
                )
            else:
                self.data_generator = self.data_generator.withColumnSpec(
                    col_name,
                    prefix=f"{col_name}_",
                    random=True,
                    **col_config.get('generation_params', {})
                )

    def _configure_non_string_columns(self):
        """Configure data generation for non-string columns."""
        for field in self.input_schema.fields:
            col_name = field.name
            data_type = field.dataType
            if col_name in self.string_columns:
                continue

            col_config = self.config.get(col_name, {})
            generation_params = col_config.get('generation_params', {'random': True})

            if isinstance(data_type, (IntegerType, LongType)):
                default_params = {'minValue': 1, 'maxValue': 1000}
            elif isinstance(data_type, (DoubleType, FloatType)):
                default_params = {'minValue': 0.0, 'maxValue': 100.0}
            elif isinstance(data_type, (DateType, TimestampType)):
                default_params = {'begin': "2015-01-01", 'end': "2021-12-31"}
            else:
                # For other data types, you can define default behavior or skip
                continue

            # Merge default_params with any user-provided params
            final_params = {**default_params, **generation_params}

            self.data_generator = self.data_generator.withColumnSpec(col_name, **final_params)

    def configure_data_generator(self):
        """Configure data generator for all columns."""
        self._configure_string_columns()
        self._configure_non_string_columns()

    def generate_data(self):
        """Generate synthetic data."""
        if not self.data_generator:
            raise RuntimeError("Data generator is not initialized.")
        try:
            synthetic_data = self.data_generator.build()
            return synthetic_data
        except Exception as e:
            print(f"Error generating data: {e}")
            self.spark.stop()
            sys.exit(1)

    def save_data(self, data, output_path, format="csv", options=None):
        """Save the generated data to a file.

        Parameters:
        - data: Spark DataFrame containing the synthetic data
        - output_path: Path to save the data
        - format: File format (default: "csv")
        - options: Dictionary of write options
        """
        options = options if options else {}
        try:
            data.write.format(format) \
                .options(**options) \
                .mode("overwrite") \
                .save(output_path)
            print(f"Generated data saved to: {output_path}")
        except Exception as e:
            print(f"Error writing generated data to file: {e}")
            self.spark.stop()
            sys.exit(1)

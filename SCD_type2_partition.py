from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit, expr, sum, col, year, month
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from dataclasses import dataclass
from configparser import ConfigParser
import logging
import time
import json
from typing import List, Dict
import os

# Schema Definitions
@dataclass
class EmployeeSchema:
    emp_id: int
    name: str
    address: str
    department: str
    salary: float
    region: str
    business_unit: str

# ConfigManager handles reading configurations from .ini files
class ConfigManager:
    def __init__(self, env: str = "prod"):
        self.config = ConfigParser()
        self.config.read(f'config_{env}.ini')
        self.env = env
    
    def get_spark_configs(self) -> Dict:
        # Returns Spark configuration properties from the config file
        return dict(self.config['spark'])
    
    def get_delta_path(self) -> str:
        # Retrieves the Delta table path from the config file
        return self.config['paths']['delta_table_path']

# MetricsTracker records job metrics such as processing duration and custom metrics
class MetricsTracker:
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.start_time = time.time()
        self.metrics = {}

    def track(self, metric_name: str, value: any):
        # Tracks a specific metric by storing its name and value
        self.metrics[metric_name] = value

    def save_metrics(self, spark: SparkSession):
        # Computes the duration and saves all metrics to a Delta table
        duration = time.time() - self.start_time
        metrics_data = {
            "job_name": self.job_name,
            "duration": duration,
            "timestamp": time.time(),
            **self.metrics
        }
        # Save the metrics data into the metrics.job_metrics table in Delta Lake
        spark.createDataFrame([metrics_data]).write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("metrics.job_metrics")

class DataValidator:
    """
    A utility class for validating dataframes against the expected schema
    and enforcing business rules for employee data.
    """
    
    @staticmethod
    def validate_schema(df, expected_schema: EmployeeSchema):
        """
        Validates that the dataframe has all required columns as per the expected schema.
        
        Parameters:
            df: The Spark DataFrame to validate.
            expected_schema: The expected schema defined as an EmployeeSchema dataclass.
            
        Raises:
            ValueError: If a required column is missing from the dataframe.
        """
        for field, dtype in expected_schema.__annotations__.items():
            if field not in df.columns:
                raise ValueError(f"Missing required column: {field}")

    @staticmethod
    def validate_business_rules(df):
        """
        Validates business-specific rules on the dataframe.
        
        Checks include:
         - Ensuring that key fields are not null.
         - Validating that salary values are within a specified acceptable range.
         
        Parameters:
            df: The Spark DataFrame containing employee data.
            
        Raises:
            ValueError: If any business rule validation fails.
        """
        # Check for nulls in key fields: emp_id, name, and department.
        null_counts = df.select([
            sum(col(c).isNull().cast("int")).alias(c) 
            for c in ["emp_id", "name", "department"]
        ]).collect()[0]
        
        if null_counts.emp_id > 0:
            raise ValueError("Found null emp_id values")
        
        # Check salary range (must be between 0 and 1,000,000)
        invalid_salary = df.filter("salary < 0 OR salary > 1000000").count()
        if invalid_salary > 0:
            raise ValueError("Found invalid salary values")

class DeltaLakeSCD2:
    def __init__(self, env: str = "prod"):
        self.config = ConfigManager(env)
        self.metrics = MetricsTracker("scd2_employee_update")
        self.setup_logging()
        self.init_spark()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scd2_job.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def init_spark(self):
        spark_configs = self.config.get_spark_configs()
        builder = SparkSession.builder.appName("DeltaLakeSCDType2")
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
        self.spark = builder.getOrCreate()

    def retry_function(self, func, retries=3, delay=5):
        for i in range(retries):
            try:
                return func()
            except Exception as e:
                self.logger.error(f"Attempt {i+1} failed: {str(e)}")
                if i < retries - 1:
                    time.sleep(delay * (2 ** i))
        raise RuntimeError(f"Function failed after {retries} attempts")

    def check_partition_sizes(self, table_path: str, partition_cols: List[str]):
        try:
            df = self.spark.read.format("delta").load(table_path) \
                .selectExpr("*", "_metadata.size as _size")
            sizes = df.groupBy(*partition_cols) \
                .agg(sum("_size").alias("partition_size")) \
                .filter("partition_size > 1024*1024*1024")
            return sizes
        except Exception as e:
            self.logger.error(f"Error checking partition sizes: {str(e)}")
            raise

    def optimize_partitions(self, table_path: str):
        try:
            self.logger.info(f"Optimizing partitions for {table_path}...")
            self.spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY emp_id")
            self.spark.sql(f"VACUUM delta.`{table_path}`")
            self.logger.info("Partition optimization completed")
        except Exception as e:
            self.logger.error(f"Failed to optimize partitions: {str(e)}")
            raise

    def prepare_staging_data(self, df_staging, effective_date=None):
        effective_date = effective_date or current_date()
        return df_staging \
            .withColumn("start_date", lit(effective_date)) \
            .withColumn("end_date", lit(None).cast("date")) \
            .withColumn("is_current", lit(True)) \
            .withColumn("load_year", year("start_date")) \
            .withColumn("load_month", month("start_date"))

    def perform_scd2_merge(self, delta_table_path: str, df_staging):
        try:
            deltaTable = DeltaTable.forPath(self.spark, delta_table_path)
        except Exception as e:
            self.logger.error(f"Delta table not found at {delta_table_path}: {str(e)}")
            raise

        change_condition = expr(" OR ".join([
            f"(source.{col} <> target.{col} OR " + 
            f"(source.{col} IS NULL AND target.{col} IS NOT NULL) OR " +
            f"(source.{col} IS NOT NULL AND target.{col} IS NULL))"
            for col in ["name", "address", "department", "salary", "region", "business_unit"]
        ]))

        merge_stmt = deltaTable.alias("target").merge(
            source=df_staging.alias("source"),
            condition="target.emp_id = source.emp_id AND target.is_current = true"
        )

        merge_stmt.whenMatchedUpdate(
            condition=change_condition,
            set={
                "end_date": expr("source.start_date - INTERVAL 1 DAY"),
                "is_current": lit(False)
            }
        ).whenNotMatchedInsertAll().execute()

    def write_initial_table(self, df, table_path: str):
        # Write the initial Delta table with schema merging and partitioning by load_year and load_month.
        df.write.format("delta") \
            .option("mergeSchema", "true") \
            .partitionBy("load_year", "load_month") \
            .mode("overwrite") \
            .save(table_path)

    def process_data(self, staging_data, effective_date=None):
        try:
            delta_table_path = self.config.get_delta_path()
            partition_cols = ["load_year", "load_month"]

            # Validate staging data
            DataValidator.validate_schema(staging_data, EmployeeSchema)
            DataValidator.validate_business_rules(staging_data)

            # Initialize table if not exists
            try:
                DeltaTable.forPath(self.spark, delta_table_path)
            except:
                self.logger.info("Delta table not found. Initializing...")
                df_initial = self.spark.createDataFrame([], schema=staging_data.schema)
                self.write_initial_table(
                    self.prepare_staging_data(df_initial, effective_date), 
                    delta_table_path
                )

            df_staging = self.prepare_staging_data(staging_data, effective_date)
            self.logger.info(f"Processing SCD Type 2 merge for {df_staging.count()} records")
            self.metrics.track("records_processed", df_staging.count())

            # Check and optimize partitions
            partition_sizes = self.check_partition_sizes(delta_table_path, partition_cols)
            if partition_sizes.count() > 0:
                self.logger.warning("Some partitions exceed 1GB, optimizing...")
                self.optimize_partitions(delta_table_path)

            # Perform merge with retry
            self.retry_function(
                lambda: self.perform_scd2_merge(delta_table_path, df_staging)
            )

            # Save metrics
            self.metrics.save_metrics(self.spark)

            return True

        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}")
            raise

def main():
    # Initialize SCD2 processor
    scd2_processor = DeltaLakeSCD2(env="prod")
    
    # Create sample staging data (replace with actual data source)
    staging_data = scd2_processor.spark.createDataFrame([
        (1, "John", "123 New Address St", "IT", 50000, "EMEA", "Technology"),
        (3, "Alice", "456 Some Rd", "Finance", 70000, "APAC", "Finance")
    ], EmployeeSchema.__annotations__.keys())

    # Process data
    scd2_processor.process_data(staging_data, effective_date="2023-10-01")

if __name__ == "__main__":
    main()

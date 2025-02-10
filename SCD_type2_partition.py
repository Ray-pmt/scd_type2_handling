from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit, expr, sum, concat, col
from delta.tables import DeltaTable

# Create Spark session
spark = SparkSession.builder \
    .appName("DeltaLakeSCDType2") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

def check_partition_sizes(table_path, partition_col):
    """Monitor partition sizes"""
    df = spark.read.format("delta").load(table_path)
    sizes = df.groupBy(partition_col) \
        .agg(sum("_size").alias("partition_size")) \
        .filter("partition_size > 1024*1024*1024")  # >1GB
    return sizes

def create_staging_data():
    """Create sample staging data"""
    incremental_data = [
        (1, "John", "123 New Address St", "IT", 50000, 2024),
        (3, "Alice", "456 Some Rd", "Finance", 70000, 2024)
    ]
    columns = ["emp_id", "name", "address", "department", "salary", "year"]
    return spark.createDataFrame(incremental_data, schema=columns)

def prepare_staging_data(df_staging):
    """Prepare staging data with SCD2 metadata"""
    return df_staging \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))

def perform_scd2_merge(delta_table_path, df_staging):
    """Execute SCD Type 2 merge operation"""
    deltaTable = DeltaTable.forPath(spark, delta_table_path)
    
    deltaTable.alias("target").merge(
        source=df_staging.alias("source"),
        condition="""target.emp_id = source.emp_id 
                    AND target.is_current = true 
                    AND target.year = source.year"""
    ).whenMatchedUpdate(
        set={
            "end_date": expr("DATE_SUB(current_date(), 1)"),
            "is_current": lit(False)
        }
    ).whenNotMatchedInsert(
        values={
            "emp_id": "source.emp_id",
            "name": "source.name",
            "address": "source.address",
            "department": "source.department",
            "salary": "source.salary",
            "year": "source.year",
            "start_date": "source.start_date",
            "end_date": "source.end_date",
            "is_current": "source.is_current"
        }
    ).execute()

def main():
    delta_table_path = "/mnt/delta/employees"
    
    # Create staging data
    df_staging = create_staging_data()
    df_staging = prepare_staging_data(df_staging)
    
    # Check partition sizes before merge
    partition_sizes = check_partition_sizes(delta_table_path, "year")
    if partition_sizes.count() > 0:
        print("Warning: Some partitions exceed 1GB")
        partition_sizes.show()
    
    # Perform SCD Type 2 merge
    perform_scd2_merge(delta_table_path, df_staging)
    
    # Verify changes
    df_result = spark.read.format("delta").load(delta_table_path)
    df_result.orderBy("emp_id", "start_date").show(truncate=False)

if __name__ == "__main__":
    main()
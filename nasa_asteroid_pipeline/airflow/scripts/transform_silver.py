from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from config import BRONZE_DIR, SILVER_DIR

def execute_silver_transform(spark):
    print("\n==================================================")
    print("[*] STARTING BATCH BRONZE TO SILVER TRANSFORMATION")
    print("==================================================")
    
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    
    # Schema updated to match the stream worker's aggregated JSON output footprint
    schema = StructType([
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
        StructField("is_potentially_hazardous_asteroid", StringType(), True),
        StructField("total_asteroids", LongType(), True),
        StructField("max_diameter", DoubleType(), True)
    ])
    
    try:
        # Read the aggregated stream metrics from Bronze
        bronze_df = spark.read.schema(schema).json(f"{BRONZE_DIR}/part-*.json")
        
        if bronze_df.count() > 0:
            # Drop duplicates on our grouping keys to ensure clean historical states
            deduped_df = bronze_df.dropDuplicates(["window_start", "is_potentially_hazardous_asteroid"])
            
            # Target limit specification: Order by latest windows to keep exactly 40 rows
            limit_window = Window.orderBy(col("window_start").desc())
            
            silver_df = deduped_df \
                .withColumn("row_num", row_number().over(limit_window)) \
                .filter(col("row_num") <= 40) \
                .drop("row_num") \
                .withColumn("updated_at", current_timestamp())
            
            # Idempotent write out to Parquet storage core
            silver_df.write.format("parquet").mode("overwrite").save(SILVER_DIR)
            
            written_count = spark.read.parquet(SILVER_DIR).count()
            print(f"[🎉 SUCCESS] Total Clean Summary Records in Silver (Capped at 40): {written_count}")
        else:
            print("[-] Execution halted: Bronze directory has no raw records yet.")
            
    except Exception as e:
        print(f"[❌ ERROR] Silver transformation execution aborted: {e}")
        
    print("==================================================\n")
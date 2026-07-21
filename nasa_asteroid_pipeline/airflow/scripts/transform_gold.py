from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, max, avg, lit, current_timestamp
from config import SILVER_DIR, GOLD_DIR  # Assuming you map GOLD_DIR in your config

def execute_gold_transform(spark):
    print("\n==================================================")
    print("[*] STARTING SILVER TO GOLD ANALYTICS SYNCHRONIZATION")
    print("==================================================")
    
    # Enable AQE for consistent optimization across layers
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    
    try:
        # 1. Read the newly structured, window-aggregated Silver Parquet data
        silver_df = spark.read.parquet(SILVER_DIR)
        
        if silver_df.count() > 0:
            print(f"[📊 SILVER RUNTIME] Reading summarized window metrics from Silver.")
            
            # 2. Generate high-level Executive Business KPIs 
            # We now SUM 'total_asteroids' and take the MAX of 'max_diameter'
            gold_kpi_df = silver_df.groupBy("is_potentially_hazardous_asteroid") \
                .agg(
                    sum("total_asteroids").alias("total_observed_asteroids"),
                    max("max_diameter").alias("historical_peak_diameter_km"),
                    avg("total_asteroids").alias("average_asteroids_per_window")
                ) \
                .withColumn("calculated_at", current_timestamp())
            
            # 3. Write out cleanly to the presentation layer
            gold_kpi_df.write \
                .format("parquet") \
                .mode("overwrite") \
                .save(f"{GOLD_DIR}/asteroid_executive_summary")
                
            print("[+] Gold analytics aggregation layer successfully synchronized.")
            
            # Print a quick preview to Airflow/Task logs for verification
            gold_kpi_df.show(truncate=False)
        else:
            print("[-] Execution halted: Silver directory contains an empty dataset.")
            
    except Exception as e:
        print(f"[❌ CRITICAL] Gold transformation pipeline aborted: {e}")
        
    print("==================================================\n")
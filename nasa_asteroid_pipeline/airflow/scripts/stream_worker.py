import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, from_json
from pyspark.sql.types import StructType, StructField, BooleanType, StringType
from delta.tables import DeltaTable

def write_to_delta_batch(batch_df, batch_id):
    """
    Processes each 10-second micro-batch. Instead of overwriting files on disk,
    it executes an upsert/merge logic to update historical aggregates.
    """
    target_path = "/opt/airflow/data/gold/asteroid_summary"
    
    # Check if the Delta table is already initialized
    if not os.path.exists(target_path) or not DeltaTable.isDeltaTable(spark, target_path):
        print(f"[Batch {batch_id}] Target Delta table doesn't exist. Initializing fresh baseline...")
        batch_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(target_path)
    else:
        print(f"[Batch {batch_id}] Merging state updates into target Delta table...")
        delta_table = DeltaTable.forPath(spark, target_path)
        
        # Merge streaming micro-batch into the Master Gold Delta state
        delta_table.alias("target") \
            .merge(
                batch_df.alias("source"),
                "target.is_potentially_hazardous_asteroid = source.is_potentially_hazardous_asteroid"
            ) \
            .whenMatchedUpdate(set={
                "total": "target.total + source.total"
            }) \
            .whenNotMatchedInsertAll() \
            .execute()


if __name__ == "__main__":
    # 1. Initialize Spark with BOTH Kafka and Delta Lake external packages
    spark = SparkSession.builder \
        .appName("NasaAsteroidStreaming") \
        .config(
            "spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0"
        ) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Suppress aggressive HDFS state provider warnings from flooding your terminal
    spark.sparkContext.setLogLevel("WARN")
    log4j_logger = spark._jvm.org.apache.log4j
    log_j_manager = log4j_logger.LogManager
    hdfs_logger = log_j_manager.getLogger("org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
    hdfs_logger.setLevel(log4j_logger.Level.ERROR)

    print("Spark context successfully built with Delta extensions. Reading from Kafka source...")

    # 2. Define your inbound Kafka stream
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "nasa1") \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Extract and parse your payload data cleanly from Kafka's value column
    # We define a flexible schema to catch the field whether it comes in as boolean or text string
    nasa_schema = StructType([
        StructField("is_potentially_hazardous_asteroid", StringType(), True)
    ])

    parsed_df = kafka_stream_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), nasa_schema).alias("data")) \
        .select("data.*") \
        .withColumn(
            "is_potentially_hazardous_asteroid", 
            col("is_potentially_hazardous_asteroid").cast(BooleanType())
        )

    # 4. Create your stream aggregates
    aggregated_stream = parsed_df \
        .groupBy("is_potentially_hazardous_asteroid") \
        .agg(count("*").alias("total"))

    # 5. Configure output with 'update' mode executing via our custom Delta Merge engine
    query = aggregated_stream.writeStream \
        .foreachBatch(write_to_delta_batch) \
        .outputMode("update") \
        .option("checkpointLocation", "/opt/airflow/data/gold/_checkpoints") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()
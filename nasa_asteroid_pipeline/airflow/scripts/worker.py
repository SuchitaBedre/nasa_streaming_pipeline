import sys
import os
import argparse

# DYNAMIC SPARK_HOME PATH OVERRIDE FOR CONTAINER ENVIRONMENTS
try:
    import pyspark
    pyspark_home = os.path.dirname(pyspark.__file__)
    os.environ["SPARK_HOME"] = pyspark_home
except ImportError:
    print("[-] PySpark dependency missing from environment runtime.")

from pyspark.sql import SparkSession
from transform_silver import execute_silver_transform
from transform_gold import execute_gold_transform

def init_spark():
    return SparkSession.builder \
        .appName("NasaAsteroidMedallionPipeline") \
        .config("spark.sql.warehouse.dir", "/opt/airflow/hive/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .master("local[*]") \
        .getOrCreate()

def main():
    parser = argparse.ArgumentParser(description="Medallion Layer Processing Worker CLI")
    parser.add_argument("layer", choices=["silver", "gold"], help="Target pipeline layer")
    parser.add_argument("--format", default="parquet", help="Storage target format choice")
    args = parser.parse_args()

    spark = init_spark()
    
    # Initialize separate relational catalogs
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    if args.layer == "silver":
        execute_silver_transform(spark)
        spark.catalog.createTable("silver.asteroid_snapshot", path="/opt/airflow/data/silver/nasa_asteroid", source=args.format)
        
    elif args.layer == "gold":
        execute_gold_transform(spark)
        spark.catalog.createTable("gold.asteroid_summary_kpi", path="/opt/airflow/data/gold/asteroid_summary", source=args.format)

if __name__ == "__main__":
    main()
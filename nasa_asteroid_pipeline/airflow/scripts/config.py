import os

# Base directory paths inside containers
BASE_DATA_DIR = "/opt/airflow/data"
BRONZE_DIR = os.path.join(BASE_DATA_DIR, "bronze/nasa_asteroid")
SILVER_DIR = os.path.join(BASE_DATA_DIR, "silver/nasa_asteroid")
GOLD_DIR = os.path.join(BASE_DATA_DIR, "gold/asteroid_summary")

# Streaming Configurations
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "nasa-asteroids"
CHECKPOINT_DIR = os.path.join(BASE_DATA_DIR, "silver/_checkpoints/nasa_asteroid")
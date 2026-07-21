import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta  # <-- ADDED timedelta HERE
from kafka import KafkaProducer

RAW_LANDING_DIR = "/opt/airflow/data/bronze/nasa_asteroid"
KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]
KAFKA_TOPIC = "nasa1"

def run_bronze_ingestion():
    os.makedirs(RAW_LANDING_DIR, exist_ok=True)
    
    # Initialize Kafka Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("[+] Kafka Producer initialized successfully.")
    except Exception as ke:
        print(f"[-] Failed to connect to Kafka: {ke}")
        producer = None

    # 1. Fetching raw streaming payload from NASA API
    url = "https://api.nasa.gov/neo/rest/v1/feed"
    
    # --- UPDATED REGION: CALCULATE A 7-DAY RANGE DYNAMICALLY ---
    today_str = datetime.now().strftime("%Y-%m-%d")
    seven_days_ago_str = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
    
    params = {
        "start_date": seven_days_ago_str,
        "end_date": today_str,
        "api_key": "DEMO_KEY"
    }
    # -----------------------------------------------------------
    
    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 200:
            # Keep your local Bronze JSON file backup
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(RAW_LANDING_DIR, f"raw_data_{timestamp}.json")
            with open(output_file, "w") as f:
                f.write(response.text)
            print(f"[+] Raw API snapshot written to {output_file}")
            
            # --- NEW KAFKA STREAMING BLOCK ---
            if producer:
                data = response.json()
                # Parse out individual asteroid records across different dates
                element_count = data.get("element_count", 0)
                near_earth_objects = data.get("near_earth_objects", {})
                
                print(f"[*] Parsing {element_count} asteroids to stream into Kafka...")
                
                for date, asteroids in near_earth_objects.items():
                    for asteroid in asteroids:
                        # Extract only the critical fields your Spark Schema expects
                        payload = {
                            "id": str(asteroid.get("id")),
                            "name": str(asteroid.get("name")),
                            "estimated_diameter_max_km": float(asteroid.get("estimated_diameter", {}).get("kilometers", {}).get("estimated_diameter_max", 0.0)),
                            "is_potentially_hazardous_asteroid": str(asteroid.get("is_potentially_hazardous_asteroid")).lower(),
                            "close_approach_date": str(date)
                        }
                        # Send to Kafka topic
                        producer.send(KAFKA_TOPIC, value=payload)
                
                producer.flush()
                print(f"[+] Successfully streamed records to Kafka topic: {KAFKA_TOPIC}")
                
    except Exception as e:
        print(f"[-] API ingestion failed: {e}")

    # 2. Check for any dropped local tracking CSV files to merge into Bronze
    csv_landing_file = "/opt/airflow/data/local_drops.csv"
    if os.path.exists(csv_landing_file):
        df = pd.read_csv(csv_landing_file)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df.to_csv(os.path.join(RAW_LANDING_DIR, f"csv_drop_{timestamp}.csv"), index=False)
        os.remove(csv_landing_file)
        print("[+] Local historical CSV moved to Bronze raw space")

if __name__ == "__main__":
    run_bronze_ingestion()
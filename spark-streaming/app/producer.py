import csv
import json
import time
import pandas as pd
from kafka import KafkaProducer

# Kafka configuration
KAFKA_TOPIC = "patient-vitals"
KAFKA_BROKER = "localhost:9092"  # Adjust to your Kafka setup

# File path to the dataset
DATA_FILE = "/app/CHARTEVENTS.csv"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Chunk size for reading the file
CHUNK_SIZE = 1000

def produce_vitals_from_csv(file_path, chunk_size):
    """
    Sends ICU patient vitals to the Kafka topic by filtering relevant rows in chunks.
    """
    for chunk_number, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size), start=1):
        # Filter rows with relevant ITEMIDs
        
        for _, row in chunk.iterrows():
            # Create a dictionary for the vital sign record
            record = {
                "subject_id": int(row["SUBJECT_ID"]),
                "icustay_id": int(row["ICUSTAY_ID"]),
                "charttime": row["CHARTTIME"],
                "vital_sign": int(row["ITEMID"]),
                "value": float(row["VALUENUM"]),
                "unit": row["VALUEUOM"],
            }

            # Send record to Kafka
            producer.send(KAFKA_TOPIC, record)
            print(f"Chunk {chunk_number}: {record}")

            # Simulate real-time streaming
            time.sleep(1)

if __name__ == "__main__":
    print("Starting Kafka producer for ICU vitals...")
    produce_vitals_from_csv(DATA_FILE, CHUNK_SIZE)
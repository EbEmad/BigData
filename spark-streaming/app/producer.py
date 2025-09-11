import csv
import json
import time
import pandas as pd
from kafka import KafkaProducer

# Kafka configuration
KAFKA_TOPIC = "patient-vitals"
KAFKA_BROKER = "kafka:9092"  # Adjust to your Kafka setup

# File path to the dataset
DATA_FILE = "../data/patient_vitals_raw.csv"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Chunk size for reading the file
CHUNK_SIZE = 100

def produce_vitals_from_csv(file_path, chunk_size):
    """
    Sends ICU patient vitals to the Kafka topic by filtering relevant rows in chunks.
    """
    for chunk_number, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size), start=1):
        # Filter rows with relevant ITEMIDs
        
        for _, row in chunk.iterrows():
            # Create a dictionary for the vital sign record
            record = {
                "subject_id": int(row["subject_id"]),
                "icustay_id": int(row["icustay_id"]),
                "charttime": row["charttime"],
                "vital_sign": int(row["vital_sign"]),
                "value": float(row["value"]),
                "unit": row["unit"],
            }

            # Send record to Kafka
            producer.send(KAFKA_TOPIC, record)
            print(f"Chunk {chunk_number}: {record}")

            # Simulate real-time streaming
            time.sleep(1)

if __name__ == "__main__":
    print("Starting Kafka producer for ICU vitals...")
    produce_vitals_from_csv(DATA_FILE, CHUNK_SIZE)
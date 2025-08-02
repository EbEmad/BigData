import time
import json
import pandas as pd
from kafka import KafkaProducer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os

# Kafka config
KAFKA_TOPIC = "stream_topic"
KAFKA_BROKER = "localhost:9092"
DATA_FILE = "../data/friends_data.csv"

# Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, file_path):
        self.file_path = file_path
        self.last_row_count = 0

    def on_modified(self, event):
        if event.src_path.endswith(os.path.basename(self.file_path)):
            try:
                df = pd.read_csv(self.file_path)
                new_rows = df.iloc[self.last_row_count:]  # Read only new rows
                if not new_rows.empty:
                    for _, row in new_rows.iterrows():
                        record = {
                            'id': int(row['id']),
                            'name': str(row['name']),
                            'age': int(row['age']),
                            'faculty': str(row['faculty']),
                            'hobby': str(row['hobby'])
                        }
                        producer.send(KAFKA_TOPIC, record)
                        print(f"Sent: {record}")
                    self.last_row_count = len(df)
            except Exception as e:
                print(f"Error reading or sending data: {e}")

if __name__ == "__main__":
    print("Kafka producer is running and watching for changes...")
    
    event_handler = FileChangeHandler(DATA_FILE)
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(DATA_FILE) or ".", recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)  # Keep main thread alive
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

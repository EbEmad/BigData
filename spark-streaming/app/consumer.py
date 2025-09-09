from kafka import KafkaConsumer
from prometheus_client import start_http_server, Gauge
import json
import threading

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_PATIENT_VITALS = 'patient-vitals'
TOPIC_ABNORMAL_VITAL_SIGN = 'abnormal-vital-sign'

# Prometheus Configuration
PROMETHEUS_PORT = 8000

# Prometheus Metrics
vitals_metric = Gauge(
    'patient_vitals', 
    'Patient vitals by icustay_id and vital_sign', 
    ['icustay_id', 'vital_sign', 'unit', 'charttime']
)

abnormal_vital_metric = Gauge(
    'abnormal_vital_sign', 
    'Abnormal vital sign alerts summary by window and subject_id', 
    ['window_start', 'window_end', 'subject_id', 'vital_sign', 'alerts_summary']
)

# Start Prometheus HTTP server
start_http_server(PROMETHEUS_PORT)

def consume_patient_vitals():
    consumer = KafkaConsumer(
        TOPIC_PATIENT_VITALS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='patient-vitals-group',
        auto_offset_reset='earliest'
    )
    print(f"Subscribed to topic: {TOPIC_PATIENT_VITALS}")

    for message in consumer:
        data = message.value
        icustay_id = data['icustay_id']
        vital_sign = data['vital_sign']
        value = data['value']
        unit = data['unit']
        charttime = data['charttime']

        # Print the message for debugging
        print(f"Patient Vitals Message: {data}")

        # Update Prometheus metric
        vitals_metric.labels(
            icustay_id=icustay_id, 
            vital_sign=vital_sign, 
            unit=unit, 
            charttime=charttime
        ).set(value)

def consume_abnormal_vital_sign():
    consumer = KafkaConsumer(
        TOPIC_ABNORMAL_VITAL_SIGN,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='abnormal-vital-sign-group',
        auto_offset_reset='earliest'
    )
    print(f"Subscribed to topic: {TOPIC_ABNORMAL_VITAL_SIGN}")

    for message in consumer:
        data = message.value
        window_start = data['window_start']
        window_end = data['window_end']
        subject_id = data['subject_id']
        vital_sign = data['vital_sign']
        alerts_summary = data['alerts_summary']

        # Print the message for debugging
        print(f"Abnormal Vital Sign Message: {data}")

        # Update Prometheus metric
        abnormal_vital_metric.labels(
            window_start=window_start,
            window_end=window_end,
            subject_id=subject_id,
            vital_sign=vital_sign,
            alerts_summary=alerts_summary
        ).set(1)  # Use 1 as the value since this is a summary metric

# Run both consumers concurrently
if __name__ == "__main__":
    threading.Thread(target=consume_patient_vitals, daemon=True).start()
    threading.Thread(target=consume_abnormal_vital_sign, daemon=True).start()

    print("Kafka Consumers running. Exposing metrics on port", PROMETHEUS_PORT)
    while True:
        pass
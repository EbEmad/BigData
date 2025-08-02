from kafka import KafkaConsumer
import psycopg2
import json

# Kafka Configuration
KAFKA_BROKER = 'localhost:29092'
# PostgreSQL Configuration
PG_HOST = 'localhost'
PG_PORT = 5432
PG_USER = 'user'
PG_PASSWORD = 'pass'

# Database Mapping for Each Topic
TOPIC_DB_MAPPING = {
    "stream_topic": {
        "database": "test",
        "insert_query": """
            INSERT INTO friends (id,name,age,faculty,hobby)
            VALUES (%s, %s, %s,%s,%s)
        """
    }
}

# Initialize Kafka Consumers for Each Topic
consumer={}
for topic in TOPIC_DB_MAPPING:
    consumer[topic] = KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id='my_consumer_group',  # <-- Add this!
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


# Process Each Topic
for topic,consumer in consumer.items():
    db_config=TOPIC_DB_MAPPING[topic]
    database_name=db_config['database']
    insert_query=db_config['insert_query']
    print(f"Processing topic: {topic}, database: {database_name}")

    # Connect to the respective PostgreSQL database
    conn=psycopg2.connect(
      host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=database_name,  
    )
    cursor=conn.cursor()

    for message in consumer:
        record=message.value
        print(f"resceived message from {topic}: {record}")

        # Map the record fields based on the topic
        try:
            if topic=='stream_topic':
                # Create table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS friends (
                        id INT,
                        name TEXT,
                        age INT,
                        faculty TEXT,
                        hobby TEXT
                    )
                    """)
                conn.commit()
                cursor.execute(
                    insert_query,(
                        record['id'],
                        record['name'],
                        record['age'],
                        record['faculty'],
                        record['hobby']
                    ),
                ) 

                # Commit the Transaction
                conn.commit()
        except Exception as e:
            print(f"Error processing record from {topic} : {e}")   
    # Close the connection after processing
    cursor.close()
    conn.close()   

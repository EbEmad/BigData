from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json, when, broadcast, concat_ws, lit, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import collect_list, array_join


# Constants for ITEMIDs (replace these with your actual ITEMIDs)

HR_ITEMID = 220045
SPO2_ITEMID = 220277
TEMP_ITEMID = 223762



def load_d_items(spark):
    """
    Load D_ITEMS.csv into a Spark DataFrame.
    """

    d_items_schema = StructType([
        StructField("ROW_ID", IntegerType(), True),
        StructField("ITEMID", IntegerType(), True),
        StructField("LABEL", StringType(), True),
        StructField("ABBREVIATION", StringType(), True),
        StructField("DBSOURCE", StringType(), True),
        StructField("LINKSTO", StringType(), True),
        StructField("CATEGORY", StringType(), True),
        StructField("UNITNAME", StringType(), True),
        StructField("PARAM_TYPE", StringType(), True),
        StructField("CONCEPTID", IntegerType(), True)
    ])


    return spark.read \
        .schema(d_items_schema) \
        .option("header","true") \
        .csv("/opt/bitnami/spark/data/D_ITEMS.csv") 


# Define schema for Kafka messages

schema = StructType([
    StructField("subject_id", IntegerType(), True),
    StructField("icustay_id", IntegerType(), True),
    StructField("charttime", TimestampType(), True),
    StructField("vital_sign", IntegerType(), True),
    StructField("value", FloatType(), True),
    StructField("unit", StringType(), True),
])


print("Starting Spark job...")

# Initialize Spark session
spark=SparkSession.builder \
    .appName("ICU_Vital_Streaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.default.parallelism", "4") \
    .config("spark.streaming.kafka.maxRatePerPartition", "100") \
    .config("spark.sql.shuffle.partitions", "4") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

print('Spark Session created....')

try:
    # Load D_ITEMS and broadcast
    d_items_df=broadcast(load_d_items(spark))
    print("D_ITEMS loaded and broadcasted...")

    # Read data from Kafka
    df=spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "patient-vitals") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("Kafka Stream Initialized...")

    # Deserialize Kafka value field (JSON)
    parsed_df=df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")
    
    #  Filter for selected ITEMIDs (HR, SpO2, Temperature)
    filtered_df=parsed_df.filter(
        col("vital_sign").isin(HR_ITEMID, SPO2_ITEMID, TEMP_ITEMID)
    )


    # Join with D_ITEMS for enrichment
    enriched_df=filtered_df.join(
        d_items_df,
        filtered_df.vital_sign==d_items_df.ITEMID,
        "left"
    )

    print("Stream processing started...")

    # Identify abnormal vitals
    abnormal_vitals=enriched_df.withColumn(
        "alert",
        when((col("ABBREVIATION")=="HR") & ((col("value")<60) |(col("value")>100)),
        concat_ws(" - ",col("LABEL"),lit("Abnormal")))
        .when((col("ABBREVIATION")=="SpO2") & (col("value") < 90),
              concat_ws(" - ",col("LABEL"),lit("Low")))
        .when((col("ABBREVIATION") == "Temperature C") & ((col("value") < 36) | (col("value") > 38)),
              concat_ws(" - ", col("LABEL"), lit("Abnormal"))) 
        .otherwise(None)
    ).filter("alert IS NOT NULL")

    # Apply a time window for aggregation 
    window_vitals=abnormal_vitals.groupBy(
        window(col('charttime'),'5 minutes'),   # 5-minute window
        col("subject_id"),
        col("vital_sign")
    ).agg(
        collect_list(col("alert")).alias("alerts_list")  # Aggregate alerts into a list
    ).withColumn(
        "alerts_summary",array_join(col("alerts_list"),", ")  # Join alerts into a single string
    )

    # Convert aggregated data to JSON format for Kafka
    kafka_output=window_vitals.select(
        to_json(
            struct(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("subject_id").cast("string").alias("subject_id"),
                col("vital_sign").cast("string").alias("vital_sign"),
                col("alerts_summary").alias("alerts_summary")
            )
        ).alias("value")
    )

    print("Abnormal vitals aggregated by time window...")

    # Write abnormal vitals to Kafka
    query=kafka_output.writeStream \
        .outputMode('update') \
        .format("kafka") \
        .trigger(processingTime='5 seconds') \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "abnormal-vital-sign") \
        .option("checkpointLocation", "/tmp/checkpoint_kafka_window") \
        .start()
    
    print("Abnormal vitals (windowed) written to Kafka...")
    query.awaitTermination()
except Exception as e:
    print(f"Error occurred: {str(e)}")
    spark.stop()
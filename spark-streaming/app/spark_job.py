from spark.sql import SparkSession
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
        .csv("/data/D_ITEMS.csv") 


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
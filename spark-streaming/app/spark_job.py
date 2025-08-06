from spark.sql import  SparkSession
from pyspark.sql.functions import from_json,col,struct ,to_json,when,broadcast,concat_ws,lit,window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import collect_list, array_join



def load_d_items(spark):
    """
        Load data.csv into spark Dataframe
    """
    d_items_schema=StructType([
        StructField('id',IntegerType(),True),
        StructField('name',StringType(),True),
        StructField('age',IntegerType(),True),
        StructField('faculty',StringType(),True),
        StructField('hobby',StringType(),True)
    ])
    return spark.read \
          .schema(d_items_schema) \
          .option('header','true') \
          .csv('../data/friends_data.csv')

# define schema for kafka messages
schema=StructType([
    StructField('id',IntegerType(),True),
    StructField('name',StringType(),True),
    StructField('age',IntegerType(),True),
    StructField('faculty',StringType(),True),
    StructField('hobby',StringType(),True)
])

print("Starting spark job")

# Set log level to reduce noise
spark=SparkSession.builder \
    .appName('friends_data') \
    .config("spark.sql.streaming.checkpointLocation","/tmp/checkpoint") \
    .config("spark.executor.memory","2g") \
    .config("spark.driver.memory","2g") \
    .config("spark.executor.cores","2")\
    .config("spark.driver.cores","2")\
    .config("spark.default.parallelism","4")\
    .config("spark.streaming.kafka.maxRatePerPartition","2")\
    .config("spark.sql.shuffle.partitions", "4") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")
print("Spark session created...")

try:
    # Load data 
    df=broadcast(load_d_items(spark))
    print("data loaded and broadcasted")
    
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
def get_spark(app_name="SparkOptDemo", configs=None):
    """Create a SparkSession with optional config overrides."""
    builder=SparkSession.builder.appName(app_name).master("local[*]")
    if configs:
        for k,v in configs.items():
            builder=builder.config(k,v)
    return builder.getOrcreate()
    
def generate_large_table(spark,num_rows=10_000_000,num_partitions=200):
    """Simulate a protocol-events-style table (large fact table)."""
    return (
        spark.range(0,num_rows,numPartitions=num_partitions)
        .withColumn("account_number",(F.col("id")% 500_000).cast("string"))
        .withColumn("event_date", F.date_add(F.lit("2025-01-01"), (F.col("id") % 90).cast("int")))
        .withColumn("session_duration",(F.rand()*3600).cast("int"))
        .withColumn("download_bytes",(F.rand()*1_000_000).cast("long"))
        .withColumn("upload_bytes", (F.rand() * 500_000).cast("long"))
        .withColumn("category", F.concat(F.lit("cat_"), (F.col("id") % 50).cast("string")))
    )

def generate_small_lookup(spark,num_rows=5_000):
    """Simulate a small dimension / lookup table (e.g., URL categories)."""
    return (
        spark.range(0, num_rows)
        .withColumn("category", F.concat(F.lit("cat_"), F.col("id").cast("string")))
        .withColumn("category_name", F.concat(F.lit("Category "), F.col("id").cast("string")))
        .withColumn("priority", (F.rand() * 10).cast("int"))
    )


def generate_skewed_table(spark, num_rows=10_000_000, hot_keys=5, hot_key_ratio=0.6):
    """
    Generate a table where a few 'hot' keys hold 60% of the data.
    This simulates real-world data skew (e.g., a few heavy-usage accounts).
    """
    hot_rows = int(num_rows * hot_key_ratio)
    normal_rows = num_rows - hot_rows

    hot_df = (
        spark.range(0, hot_rows)
        .withColumn("account_number", (F.col("id") % hot_keys).cast("string"))
        .withColumn("value", F.rand() * 1000)
    )
    normal_df = (
        spark.range(0, normal_rows)
        .withColumn("account_number", (F.col("id") % 100_000 + hot_keys).cast("string"))
        .withColumn("value", F.rand() * 1000)
    )
    return hot_df.union(normal_df)
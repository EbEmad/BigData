from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Silver layer: Cleaned transaction data with standardized product names and categories"
)
def silver_transactions_cleaned():
    return (
        spark.read.table("bronze_transactions")
        .filter(
            (F.col("transaction_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("quantity") > 0) &
            (F.col("total_amount") > 0)
        )
        .withColumn("product_name", F.trim(F.lower(F.col("product_name"))))
        .withColumn("category", F.trim(F.initcap(F.col("category"))))
        .withColumn("store_location", F.trim(F.col("store_location")))
        .withColumn("payment_method", F.trim(F.col("payment_method")))
    )

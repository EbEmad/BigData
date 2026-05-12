from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Gold layer: Daily transaction metrics aggregated by date, category, and store location"
)
def gold_daily_transactions():
    return (
        spark.read.table("silver_transactions_cleaned")
        .withColumn("transaction_day", F.to_date(F.col("transaction_date")))
        .groupBy("transaction_day", "category", "store_location")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_transaction_amount"),
            F.sum("quantity").alias("total_quantity_sold"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("product_id").alias("unique_products")
        )
        .orderBy("transaction_day", "category", "store_location")
    )

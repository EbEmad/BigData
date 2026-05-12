from pyspark import pipelines as dp

@dp.materialized_view(
    name="bronze_transactions",
    comment="Bronze layer: Raw transaction data"
)
def bronze_transactions():
    return spark.read.table("test.default.transactions_2025_01_06")
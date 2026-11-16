from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp

# ──────────────────────────────────────────────
# 1. Create Spark Session
#    Catalogs (iceberg, iceberg_rest, hive_prod)
#    are already configured in spark-defaults.conf
# ──────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("TestSparkJob") \
    .getOrCreate()

print("=" * 60)
print("Spark session created successfully!")
print(f"App Name : {spark.sparkContext.appName}")
print(f"Spark UI : {spark.sparkContext.uiWebUrl}")
print("=" * 60)

# ──────────────────────────────────────────────
# 2. Create sample data
# ──────────────────────────────────────────────
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
])

data = [
    (1, "Ahmed", "Engineering", 8000),
    (2, "Sara", "Data Science", 9000),
    (3, "Mohamed", "Engineering", 7500),
    (4, "Nour", "Marketing", 6000),
    (5, "Omar", "Data Science", 8500),
]

df = spark.createDataFrame(data, schema)

print("\n>> Sample Data:")
df.show()

# ──────────────────────────────────────────────
# 3. Write to HDFS as Parquet
# ──────────────────────────────────────────────
hdfs_path = "hdfs://namenode:8020/data/employees_parquet"

df.write \
    .mode("overwrite") \
    .parquet(hdfs_path)

print(f">> Data written to HDFS: {hdfs_path}")

# ──────────────────────────────────────────────
# 4. Read back from HDFS
# ──────────────────────────────────────────────
df_from_hdfs = spark.read.parquet(hdfs_path)

print("\n>> Data read back from HDFS:")
df_from_hdfs.show()

# ──────────────────────────────────────────────
# 5. Create an Iceberg table (REST Catalog)
# ──────────────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_rest.demo")

# Add a timestamp column and write as an Iceberg table
df_with_ts = df.withColumn("created_at", current_timestamp())

df_with_ts.writeTo("iceberg_rest.demo.employees") \
    .using("iceberg") \
    .createOrReplace()

print(">> Iceberg table created: iceberg_rest.demo.employees")

# ──────────────────────────────────────────────
# 6. Query the Iceberg table with Spark SQL
# ──────────────────────────────────────────────
print("\n>> Querying Iceberg table:")
spark.sql("SELECT * FROM iceberg_rest.demo.employees").show()

print("\n>> Employees with salary > 7500:")
spark.sql("""
    SELECT name, department, salary
    FROM iceberg_rest.demo.employees
    WHERE salary > 7500
    ORDER BY salary DESC
""").show()

print("\n>> Average salary by department:")
spark.sql("""
    SELECT department, 
           COUNT(*) as employee_count,
           AVG(salary) as avg_salary
    FROM iceberg_rest.demo.employees
    GROUP BY department
    ORDER BY avg_salary DESC
""").show()

# ──────────────────────────────────────────────
# 7. Done
# ──────────────────────────────────────────────
print("=" * 60)
print("Spark job completed successfully!")
print("=" * 60)

spark.stop()

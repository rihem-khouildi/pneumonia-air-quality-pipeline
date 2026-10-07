from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as sum_, when, current_timestamp

# -------------------------
# Spark session
# -------------------------
spark = SparkSession.builder \
    .appName("BatchCityAggregator") \
    .config("spark.cassandra.connection.host", "cassandra-iot") \
    .getOrCreate()

# Reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# -------------------------
# Read data from HDFS (Parquet)
# -------------------------
hdfs_path = "hdfs://namenode:9000/data/data_folder/"
print(f"Reading data from HDFS: {hdfs_path}")
df = spark.read.parquet(hdfs_path)

# Check if data is loaded
total_rows = df.count()
print(f"Total rows in HDFS: {total_rows}")
if total_rows == 0:
    print("⚠️ No data found in HDFS. Exiting.")
    spark.stop()
    exit(1)

df.show(5, truncate=False)
print(f"Columns: {df.columns}")

# -------------------------
# Aggregate by city
# -------------------------
print("Aggregating data by city...")
city_summary = df.groupBy("city", "country", "who_region", "year") \
    .agg(
        count("patient_id").alias("total_patients"),
        sum_(when(col("label") == "PNEUMONIA", 1).otherwise(0)).alias("pneumonia_count"),
        avg("oxygen_saturation").alias("avg_spo2"),
        avg("pm25").alias("avg_pm25"),
        avg("pm10").alias("avg_pm10"),
        avg("no2").alias("avg_no2")
    )

# Add last_update timestamp
city_summary = city_summary.withColumn("last_update", current_timestamp())

# -------------------------
# Compute city risk score
# -------------------------
city_summary = city_summary.withColumn(
    "city_risk_score",
    (col("pneumonia_count") / col("total_patients") * 0.5 +
     col("avg_pm25") / 100 * 0.5)
)

# Preview aggregated data
print("Preview of aggregated city data:")
city_summary.show(5, truncate=False)

# -------------------------
# Write aggregated results to Cassandra
# -------------------------
print("Writing aggregated data to Cassandra...")
try:
    city_summary.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(keyspace="iot_keyspace", table="city_patient_summary") \
        .save()
    print("✓ Data successfully written to Cassandra (city_patient_summary)")
except Exception as e:
    print(f"✗ Error writing to Cassandra: {str(e)}")

print("Batch aggregation from HDFS Parquet completed.")
spark.stop()

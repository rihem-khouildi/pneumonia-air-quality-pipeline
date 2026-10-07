
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.sql.types import StringType, DoubleType, IntegerType

# Create Spark 

spark = SparkSession.builder \
    .appName("KafkaToCassandraAndHDFS") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.cassandra.connection.host", "cassandra-iot") \
    .config("spark.cassandra.auth.username", "cassandra_user") \
    .config("spark.cassandra.auth.password", "cassandra_password") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-iot:9092") \
    .option("subscribe", "kafka_topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert value to string
string_df = kafka_df.selectExpr("CAST(value AS STRING) as csv_string")

# Split CSV string into columns based on your dataset structure
parsed_df = string_df.select(
    split(col("csv_string"), ",").getItem(0).alias("patient_id"),  # patient_id
    split(col("csv_string"), ",").getItem(1).alias("image_path"),  # image_path
    split(col("csv_string"), ",").getItem(2).alias("label"),  # label (NORMAL, etc.)
    split(col("csv_string"), ",").getItem(3).cast(DoubleType()).alias("heart_rate"),  # heart_rate
    split(col("csv_string"), ",").getItem(4).cast(DoubleType()).alias("oxygen_saturation"),  # oxygen_saturation
    split(col("csv_string"), ",").getItem(5).cast(DoubleType()).alias("body_temperature"),  # body_temperature
    split(col("csv_string"), ",").getItem(6).cast(DoubleType()).alias("respiratory_rate"),  # respiratory_rate
    split(col("csv_string"), ",").getItem(7).cast(IntegerType()).alias("cough"),  # cough (0 or 1)
    split(col("csv_string"), ",").getItem(8).cast(IntegerType()).alias("chest_pain"),  # chest_pain (0 or 1)
    split(col("csv_string"), ",").getItem(9).cast(IntegerType()).alias("shortness_of_breath"),  # shortness_of_breath (0 or 1)
    split(col("csv_string"), ",").getItem(10).alias("who_region"),  # WHO region
    split(col("csv_string"), ",").getItem(11).alias("country"),  # country
    split(col("csv_string"), ",").getItem(12).alias("city"),  # city
    split(col("csv_string"), ",").getItem(13).cast(IntegerType()).alias("year"),  # year
    split(col("csv_string"), ",").getItem(14).cast(DoubleType()).alias("pm25"),  # pm25
    split(col("csv_string"), ",").getItem(15).cast(DoubleType()).alias("pm10"),  # pm10
    split(col("csv_string"), ",").getItem(16).cast(DoubleType()).alias("no2")  # no2
)

# Function to write each micro-batch to HDFS and Cassandra
def write_to_hdfs_and_cassandra(batch_df, batch_id):
    print(f"\n========== Processing Batch {batch_id} ==========")
    print(f"Batch row count: {batch_df.count()}")
    
    if batch_df.count() > 0:
        # Show sample data for debugging
        print("Sample data from this batch:")
        batch_df.show(5, truncate=False)
        
        # Filter out null rows
        valid_df = batch_df.filter(col("patient_id").isNotNull())
        
        print(f"Valid rows after filtering: {valid_df.count()}")
        
        if valid_df.count() > 0:
            # Write data to HDFS (as CSV, can be changed to Parquet if preferred)
            try:
                valid_df.write \
                    .format("parquet") \
                    .mode("append") \
                     .save("hdfs://namenode:9000/data/data_folder")
                print(f"✓ Batch {batch_id} written to HDFS")
            except Exception as e:
                print(f"✗ Error writing batch {batch_id} to HDFS: {str(e)}")

            # Write data to Cassandra
            try:
                valid_df.write \
                    .format("org.apache.spark.sql.cassandra") \
                    .option("keyspace", "iot_keyspace") \
                    .option("table", "iot_data") \
                    .mode("append") \
                    .save()
                print(f"✓ Batch {batch_id} written to Cassandra")
            except Exception as e:
                print(f"✗ Error writing batch {batch_id} to Cassandra: {str(e)}")
        else:
            print(f"⚠ Batch {batch_id} has no valid data after filtering")
    else:
        print(f"⚠ Batch {batch_id} is empty")
    print("=" * 50)

# Start streaming query using foreachBatch
query = parsed_df.writeStream \
    .foreachBatch(write_to_hdfs_and_cassandra) \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .trigger(processingTime='5 seconds') \
    .start()

print("\n✓ Streaming query started successfully!")
print("Waiting for data from Kafka topic 'kafka_topic'...")
print("Press Ctrl+C to stop\n")

query.awaitTermination()

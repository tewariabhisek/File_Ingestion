# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, year, month, dayofmonth
from pyspark.sql.window import Window

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("EventProcessingPipeline") \
    .getOrCreate()

# Input and output paths
input_path = "dbfs:/FileStore/shared_uploads/tewariabhisek@gmail.com/events.json"  # Path where JSON files are stored
output_path = "/dbfs/FileStore/shared_uploads/tewariabhisek@gmail.com/"   # Path where Parquet files will be saved

# Step 2: Read JSON Files into DataFrame
df = spark.read.json(input_path)

# Step 3: Convert Timestamp to Timestamp Data Type
# Ensure timestamp is in proper format and add year, month, and day columns
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Step 4: Add Partitioning Columns
df = df.withColumn("year", year(col("timestamp"))) \
       .withColumn("month", month(col("timestamp"))) \
       .withColumn("day", dayofmonth(col("timestamp")))

# Step 5: Deduplicate Events
# Use a Window function to rank events by timestamp for each event_id
window_spec = Window.partitionBy("event_id").orderBy(col("timestamp").desc())
deduped_df = df.withColumn("rank", row_number().over(window_spec)) \
               .filter(col("rank") == 1) \
               .drop("rank")  # Drop the temporary rank column

# Step 6: Write Deduplicated Data to Parquet with Partitioning
# Partition by year, month, day, and event_type
deduped_df.write.partitionBy("year", "month", "day", "event_type") \
               .mode("append") \
               .parquet(output_path)

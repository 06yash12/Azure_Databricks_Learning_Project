# Databricks notebook source
checkpoint_location = "abfss://raw@yashdatabricks.dfs.core.windows.net/standard/autoloader_sink/checkpoint"

schema_location = "abfss://raw@yashdatabricks.dfs.core.windows.net/standard/autoloader_sink/checkpoint/schema_location"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schema explicitly to avoid schema inference error when input path is empty
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("value", StringType(), True)
])

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option('multiLine', 'true') \
  .option("cloudFiles.schemaLocation", schema_location) \
  .schema(schema) \
  .load("abfss://raw@yashdatabricks.dfs.core.windows.net/standard/autoloader_source") \
  .writeStream.format("delta") \
  .option("checkpointLocation", checkpoint_location) \
  .trigger(once=True) \
  .option("path", "abfss://raw@yashdatabricks.dfs.core.windows.net/standard/autoloader_sink/data") \
  .start()

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option('multiLine', 'true')\
  .option("cloudFiles.schemaLocation", schema_location) \
  .load("abfss://raw@yashdatabricks.dfs.core.windows.net/standard/autoloader_source")
  .writeStream.format("delta") \
  .option("checkpointLocation", checkpoint_location) \
  .trigger(once=True) \
  .option("path", "abfss://raw@yashdatabricks.dfs.core.windows.net/standard/autoloader_sink/data")
) .start()
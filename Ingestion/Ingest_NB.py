# Databricks notebook source
df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("abfss://raw@yashdatabricks.dfs.core.windows.net/standard/notebook/flights.csv")

# COMMAND ----------

display(df)

# COMMAND ----------


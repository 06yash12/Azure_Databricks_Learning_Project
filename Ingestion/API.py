# Databricks notebook source
import requests

data = requests.get('https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json').json()

# COMMAND ----------

data

# COMMAND ----------

data.get('Results',[])

# COMMAND ----------

data_records = data.get('Results',[])
df = spark.createDataFrame(data_records)

# COMMAND ----------

display (df)

# COMMAND ----------

df.write.format('delta')\
    .mode("append")\
    .option('path', "abfss://raw@yashdatabricks.dfs.core.windows.net/standard/api_data")\
    .saveAsTable("azuredatabricks_catalog.ingest.api_data")

# COMMAND ----------


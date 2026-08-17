from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table
def bronze_trips():
    df = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format","csv")\
                .option("cloudFiles.inferColumnTypes",True)\
                .load("abfss://raw@yashdatabricks.dfs.core.windows.net/rideshare/trips")
    return df
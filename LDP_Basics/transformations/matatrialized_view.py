from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.materialized_view
def aggregarted_table():
    df = spark.read.table("served_data")
    df = df.groupBy("id").agg(count("name").alias("count"))
    return df
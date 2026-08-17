from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table
def ingest_data():
    df = spark.readStream.table("azuredatabricks_catalog.dummy.src_tbl")
    df = df.withColumn("age", col("age").cast("double"))
    df = df.withColumn("ingest_timestamp", current_timestamp())
    return df


@dp.temporary_view
def transform_data():
    df = spark.readStream.table("ingest_data")
    df = df.withColumn("transformed", lit("transformed"))
    return df


@dp.table
def served_data():
    df = spark.readStream.table("transform_data")
    df = df.withColumn("served", lit("served"))
    return df
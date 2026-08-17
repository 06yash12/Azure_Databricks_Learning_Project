from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.table
def fact():

    df = spark.readStream.table("gold_obt")

    df = df.select(
        "trip_id",
        "customer_id",
        "driver_id",
        "vehicle_id",
        "payment_id",
        "distance_km",
        "fare_amount"
    )

    return df

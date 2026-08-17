from pyspark import pipelines as dp
from pyspark.sql.functions import *


trips_exp = {"notnull":"trip_id is not null",
             "greater":"fare_amount < 0"}

@dp.table
# @dp.expect_all_or_fail(trips_exp)
def silver_trips():

    df = spark.readStream.table('bronze_trips')

    # Any PySpark Transformation
    df = df.withColumn('trip_status',upper(col('trip_status')))
    df = df.withColumn('silver_processed_at',current_timestamp())

    return df
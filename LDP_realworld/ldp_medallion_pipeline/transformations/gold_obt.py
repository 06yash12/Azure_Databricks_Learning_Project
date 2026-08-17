from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.table
def gold_obt():

    trips = spark.readStream.table("silver_trips")\
    .withWatermark("updated_datetime", "10 minutes")\
    .alias('t')

    customers = spark.read.table("azuredatabricks_catalog.ldp_medallion.customers").alias('c')
    drivers = spark.read.table("azuredatabricks_catalog.ldp_medallion.drivers").alias('d')
    vehicles = spark.read.table("azuredatabricks_catalog.ldp_medallion.vehicles").alias('v')
    payments = spark.read.table("azuredatabricks_catalog.ldp_medallion.payments").alias('p')
    
    df_join = trips.join(customers, col("t.customer_id") == col("c.customer_id"), "left")\
                    .join(drivers, col("t.driver_id") == col("d.driver_id"), "left")\
                    .join(vehicles, col("d.driver_id") == col("v.driver_id"), "left")\
                    .join(payments, col("t.trip_id") == col("p.trip_id"), "left")\
                    .select(
            # -- Trip --
            col("t.trip_id"),
            col("t.pickup_datetime"),
            col("t.dropoff_datetime"),
            col("t.pickup_location"),
            col("t.dropoff_location"),
            col("t.distance_km"),
            col("t.fare_amount"),
            col("t.trip_status"),
            col("t.updated_datetime"),
            # -- Customer --
            col("t.customer_id"),
            col("c.customer_name"),
            col("c.email"),
            col("c.phone"),
            col("c.city").alias("customer_city"),
            col("c.rating").alias("customer_rating"),
            col("c.customers_updated_datetime"),
            # -- Driver --
            col("t.driver_id"),
            col("d.driver_name"),
            col("d.license_number"),
            col("d.driver_rating"),
            col("d.city").alias("driver_city"),
            col("d.drivers_updated_datetime"),
            # -- Vehicle --
            col("v.vehicle_id"),
            col("v.vehicle_model"),
            col("v.vehicle_year"),
            col("v.license_plate"),
            col("v.vehicles_updated_datetime"),
            # -- Payment --
            col("p.payment_id"),
            col("p.payment_method"),
            col("p.payment_amount"),
            col("p.payment_status"),
            col("p.payments_updated_datetime")
        )
                    
    return df_join 

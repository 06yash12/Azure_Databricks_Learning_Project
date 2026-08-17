from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.temporary_view
def stg_dim_customers():

    df = spark.readStream.table("gold_obt")

    df = df.select(
        "customer_id",
        "email",
        "phone",
        "customer_city",
        "customer_rating",
        "customers_updated_datetime"
    )

    return df    

@dp.temporary_view
def stg_dim_drivers():

    df = spark.readStream.table("gold_obt")

    df = df.select(
        "driver_id",
        "driver_name",
        "driver_rating",
        "license_number",
        "driver_city",
        "drivers_updated_datetime"
    )

    return df   


@dp.temporary_view
def stg_dim_vehicles():

    df = spark.readStream.table("gold_obt")

    df = df.select(
        "vehicle_id",
        "vehicle_model",
        "vehicle_year",
        "license_plate",
        "vehicles_updated_datetime"
    )

    return df  


@dp.temporary_view
def stg_dim_payments():

    df = spark.readStream.table("gold_obt")

    df = df.select(
        "payment_id",
        "payment_method",
        "payment_amount",
        "payment_status",
        "payments_updated_datetime"
    )

    return df  


@dp.temporary_view
def stg_dim_trips():

    df = spark.readStream.table("gold_obt")

    df = df.select(
        "trip_id",
        "pickup_location",
        "dropoff_datetime",
        "dropoff_location",
        "trip_status",
        "updated_datetime"
    )

    return df  





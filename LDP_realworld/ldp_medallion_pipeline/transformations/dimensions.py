from pyspark import pipelines as dp
from pyspark.sql.functions import *

# --------- DIM CUSTOMERS ----------------------
dp.create_streaming_table(
    name = "dim_customers",
    expect_all = {"notnull":"customer_id is not null"}
)

dp.create_auto_cdc_flow(
    target = "dim_customers",
    source = "stg_dim_customers",
    keys = ["customer_id"],
    sequence_by = "customers_updated_datetime",
    stored_as_scd_type = "2"
)

# --------- DIM DRIVERS ----------------------
dp.create_streaming_table(
    name = "dim_drivers"
)

dp.create_auto_cdc_flow(
    target = "dim_drivers",
    source = "stg_dim_drivers",
    keys = ["driver_id"],
    sequence_by = "drivers_updated_datetime",
    stored_as_scd_type = "2"
)

# --------- DIM VEHICLES ----------------------
dp.create_streaming_table(
    name = "dim_vehicles"
)

dp.create_auto_cdc_flow(
    target = "dim_vehicles",
    source = "stg_dim_vehicles",
    keys = ["vehicle_id"],
    sequence_by = "vehicles_updated_datetime",
    stored_as_scd_type = "2"
)

# --------- DIM PAYMENTS ----------------------
dp.create_streaming_table(
    name = "dim_payments"
)

dp.create_auto_cdc_flow(
    target = "dim_payments",
    source = "stg_dim_payments",
    keys = ["payment_id"],
    sequence_by = "payments_updated_datetime",
    stored_as_scd_type = "2"
)


# --------- DIM TRIPS ----------------------
dp.create_streaming_table(
    name = "dim_trips"
)

dp.create_auto_cdc_flow(
    target = "dim_trips",
    source = "stg_dim_trips",
    keys = ["trip_id"],
    sequence_by = "updated_datetime",
    stored_as_scd_type = "2"
)







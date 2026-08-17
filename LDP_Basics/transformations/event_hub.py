from pyspark import pipelines as dp
from pyspark.sql.functions import *


# Event Hubs Configs
EH_NAMESPACE = "yasheventhub"
EH_NAME = "eventhubtopic"

EH_CONN_STR = spark.conf.get("EH_CONN_STR")

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',
    "kafka.request.timeout.ms": 10000,
    "kafka.session.timeout.ms": 10000,
    "maxOffsetsPerTrigger": 10000,
    "failOnDataLoss": "true",
    "startingOffsets": "earliest"
}


@dp.table
def events_table_new():

    df = spark.readStream.format("kafka") \
        .options(**KAFKA_OPTIONS) \
        .load()

    df = df.withColumn("value", col("value").cast("string"))
    return df
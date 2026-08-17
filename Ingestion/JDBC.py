# Databricks notebook source
jdbc_host = "azuredatabricksserveryash.database.windows.net"
jdbc_db = "azuredatabricksdb"
jdbc_port = "1433"

jdbc_url = (
    f"jdbc:sqlserver://{jdbc_host}:{jdbc_port};"
    f"database={jdbc_db};"
    "encryption=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)

connectionProperties = {
    "user": "adminyash",
    "password": "Sql@1234",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

df = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.ordersnew",
    properties=connectionProperties
)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("azuredatabricks_catalog.ingest.sql_data")

# COMMAND ----------


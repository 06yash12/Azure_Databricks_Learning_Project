# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG azuredatabricks_catalog;
# MAGIC
# MAGIC USE SCHEMA gold;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS profiles (
# MAGIC     First_Name STRING,
# MAGIC     Last_Name STRING,
# MAGIC     Phone_Number STRING,
# MAGIC     Address STRING,
# MAGIC     SSN STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO profiles (First_Name, Last_Name, Phone_Number, Address, SSN)
# MAGIC VALUES
# MAGIC ('John', 'Doe', '123-456-7890', '123 Main St, NY', '123-45-6789'),
# MAGIC ('Jane', 'Smith', '234-567-8901', '456 Oak St, CA', '234-56-7890'),
# MAGIC ('Alice', 'Johnson', '345-678-9012', '789 Pine St, TX', '345-67-8901'),
# MAGIC ('Bob', 'Brown', '456-789-0123', '321 Maple St, FL', '456-78-9012'),
# MAGIC ('Charlie', 'Davis', '567-890-1234', '654 Cedar St, IL', '567-89-0123'),
# MAGIC ('Emily', 'White', '678-901-2345', '987 Birch St, WA', '678-90-1234'),
# MAGIC ('Frank', 'Miller', '789-012-3456', '741 Spruce St, WA', '789-01-2345'),
# MAGIC ('Grace', 'Wilson', '890-123-4567', '852 Elm St, NV', '890-12-3456'),
# MAGIC ('Hank', 'Moore', '901-234-5678', '963 Walnut St, CO', '901-23-4567'),
# MAGIC ('Ivy', 'Taylor', '012-345-6789', '159 Aspen St, AZ', '012-34-5678'),
# MAGIC ('Liam', 'Connor', '111-222-3333', '12 Abbey Street, Dublin, Ireland EU', '111-22-3333'),
# MAGIC ('Sophie', 'Dubois', '222-333-4444', '45 Rue de Rivoli, Paris, France Europe', '222-33-4444'),
# MAGIC ('Hans', 'Müller', '333-444-5555', '78 Berliner Str., Berlin, Germany E.U.', '333-44-5555'),
# MAGIC ('Elena', 'Rossi', '444-555-6666', '23 Via Roma, Milan, Italy Europe', '444-55-6666'),
# MAGIC ('Johan', 'Andersson', '555-666-7777', '56 Drottninggatan, Stockholm, Sweden EU', '555-66-7777');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from profiles

# COMMAND ----------

# MAGIC %md
# MAGIC # Row Level

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION is_not_eu_address(address STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN 
# MAGIC (
# MAGIC     SELECT 
# MAGIC         CASE 
# MAGIC         WHEN LOWER(address) LIKE '%eu%'
# MAGIC           OR LOWER(address) LIKE '%e.u.%'
# MAGIC           OR LOWER(address) LIKE '%europe%'
# MAGIC         THEN FALSE 
# MAGIC         ELSE TRUE 
# MAGIC         END
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE profiles
# MAGIC ALTER COLUMN address 
# MAGIC SET TAGS ("pii_tag"="address");

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Policy

# COMMAND ----------

# %sql
# CREATE OR REPLACE POLICY `eu_policy_not`
# ON SCHEMA `azuredatabricks_catalog`.`gold`
# ROW FILTER `azuredatabricks_catalog`.`gold`.`is_not_eu_address`
# TO `account users`
# EXCEPT `Admins`
# FOR TABLES
# MATCH COLUMNS has_tag_value('pii_tag','address') AS u0
# USING COLUMNS (u0)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM profiles

# COMMAND ----------

# MAGIC %md
# MAGIC # Column Level

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION mask_ssn(ssn STRING)
# MAGIC RETURN "******";

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE profiles
# MAGIC ALTER COLUMN ssn 
# MAGIC SET TAGS ("pii_tag"="ssn");

# COMMAND ----------

# %sql
# CREATE OR REPLACE POLICY `mask_ssn_policy`
# ON CATALOG `azuredatabricks_catalog`
# COLUMN MASK `azuredatabricks_catalog`.`gold`.`mask_ssn`
# TO `account users`
# EXCEPT `Admins`
# FOR TABLES
# MATCH COLUMNS has_tag_value('pii_tag','ssn') AS m
# ON COLUMN m


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM profiles

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE profiles
# MAGIC ALTER COLUMN ssn 
# MAGIC UNSET TAGS ("pii_tag");

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE profiles
# MAGIC ALTER COLUMN address 
# MAGIC UNSET TAGS ("pii_tag");

# COMMAND ----------


# Databricks notebook source
# MAGIC %md
# MAGIC ### check login user

# COMMAND ----------

user = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Executed by: {user}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### select catalog and schema

# COMMAND ----------

spark.sql("USE CATALOG fraud_catalog")
spark.sql("USE SCHEMA silver")



# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG fraud_catalog TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### add schema level permissions

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA ON SCHEMA fraud_catalog.silver 
# MAGIC TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON SCHEMA fraud_catalog.silver 
# MAGIC TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### read table from silver schemas

# COMMAND ----------

df = spark.table("fraud_catalog.silver.banking_silver")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION goldextloc 
# MAGIC TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT WRITE FILES ON EXTERNAL LOCATION goldextloc 
# MAGIC TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION goldextloc
# MAGIC TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA ON SCHEMA fraud_catalog.gold 
# MAGIC TO `akshythorat007@gmail.com`;
# MAGIC
# MAGIC GRANT CREATE TABLE ON SCHEMA fraud_catalog.gold 
# MAGIC TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### add new column which is combination of "first" and "last" column

# COMMAND ----------

from pyspark.sql.functions import concat_ws

gold_df = df.withColumn(
    "full_name",
    concat_ws(" ", "first", "last")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### remove existing unused column

# COMMAND ----------

gold_df = gold_df.drop("first", "last")

# COMMAND ----------

gold_path = "abfss://gold@thorac.dfs.core.windows.net/banking_gold/"

gold_df.write \
.format("delta") \
.mode("overwrite") \
.save(gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### create managed table

# COMMAND ----------

gold_df.write \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable("fraud_catalog.gold.banking_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### create external table

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS fraud_catalog.gold.banking_gold
USING DELTA
LOCATION 'abfss://gold@thorac.dfs.core.windows.net/banking_gold/'
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ### read data from managed table

# COMMAND ----------

df = spark.table("fraud_catalog.gold.banking_gold")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### extract year from "trans_date_trans_time" column

# COMMAND ----------

from pyspark.sql.functions import year

years_df = df.select(year("trans_date_trans_time").alias("year"))

# COMMAND ----------

years_df = years_df.distinct()
years_df.display()

# COMMAND ----------

from pyspark.sql.functions import year

# COMMAND ----------

# MAGIC %md
# MAGIC ### filter data base on year

# COMMAND ----------

df_2019 = df.filter(year("trans_date_trans_time") == 2019)

# COMMAND ----------

df_2020 = df.filter(year("trans_date_trans_time") == 2020)

# COMMAND ----------

# MAGIC %md
# MAGIC ### create seperate table based on year

# COMMAND ----------

# MAGIC %md
# MAGIC year 2019

# COMMAND ----------

df_2019.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.gold.banking_gold_2019")

# COMMAND ----------

gold_path = "abfss://gold@thorac.dfs.core.windows.net/banking_gold_2019_trans/"

df_2019.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.save(gold_path)



# COMMAND ----------

# MAGIC %md
# MAGIC year 2020

# COMMAND ----------

df_2020.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.gold.banking_gold_2020")

# COMMAND ----------

gold_path = "abfss://gold@thorac.dfs.core.windows.net/banking_gold_2020_trans/"

df_2020.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.save(gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC **We can split data into separate tables using filtering, but in production we prefer partitioning by year to improve performance and scalability.**

# COMMAND ----------

spark.table("fraud_catalog.gold.banking_gold_2019").display()


# COMMAND ----------

spark.table("fraud_catalog.gold.banking_gold_2020").display()

# COMMAND ----------

df_2019 = spark.table("fraud_catalog.gold.banking_gold_2019")

df_2019.select("state").distinct().display()

# COMMAND ----------

df_2019 = spark.table("fraud_catalog.gold.banking_gold_2020")

df_2019.select("state").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### filer data base on fraud status

# COMMAND ----------

df_2019 = spark.table("fraud_catalog.gold.banking_gold_2019")
df_2020 = spark.table("fraud_catalog.gold.banking_gold_2020")

# COMMAND ----------

# MAGIC %md
# MAGIC ### extract all fraud and non fraud transcation of year 2019

# COMMAND ----------

fraud_2019 = df_2019.filter("is_fraud = 1")
nonfraud_2019 = df_2019.filter("is_fraud = 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ### create tables based on filter

# COMMAND ----------

fraud_2019.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.gold.transaction_status_2019_fraud")

nonfraud_2019.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.gold.transaction_status_2019_nonfraud")

# COMMAND ----------

gold_path = "abfss://gold@thorac.dfs.core.windows.net/transaction_status_2019_fraud/"

fraud_2019.write \
.format("delta") \
.mode("overwrite") \
.save(gold_path)

# COMMAND ----------

gold_path = "abfss://gold@thorac.dfs.core.windows.net/transaction_status_2019_unfraud/"

nonfraud_2019.write \
.format("delta") \
.mode("overwrite") \
.save(gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### extract all fraud and non fraud transcation of year 2020

# COMMAND ----------

fraud_2020 = df_2020.filter("is_fraud = 1")
nonfraud_2020 = df_2020.filter("is_fraud = 0")

# COMMAND ----------

fraud_2020.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.gold.transaction_status_2020_fraud")

nonfraud_2020.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.gold.transaction_status_2020_nonfraud")

# COMMAND ----------

gold_path = "abfss://gold@thorac.dfs.core.windows.net/transaction_status_2020_fraud/"

fraud_2020.write \
.format("delta") \
.mode("overwrite") \
.save(gold_path)

# COMMAND ----------

gold_path = "abfss://gold@thorac.dfs.core.windows.net/transaction_status_2020_unfraud/"

nonfraud_2020.write \
.format("delta") \
.mode("overwrite") \
.save(gold_path)

# COMMAND ----------

spark.table("fraud_catalog.gold.transaction_status_2019_fraud").display()
spark.table("fraud_catalog.gold.transaction_status_2019_nonfraud").display()



# COMMAND ----------

spark.table("fraud_catalog.gold.transaction_status_2020_fraud").display()
spark.table("fraud_catalog.gold.transaction_status_2020_nonfraud").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > ### check distinct value from "transaction_status_2019_fraud" table

# COMMAND ----------

print("transaction_status_2019_fraud")
spark.table("fraud_catalog.gold.transaction_status_2019_fraud") \
     .select("is_fraud") \
     .distinct() \
     .display()

print("transaction_status_2019_nonfraud")
spark.table("fraud_catalog.gold.transaction_status_2019_nonfraud") \
     .select("is_fraud") \
     .distinct() \
     .display()

# COMMAND ----------

# MAGIC %md
# MAGIC > ### check distinct value from "transaction_status_2020_fraud" table

# COMMAND ----------

print("transaction_status_2020_fraud")
spark.table("fraud_catalog.gold.transaction_status_2020_fraud") \
     .select("is_fraud") \
     .distinct() \
     .display()

print("transaction_status_2020_nonfraud")
spark.table("fraud_catalog.gold.transaction_status_2020_nonfraud") \
     .select("is_fraud") \
     .distinct() \
     .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union Operation

# COMMAND ----------

df_2019_fraud = spark.table("fraud_catalog.gold.transaction_status_2019_fraud")
df_2019_nonfraud = spark.table("fraud_catalog.gold.transaction_status_2019_nonfraud")

df_2020_fraud = spark.table("fraud_catalog.gold.transaction_status_2020_fraud")
df_2020_nonfraud = spark.table("fraud_catalog.gold.transaction_status_2020_nonfraud")

# COMMAND ----------

df = df_2019_fraud.union(df_2019_nonfraud) \
                 .union(df_2020_fraud) \
                 .union(df_2020_nonfraud)

# COMMAND ----------

from pyspark.sql.functions import month, year

df = df.withColumn("year", year("trans_date_trans_time")) \
       .withColumn("month", month("trans_date_trans_time"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### add custom flag column based on transcation amount

# COMMAND ----------

from pyspark.sql.functions import col

df = df.withColumn(
    "high_value_flag",
    (col("amt") > 1000).cast("int")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### windows function to generate insight

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import count

window_spec = Window.partitionBy("cc_num") \
                    .orderBy("trans_date_trans_time") \
                    .rowsBetween(-5, 0)

df = df.withColumn(
    "txn_count_last_5",
    count("*").over(window_spec)
)

df = df.withColumn(
    "multi_txn_flag",
    (col("txn_count_last_5") > 3).cast("int")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### add suspicious_flag flag column based on custom columns (high_value_flag, multi_txn_flag).

# COMMAND ----------

df = df.withColumn(
    "suspicious_flag",
    ((col("high_value_flag") == 1) | (col("multi_txn_flag") == 1)).cast("int")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### create table based on insight

# COMMAND ----------

df.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.gold.fraud_detection_output")

# COMMAND ----------

spark.sql("SELECT * FROM fraud_catalog.gold.fraud_detection_output").display()

# COMMAND ----------


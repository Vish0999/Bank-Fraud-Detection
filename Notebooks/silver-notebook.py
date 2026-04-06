# Databricks notebook source
# MAGIC %md
# MAGIC ### check user id 

# COMMAND ----------

user = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Executed by: {user}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### read data from bronze container

# COMMAND ----------

df = spark.read.format("csv") \
.option("header","true") \
.option("inferSchema","true") \
.load("abfss://bronze-1@thorac.dfs.core.windows.net/credit_card_transactions.csv")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### count the total number of null values per each column

# COMMAND ----------

from pyspark.sql.functions import col, sum

null_df = df.select([
    sum(col(c).isNull().cast("int")).alias(c) for c in df.columns
])

display(null_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### pass constant values to null values 

# COMMAND ----------

df_clean = df.fillna({
"merch_zipcode":111111
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### select unique values from  is_fraud column

# COMMAND ----------

df.select('is_fraud').distinct().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### recheck null vaules per column

# COMMAND ----------

from pyspark.sql.functions import col, sum

null_df = df_clean.select([
    sum(col(c).isNull().cast("int")).alias(c) for c in df.columns
])

display(null_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### select unique values from merchant column

# COMMAND ----------


df.select('merchant').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### rename the column

# COMMAND ----------

df = df.withColumnRenamed("Unnamed: 0", "id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### generate parquet files from csv files

# COMMAND ----------

df.write \
.mode("overwrite") \
.format("parquet") \
.save("abfss://silver@thorac.dfs.core.windows.net/banking_parquet/")

# COMMAND ----------

display(dbutils.fs.ls("abfss://silver@thorac.dfs.core.windows.net/banking_parquet/"))

# COMMAND ----------

files = dbutils.fs.ls("abfss://silver@thorac.dfs.core.windows.net/banking_parquet/")

for f in files:
    print(f.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### generate delta files

# COMMAND ----------

df.write \
.format("delta") \
.mode("overwrite") \
.save("abfss://silver@thorac.dfs.core.windows.net/banking_delta/")

# COMMAND ----------

files = dbutils.fs.ls("abfss://silver@thorac.dfs.core.windows.net/banking_delta/")

for f in files:
    print(f.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### read data from parquet file

# COMMAND ----------

spark.read.format("parquet") \
.load("abfss://silver@thorac.dfs.core.windows.net/banking_parquet/").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### read data from delta file

# COMMAND ----------

spark.read.format("delta") \
.load("abfss://silver@thorac.dfs.core.windows.net/banking_delta/").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### replace special character from each column

# COMMAND ----------

import re

df = df.toDF(*[
    re.sub(r'[^a-zA-Z0-9_]', '_', c) for c in df.columns
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### use catalog and schema to create tables

# COMMAND ----------

spark.sql("USE CATALOG fraud_catalog")
spark.sql("USE SCHEMA silver")



# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT MODIFY ON TABLE fraud_catalog.silver.banking_silver TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create managed table

# COMMAND ----------

df.write.format("delta") \
.mode("overwrite") \
.saveAsTable("fraud_catalog.silver.banking_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create external `table`

# COMMAND ----------

df.write.format("delta") \
.mode("overwrite") \
.option("path","abfss://silver@thorac.dfs.core.windows.net/banking_delta_table/") 


# COMMAND ----------

# MAGIC %md
# MAGIC ### read data from managed `table`

# COMMAND ----------

spark.sql("SELECT * FROM fraud_catalog.silver.banking_silver LIMIT 10").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### optimize managed `table`

# COMMAND ----------


spark.sql("""
OPTIMIZE fraud_catalog.silver.banking_silver
ZORDER BY (is_fraud, trans_date_trans_time)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### read data from managed table

# COMMAND ----------

spark.sql("SELECT * FROM fraud_catalog.silver.banking_silver LIMIT 10").display()

# COMMAND ----------

spark.sql("VACUUM fraud_catalog.silver.banking_silver")

# COMMAND ----------

spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false"); spark.sql("VACUUM fraud_catalog.silver.banking_silver RETAIN 0 HOURS")

# COMMAND ----------

display(dbutils.fs.ls("abfss://silver@thorac.dfs.core.windows.net/banking_delta_table/"))

# COMMAND ----------


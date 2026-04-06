# Databricks notebook source
# MAGIC %md
# MAGIC ### add permission 

# COMMAND ----------

spark.sql("USE CATALOG fraud_catalog")
spark.sql("USE SCHEMA bank_transaction_insight")

# COMMAND ----------

# MAGIC %md
# MAGIC ### read data from "fraud_detection_output" table

# COMMAND ----------

df = spark.table("fraud_catalog.gold.fraud_detection_output")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### use group by to calculate total_transactions per state

# COMMAND ----------

from pyspark.sql.functions import count

count_df = df.groupBy("state", "year", "is_fraud") \
             .agg(count("*").alias("total_transactions"))

count_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### use group by to calculate total_amount per state

# COMMAND ----------

from pyspark.sql.functions import sum

sum_df = df.groupBy("state", "year", "is_fraud") \
           .agg(sum("amt").alias("total_amount"))

sum_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### use group by to calculate avg_amount per state

# COMMAND ----------

from pyspark.sql.functions import avg

avg_df = df.groupBy("state", "year", "is_fraud") \
           .agg(avg("amt").alias("avg_amount"))

avg_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### use group by to calculate max_transcation_amount per state

# COMMAND ----------

from pyspark.sql.functions import max

max_df = df.groupBy("state", "year", "is_fraud") \
           .agg(max("amt").alias("max_transaction_amount"))

max_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### perform aggregation base on cc_num column

# COMMAND ----------

from pyspark.sql.functions import sum, count

cust_df = df.groupBy("cc_num") \
    .agg(
        count("*").alias("total_txn"),
        sum("amt").alias("total_amt"),
        sum("is_fraud").alias("fraud_txn")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### calculate risky customer base on column total_txn, total_amt, fraud_txn

# COMMAND ----------

from pyspark.sql.functions import col

cust_df = cust_df.withColumn(
    "is_risky_cust",
    ((col("total_txn") > 50) | 
     (col("total_amt") > 50000) | 
     (col("fraud_txn") > 0)).cast("int")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### perform join operation

# COMMAND ----------

final_df = df.join(cust_df.select("cc_num", "is_risky_cust"), on="cc_num", how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC ### create managed table transaction_count

# COMMAND ----------

count_df.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.bank_transaction_insight.transaction_count")

# COMMAND ----------

# MAGIC %md
# MAGIC ### create manged table total_amount

# COMMAND ----------

sum_df.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.bank_transaction_insight.total_amount")

# COMMAND ----------

# MAGIC %md
# MAGIC ### create table avg_amount

# COMMAND ----------

avg_df.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.bank_transaction_insight.avg_amount")

# COMMAND ----------

# MAGIC %md
# MAGIC ### create table max_transaction

# COMMAND ----------

max_df.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.bank_transaction_insight.max_transaction")

# COMMAND ----------

# MAGIC %md
# MAGIC ### create table risky_customers

# COMMAND ----------

cust_df.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.bank_transaction_insight.risky_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### create table final_insights

# COMMAND ----------

final_df.write \
.mode("overwrite") \
.saveAsTable("fraud_catalog.bank_transaction_insight.final_insights")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

df = spark.table("fraud_catalog.gold.fraud_detection_output")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Other Windows Function**

# COMMAND ----------

# MAGIC %md
# MAGIC ## **row_number()**

# COMMAND ----------

from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window


window_spec = Window.partitionBy("state").orderBy(col("amt").desc())
df = df.withColumn("row_number_rank", row_number().over(window_spec))


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **dense_rank()**

# COMMAND ----------

df = df.withColumn("dense_rank_amt", dense_rank().over(window_spec))
df.display()

# COMMAND ----------

df.drop("prev_amt").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **lag()**

# COMMAND ----------

window_spec_txn = Window.partitionBy("cc_num").orderBy("trans_date_trans_time")

df = df.withColumn(
    "prev_amt_lag",
    lag("amt").over(window_spec_txn)
)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **lead()**

# COMMAND ----------

df = df.withColumn(
    "next_amt_lead",
    lead("amt").over(window_spec_txn)
)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **sum() OVER()**

# COMMAND ----------

df = df.withColumn(
    "running_total",
    sum("amt").over(window_spec_txn)
)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **OVER()**

# COMMAND ----------

count("*").over(window_spec_txn)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **rangeBetween()**

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp

df = df.withColumn("ts", unix_timestamp("trans_date_trans_time"))

window_range = Window.partitionBy("cc_num") \
                     .orderBy("ts") \
                     .rangeBetween(-3600, 0)   # last 1 hour

df = df.withColumn(
    "txn_last_1hr",
    count("*").over(window_range)
)

# COMMAND ----------

df.display()

# COMMAND ----------

output_path = "abfss://gold@thorac.dfs.core.windows.net/banking_final_insight/"

# COMMAND ----------

# MAGIC %md
# MAGIC Since Spark generates dynamic file names, I designed the pipeline to write data into a fixed folder and configured Power BI to read from the folder instead of a specific file. This ensures seamless refresh even when file names change.

# COMMAND ----------

df.coalesce(1).write \
.mode("overwrite") \
.option("header", "true") \
.csv(output_path)

# COMMAND ----------


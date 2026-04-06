# Databricks notebook source
# MAGIC %sql
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION bronzeextloc TO `akshythorat007@gmail.com`;
# MAGIC GRANT WRITE FILES ON EXTERNAL LOCATION bronzeextloc TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Read data from bronze container**

# COMMAND ----------

df = spark.read.format("csv") \
.option("header","true") \
.option("inferSchema","true") \
.load("abfss://bronze-1@thorac.dfs.core.windows.net/credit_card_transactions.csv")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### print schema of dataset

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### check total number of rows and columns

# COMMAND ----------

row_count = df.count()
print("Total rows:", row_count)
col_count = len(df.columns)
print("Total columns:", col_count)

# COMMAND ----------

df.describe().display()

# COMMAND ----------


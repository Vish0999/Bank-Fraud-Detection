# Databricks notebook source
# MAGIC %md
# MAGIC  This Notebook used to setup envirnoment including catalog, schemas, storage credientals, external locations and this Notebook also used to give grant permission to all account users.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create Catalog : fraud_catalog and schemas**

# COMMAND ----------

catalog = "fraud_catalog"

# ---------- Create Catalog ----------
try:
    spark.sql(f"CREATE CATALOG {catalog}")
    print(f"Catalog '{catalog}' created")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Catalog '{catalog}' already exists")
    else:
        print(f"Error creating catalog: {e}")


# ---------- Function to create schema ----------
def create_schema(schema_name):
    try:
        spark.sql(f"CREATE SCHEMA {catalog}.{schema_name}")
        print(f"Schema '{schema_name}' created")
        
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"Schema '{schema_name}' already exists")
        else:
            print(f"Error creating schema '{schema_name}': {e}")


# ---------- Create Schemas ----------
create_schema("bronze")
create_schema("silver")
create_schema("gold")
print("Catalog setup completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **add/give grant permission to normal user**

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG fraud_catalog TO `akshythorat007@gmail.com`;
# MAGIC GRANT CREATE SCHEMA ON CATALOG fraud_catalog TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **create schema bank_transaction_insight**

# COMMAND ----------

create_schema("bank_transaction_insight")

# COMMAND ----------

spark.sql("SELECT current_catalog()").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create storage_credentials**

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog


w = WorkspaceClient()

try:
    created_credential = w.storage_credentials.create(
        name="vishcred",
        azure_managed_identity=catalog.AzureManagedIdentityRequest(
            access_connector_id="/subscriptions/0a660e89-9062-430b-a5b0-1302c08a1aa4/resourceGroups/vmrg/providers/Microsoft.Databricks/accessConnectors/VishUnityConn"
        ),
        comment="Credential for Azure Data Lake access"
    )
    
    print("Storage credential 'vishcred' created")

except Exception as e:
    if e.error_code == "RESOURCE_ALREADY_EXISTS":
        print("Storage credential 'vishcred' already exists")
    else:
        print(f"Error creating storage credential: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create external Location for bronze container**

# COMMAND ----------

try:
    spark.sql("""
    CREATE EXTERNAL LOCATION bronzeextloc
    URL 'abfss://bronze-1@thorac.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL vishcred)
    """)
    print("External location created")

except Exception as e:
    if "ExternalLocationAlreadyExistsException" in str(e):
        print("external location 'bronzeextloc' already exists")
    else:
        print(f"Error creating external location: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create external Location for silver container**

# COMMAND ----------

try:
    spark.sql("""
    CREATE EXTERNAL LOCATION silverextloc
    URL 'abfss://silver@thorac.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL vishcred)
    """)
    print("External location created")

except Exception as e:
    if "ExternalLocationAlreadyExistsException" in str(e):
        print("external location 'silverextloc' already exists")
    else:
        print(f"Error creating external location: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create external Location for gold container**

# COMMAND ----------

try:
    spark.sql("""
    CREATE EXTERNAL LOCATION goldextloc
    URL 'abfss://gold@thorac.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL vishcred)
    """)
    print("External location created")

except Exception as e:
    if "ExternalLocationAlreadyExistsException" in str(e):
        print("external location 'goldextloc' already exists")
    else:
        print(f"Error creating external location: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **add grant permission **

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION goldextloc TO `akshythorat007@gmail.com`;
# MAGIC GRANT WRITE FILES ON EXTERNAL LOCATION goldextloc TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION silverextloc TO `akshythorat007@gmail.com`;
# MAGIC GRANT WRITE FILES ON EXTERNAL LOCATION silverextloc TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION bronzeextloc TO `akshythorat007@gmail.com`;
# MAGIC GRANT WRITE FILES ON EXTERNAL LOCATION bronzeextloc TO `akshythorat007@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC grant read files
# MAGIC on external location silverextloc
# MAGIC to `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC grant read files
# MAGIC on external location goldextloc
# MAGIC to `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC grant read files
# MAGIC on external location bankinsightextloc
# MAGIC to `account users`;
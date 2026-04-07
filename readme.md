## **Repository Structure :**

```
notebooks/
│
├── catalog_notebook
├── bronze_notebook
├── silver_notebook
├── gold_notebook
├── bank_transaction_insights
│
data/
│
├── transactions.csv

output_screens/
│
├── screenshots
```



## **Banking Fraud Detection Project :**
End-to-end Data Engineering project to detect fraudulent banking transactions using Azure + Spark + Delta Lake architecture + Azure ADF.

## **Project Overview:**

This project builds a Fraud Detection Data Pipeline that:

* Ingests raw banking transactions
* Cleans and transforms data
* Applies fraud detection rules
* Stores curated fraud dataset
* Builds dashboard for insights

Architecture follows Medallion Architecture:

**Bronze → Silver → Gold**

## **Project Architecture :**

```
Source CSV
   ↓
Bronze Layer (Raw Data)
   ↓
Silver Layer (Cleaned Data)
   ↓
Gold Layer (Fraud Rules Applied)
   ↓
Power BI Dashboard
```

## **Pre-requisite / Technologies Used:**

1) Azure Data Lake Storage Gen2
2) Azure Databricks
3) PySpark
5) Power BI
6) Python
7) SQL
8) Unity Catalog
9) Azure ADF

## **Project Structure :**

```
notebooks/
│
├── catalog_notebook
├── bronze_notebook
├── silver_notebook
├── gold_notebook
├── bank_transaction_insights
│
data/
│
├── transactions.csv

output_screens/
│
├── screenshots
```


## **Infrastructure Setup:**
<img src="Out_Screens/infracture.jpg" width="500" height="250"/>


## **Databricks Setup:**
<img src="Out_Screens/dataBricksSetup.png" width="500" height="250"/>

## **Storage account Setup:**
<img src="Out_Screens/Storage-Account-Setup.png" width="500" height="250"/>


## **ADF pipeline Setup:**



## **Notebooks Explaination:**
use of each notebook are as follows:

### **1) Databricks_Setup_Notebook.py:**
* This spark notebook use to setup the required environment in databricks workspace.
* This notebook perform following operations:
   ```
      1) create storage credientails.
      2) create external location.
      3) create catalog.
      4) create schema.
      5) add permissions.
   
   ```

### **2) bronze_Notebook.py:**

* This spark notebook read source raw data from storage account broze container.
* This notebook perform following operations:
   ```
      1) Read the source data.
      2) describe schema of source data.
   ```

### **3) Silver_Notebook.py:**

* This spark notebook perform following operations:
   ```
      1) Read the data from bronze container.
      2) Clean source data.
      3) Check null values.
      4) generate parquet files from clean data.
      5) Transfer parquet file in silver container.
      6) create managed and external tables.
      7) perform optimizations.
   ```
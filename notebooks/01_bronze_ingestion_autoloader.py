# Databricks notebook source
# MAGIC %md
# MAGIC # D_SHOP_FLC: PHASE 1 - BRONZE INGESTION WITH AUTO LOADER
# MAGIC ---
# MAGIC - **Project:** Modern Enterprise Retail Lakehouse & Executive Analytics
# MAGIC - **Engineer:** Data Engineer (Personal Flagship Portfolio)
# MAGIC - **Objectives:**
# MAGIC   - Initialize 3 Medallion architecture schemas (`bronze`, `silver`, `gold`) in Unity Catalog.
# MAGIC   - Clean slate reset: Drop legacy tables and wipe stale checkpoints.
# MAGIC   - Build an incremental ingestion pipeline using Databricks Auto Loader (`cloudFiles`).
# MAGIC   - Standardize column headers to `snake_case` (optimized for Spark Catalyst).
# MAGIC   - Attach technical audit metadata (`_ingested_at`, `_source_file`) for complete Data Lineage.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. INITIALIZE MEDALLION SCHEMAS IN UNITY CATALOG

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ensure the 3 Medallion schemas exist under the workspace catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze
# MAGIC COMMENT 'Bronze layer storing immutable, append-only raw ingestion data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver
# MAGIC COMMENT 'Silver layer containing cleansed, enriched, and standardized datasets';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold
# MAGIC COMMENT 'Gold layer housing star-schema dimensional models optimized for Power BI reporting';
# MAGIC
# MAGIC -- Create isolated volume for streaming checkpoints
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.bronze.checkpoints
# MAGIC COMMENT 'Dedicated storage volume for streaming checkpoints and schemas';
# MAGIC
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. CLEAN SLATE RESET (DROP LEGACY TABLES & STALE CHECKPOINTS)

# COMMAND ----------

# Reset environment for a 100% clean, reproducible fresh run
tables_to_reset = [
    "bronze_customers",
    "bronze_products",
    "bronze_stores",
    "bronze_sales",
    "bronze_exchange_rates"
]

print("[*] Cleaning legacy tables and checkpoints...")
for tbl in tables_to_reset:
    spark.sql(f"DROP TABLE IF EXISTS workspace.bronze.{tbl}")

dbutils.fs.rm("/Volumes/workspace/bronze/checkpoints", True)
dbutils.fs.rm("/Volumes/workspace/bronze/raw_data/_checkpoints", True)
print("[CLEAN SLATE] All legacy Bronze tables and checkpoints successfully cleared!\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. DEFINE REUSABLE PRODUCTION AUTO LOADER FUNCTION

# COMMAND ----------

import pyspark.sql.functions as F

def ingest_to_bronze(file_name: str, table_name: str) -> None:
    """
    Incrementally ingests raw CSV files into Delta Lake Bronze tables using Auto Loader.
    
    :param file_name: Source CSV file name inside Unity Catalog Volume (e.g., 'Products.csv')
    :param table_name: Target Delta table name within Bronze schema (e.g., 'bronze_products')
    """
    raw_source_dir  = "/Volumes/workspace/bronze/raw_data"
    checkpoint_path = f"/Volumes/workspace/bronze/checkpoints/{table_name}"
    schema_path     = f"/Volumes/workspace/bronze/checkpoints/{table_name}_schema"
    target_table    = f"workspace.bronze.{table_name}"
    
    print(f"[*] Starting ingestion: {file_name} -> {target_table} ...")
    
    # 1. Read incremental stream with Auto Loader using pathGlobFilter for specific file
    df_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", file_name)  # Filter for this specific CSV within the directory
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(raw_source_dir)  # Load directory path (Auto Loader requirement)
    )
    
    # 2. Standardize column names in a single pass (Catalyst execution plan optimization)
    df_stream = df_stream.select([
        F.col(c).alias(c.strip().lower().replace(" ", "_")) 
        for c in df_stream.columns
    ])
        
    # 3. Attach technical audit lineage metadata (Unity Catalog standard)
    df_stream = (df_stream
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    
    # 4. Write micro-batch stream to Delta table with Auto Optimize enabled
    query = (df_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    
    query.awaitTermination()
    
    # Audit row count verification
    total_rows = spark.table(target_table).count()
    print(f"[SUCCESS] Table {target_table} loaded successfully! Total rows: {total_rows:,}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. EXECUTE INGESTION FOR ALL 5 CORE DATASETS

# COMMAND ----------

# Core retail dataset mappings: (Source CSV, Target Bronze Table)
datasets = [
    ("Customers.csv",      "bronze_customers"),
    ("Products.csv",       "bronze_products"),
    ("Stores.csv",         "bronze_stores"),
    ("Sales.csv",          "bronze_sales"),
    ("Exchange_Rates.csv", "bronze_exchange_rates")
]

# Sequentially trigger ingestion for each dataset
for file_csv, table_bronze in datasets:
    ingest_to_bronze(file_csv, table_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. DATA AUDIT & INTEGRITY VERIFICATION

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect row counts across all 5 Bronze tables
# MAGIC SELECT 'bronze_customers' AS table_name, COUNT(*) AS total_rows FROM workspace.bronze.bronze_customers
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_products', COUNT(*) FROM workspace.bronze.bronze_products
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_stores', COUNT(*) FROM workspace.bronze.bronze_stores
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_sales', COUNT(*) FROM workspace.bronze.bronze_sales
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_exchange_rates', COUNT(*) FROM workspace.bronze.bronze_exchange_rates;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview first 3 rows of bronze_sales to verify audit metadata columns
# MAGIC SELECT 
# MAGIC     order_number, 
# MAGIC     line_item, 
# MAGIC     order_date, 
# MAGIC     customerkey, 
# MAGIC     storekey, 
# MAGIC     productkey, 
# MAGIC     quantity, 
# MAGIC     currency_code, 
# MAGIC     _ingested_at, 
# MAGIC     _source_file
# MAGIC FROM workspace.bronze.bronze_sales
# MAGIC LIMIT 3;

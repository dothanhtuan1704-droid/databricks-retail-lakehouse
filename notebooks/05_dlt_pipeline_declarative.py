# Databricks notebook source
# MAGIC %md
# MAGIC # D_SHOP_FLC: DECLARATIVE DELTA LIVE TABLES (DLT) PIPELINE
# MAGIC ---
# MAGIC - **Project:** Modern Enterprise Retail Lakehouse & Executive Analytics
# MAGIC - **Component:** Syllabus Session 2.3 - Lakeflow Declarative Pipelines (DLT)
# MAGIC - **Objectives:**
# MAGIC   - Implement an end-to-end declarative pipeline using Delta Live Tables (`dlt`).
# MAGIC   - Enforce rigorous **Data Quality Expectations**:
# MAGIC     - `@dlt.expect`: Non-blocking warning logged in DLT event logs.
# MAGIC     - `@dlt.expect_or_drop`: Automatic row-level filtering of anomalous or invalid records.
# MAGIC     - `@dlt.expect_or_fail`: Pipeline failure trigger on critical integrity breaches.
# MAGIC   - Demonstrate declarative lineage DAG, incremental streaming tables, and materialized views.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. BRONZE LAYER: STREAMING INGESTION FROM VOLUMES
# MAGIC 
# MAGIC > Ingest raw files as append-only streaming tables with schema rescue.

# COMMAND ----------

@dlt.table(
    name="dlt_bronze_sales",
    comment="Raw streaming sales ingestion from bronze landing volumes",
    table_properties={"quality": "bronze"}
)
def dlt_bronze_sales():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/workspace/bronze/landing_zone/sales/")
    )

@dlt.table(
    name="dlt_bronze_customers",
    comment="Raw streaming customer data",
    table_properties={"quality": "bronze"}
)
def dlt_bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/workspace/bronze/landing_zone/customers/")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. SILVER LAYER: CONFORMED TABLES WITH EXPECTATIONS
# MAGIC 
# MAGIC > Apply cleaning, deduplication, and declarative data quality contracts.

# COMMAND ----------

@dlt.table(
    name="dlt_silver_customers",
    comment="Cleansed and deduplicated customer records with demographic profiling",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_customer_key", "CustomerKey IS NOT NULL AND CustomerKey > 0")
@dlt.expect("valid_gender", "Gender IN ('M', 'F')")
def dlt_silver_customers():
    return (
        dlt.read_stream("dlt_bronze_customers")
        .withColumnRenamed("CustomerKey", "customer_key")
        .withColumnRenamed("Name", "name")
        .withColumnRenamed("Gender", "gender")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("Continent", "continent")
        .withColumn("birthday", F.to_date(F.col("Birthday"), "M/d/yyyy"))
        .dropDuplicates(["customer_key"])
    )

@dlt.table(
    name="dlt_silver_sales",
    comment="Cleansed order transactions with currency conversion and delivery SLA",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_number", "Order_Number IS NOT NULL")
@dlt.expect_or_drop("positive_quantity", "Quantity > 0")
@dlt.expect("valid_order_date", "Order_Date IS NOT NULL")
def dlt_silver_sales():
    return (
        dlt.read_stream("dlt_bronze_sales")
        .withColumnRenamed("Order_Number", "order_number")
        .withColumnRenamed("Line_Item", "line_item")
        .withColumn("order_date", F.to_date(F.col("Order_Date"), "yyyy-MM-dd"))
        .withColumn("delivery_date", F.to_date(F.col("Delivery_Date"), "yyyy-MM-dd"))
        .withColumnRenamed("CustomerKey", "customer_key")
        .withColumnRenamed("StoreKey", "store_key")
        .withColumnRenamed("ProductKey", "product_key")
        .withColumn("quantity", F.col("Quantity").cast("integer"))
        .withColumn("delivery_days", F.datediff(F.col("delivery_date"), F.col("order_date")))
        .withColumn("order_status", F.when(F.col("delivery_date").isNull(), "Pending").otherwise("Delivered"))
        .dropDuplicates(["order_number", "line_item"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. GOLD LAYER: MATERIALIZED VIEWS & SERVING AGGREGATES
# MAGIC 
# MAGIC > Materialized business view for sub-second executive reporting.

# COMMAND ----------

@dlt.table(
    name="dlt_gold_daily_sales_summary",
    comment="Executive pre-aggregated daily store performance serving Power BI",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_fail("positive_total_sales", "total_sales_volume >= 0")
def dlt_gold_daily_sales_summary():
    return (
        dlt.read("dlt_silver_sales")
        .groupBy("order_date", "store_key", "order_status")
        .agg(
            F.countDistinct("order_number").alias("total_orders"),
            F.sum("quantity").alias("total_sales_volume"),
            F.avg("delivery_days").alias("avg_delivery_lead_days")
        )
    )

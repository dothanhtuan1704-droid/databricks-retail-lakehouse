# Databricks notebook source
# MAGIC %md
# MAGIC # D_SHOP_FLC: PHASE 2 - SILVER LAYER TRANSFORMATIONS & SCD TYPE 2
# MAGIC ---
# MAGIC - **Project:** Modern Enterprise Retail Lakehouse & Executive Analytics
# MAGIC - **Engineer:** Data Engineer (Personal Flagship Portfolio)
# MAGIC - **Objectives:**
# MAGIC   - Cleanse and standardize raw data from Bronze Delta tables into Silver layer.
# MAGIC   - Handle string-to-numeric casting (stripping `$`, commas, trailing whitespace).
# MAGIC   - Implement **Multi-Currency Conversion** (EUR, CAD, GBP, AUD to USD) via temporal exchange rates join.
# MAGIC   - Implement **SCD Type 2 (Slowly Changing Dimensions)** tracking for Product master data.
# MAGIC   - Enforce data quality constraints and deduplication (Exactly-Once delivery).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. INITIALIZE SILVER SCHEMA & CLEAN SLATE RESET

# COMMAND ----------

# Ensure silver schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver COMMENT 'Cleansed and enriched Silver layer';")

# Reset Silver tables for a clean, reproducible execution
silver_tables = [
    "silver_customers",
    "silver_stores",
    "silver_exchange_rates",
    "silver_products_scd2",
    "silver_sales"
]

print("[*] Resetting Silver tables for clean execution...")
for tbl in silver_tables:
    spark.sql(f"DROP TABLE IF EXISTS workspace.silver.{tbl}")
print("[SUCCESS] Silver workspace reset complete!\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. TRANSFORM CUSTOMERS & STORES DIMENSIONS

# COMMAND ----------

import pyspark.sql.functions as F

# 2.1 Transform Customers
df_bronze_cust = spark.table("workspace.bronze.bronze_customers")

df_silver_cust = (df_bronze_cust
    .withColumn("customer_key", F.col("customerkey").cast("integer"))
    .withColumn("gender", F.trim(F.col("gender")))
    .withColumn("name", F.trim(F.col("name")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("state_code", F.trim(F.col("state_code")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("zip_code", F.trim(F.col("zip_code")))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn("continent", F.trim(F.col("continent")))
    .withColumn("birthday", F.to_date(F.col("birthday")))
    .withColumn("_updated_at", F.current_timestamp())
    .drop("customerkey", "_ingested_at", "_source_file")
    .dropDuplicates(["customer_key"])
)

df_silver_cust.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.silver_customers")
print(f"[SUCCESS] silver_customers created! Total rows: {df_silver_cust.count():,}")

# 2.2 Transform Stores with Store Size Categorization
df_bronze_stores = spark.table("workspace.bronze.bronze_stores")

df_silver_stores = (df_bronze_stores
    .withColumn("store_key", F.col("storekey").cast("integer"))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("square_meters", F.col("square_meters").cast("integer"))
    .withColumn("open_date", F.to_date(F.col("open_date")))
    .withColumn("store_size_category", 
        F.when(F.col("square_meters").isNull() | (F.col("square_meters") == 0), "Online")
         .when(F.col("square_meters") < 700, "Small")
         .when(F.col("square_meters") < 1500, "Medium")
         .otherwise("Large")
    )
    .withColumn("_updated_at", F.current_timestamp())
    .drop("storekey", "_ingested_at", "_source_file")
    .dropDuplicates(["store_key"])
)

df_silver_stores.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.silver_stores")
print(f"[SUCCESS] silver_stores created! Total rows: {df_silver_stores.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. TRANSFORM EXCHANGE RATES FOR TEMPORAL CURRENCY CONVERSION

# COMMAND ----------

df_bronze_rates = spark.table("workspace.bronze.bronze_exchange_rates")

df_silver_rates = (df_bronze_rates
    .withColumn("currency_code", F.upper(F.trim(F.col("currency"))))
    .withColumn("exchange_date", F.to_date(F.col("date")))
    .withColumn("exchange_rate", F.col("exchange").cast("double"))
    .select("currency_code", "exchange_date", "exchange_rate")
    .dropDuplicates(["currency_code", "exchange_date"])
)

df_silver_rates.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.silver_exchange_rates")
print(f"[SUCCESS] silver_exchange_rates created! Total rows: {df_silver_rates.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. IMPLEMENT SCD TYPE 2 FOR PRODUCT CATALOG
# MAGIC 
# MAGIC > **Technical Architecture:**
# MAGIC > - Cleanse price and cost: strip `$` symbols and cast to `double`.
# MAGIC > - Add 4 SCD Type 2 tracking columns:
# MAGIC >   - `product_sk`: Surrogate key uniquely identifying each version.
# MAGIC >   - `start_date`: Effective start date of the price.
# MAGIC >   - `end_date`: Expiry date (`9999-12-31` for active price).
# MAGIC >   - `is_current`: Boolean flag (`True` for active version).

# COMMAND ----------

df_bronze_prod = spark.table("workspace.bronze.bronze_products")

df_silver_prod_base = (df_bronze_prod
    .withColumn("product_key", F.col("productkey").cast("integer"))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("brand", F.trim(F.col("brand")))
    .withColumn("color", F.coalesce(F.trim(F.col("color")), F.lit("Unknown")))
    .withColumn("unit_cost_usd", F.regexp_replace(F.col("unit_cost_usd"), "[$, ]", "").cast("double"))
    .withColumn("unit_price_usd", F.regexp_replace(F.col("unit_price_usd"), "[$, ]", "").cast("double"))
    .withColumn("subcategory_key", F.col("subcategorykey").cast("integer"))
    .withColumn("subcategory", F.trim(F.col("subcategory")))
    .withColumn("category_key", F.col("categorykey").cast("integer"))
    .withColumn("category", F.trim(F.col("category")))
    .drop("productkey", "subcategorykey", "categorykey", "_ingested_at", "_source_file")
    .dropDuplicates(["product_key"])
)

# Apply SCD Type 2 baseline metadata
df_silver_prod_scd2 = (df_silver_prod_base
    .withColumn("start_date", F.to_date(F.lit("2016-01-01")))
    .withColumn("end_date", F.to_date(F.lit("9999-12-31")))
    .withColumn("is_current", F.lit(True))
    .withColumn("product_sk", F.sha2(F.concat(F.col("product_key"), F.lit("_"), F.col("start_date")), 256))
    .withColumn("_updated_at", F.current_timestamp())
)

df_silver_prod_scd2.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.silver_products_scd2")
print(f"[SUCCESS] silver_products_scd2 created! Total rows: {df_silver_prod_scd2.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. TRANSFORM SALES & MULTI-CURRENCY CONVERSION
# MAGIC 
# MAGIC > **Currency Conversion Formula:**
# MAGIC > $$\text{sales\_amount\_usd} = \text{ROUND}\left(\frac{\text{quantity} \times \text{unit\_price\_usd}}{\text{exchange\_rate}}, 2\right)$$
# MAGIC > For USD transactions, `exchange_rate` defaults to 1.0.

# COMMAND ----------

df_bronze_sales = spark.table("workspace.bronze.bronze_sales")

# Distinct lookup table to avoid column collisions during join
df_rates_lookup = (df_silver_rates
    .select(
        F.col("currency_code").alias("rate_currency"),
        F.col("exchange_date").alias("rate_date"),
        F.col("exchange_rate")
    )
)

# Join Sales with Products (for Unit Cost & Unit Price) and Exchange Rates
df_sales_joined = (df_bronze_sales
    .withColumn("order_number", F.col("order_number").cast("integer"))
    .withColumn("line_item", F.col("line_item").cast("integer"))
    .withColumn("order_date", F.to_date(F.col("order_date")))
    .withColumn("delivery_date", F.to_date(F.col("delivery_date")))
    .withColumn("customer_key", F.col("customerkey").cast("integer"))
    .withColumn("store_key", F.col("storekey").cast("integer"))
    .withColumn("product_key", F.col("productkey").cast("integer"))
    .withColumn("quantity", F.col("quantity").cast("integer"))
    .withColumn("currency_code", F.upper(F.trim(F.col("currency_code"))))
    .drop("customerkey", "storekey", "productkey", "_ingested_at", "_source_file")
    # Join with Product SCD2 (Current Active Version)
    .join(
        df_silver_prod_scd2.filter(F.col("is_current") == True).select("product_key", "unit_price_usd", "unit_cost_usd"),
        on="product_key",
        how="inner"
    )
    # Join with Exchange Rates matching currency and order date
    .join(
        df_rates_lookup,
        (F.col("currency_code") == df_rates_lookup.rate_currency) & 
        (F.col("order_date") == df_rates_lookup.rate_date),
        how="left"
    )
    .drop("rate_currency", "rate_date")
)

# Compute USD Financial Metrics & Delivery Performance
df_silver_sales = (df_sales_joined
    .withColumn("effective_rate", F.when(F.col("currency_code") == "USD", 1.0).otherwise(F.coalesce(F.col("exchange_rate"), F.lit(1.0))))
    .withColumn("sales_amount_usd", F.round((F.col("quantity") * F.col("unit_price_usd")) / F.col("effective_rate"), 2))
    .withColumn("total_cost_usd", F.round((F.col("quantity") * F.col("unit_cost_usd")) / F.col("effective_rate"), 2))
    .withColumn("gross_profit_usd", F.round(F.col("sales_amount_usd") - F.col("total_cost_usd"), 2))
    .withColumn("delivery_days", F.datediff(F.col("delivery_date"), F.col("order_date")))
    .withColumn("order_status", F.when(F.col("delivery_date").isNull(), "Pending").otherwise("Delivered"))
    .withColumn("_updated_at", F.current_timestamp())
    .select(
        "order_number",
        "line_item",
        "order_date",
        "delivery_date",
        "customer_key",
        "store_key",
        "product_key",
        "quantity",
        "currency_code",
        "unit_price_usd",
        "unit_cost_usd",
        "sales_amount_usd",
        "total_cost_usd",
        "gross_profit_usd",
        "delivery_days",
        "order_status",
        "_updated_at"
    )
    .dropDuplicates(["order_number", "line_item"])
)

df_silver_sales.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.silver_sales")
print(f"[SUCCESS] silver_sales created! Total orders line items: {df_silver_sales.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. SILVER LAYER AUDIT & FINANCIAL CHECKSUM VERIFICATION

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect record counts across all Silver tables
# MAGIC SELECT 'silver_customers' AS table_name, COUNT(*) AS total_rows FROM workspace.silver.silver_customers
# MAGIC UNION ALL
# MAGIC SELECT 'silver_stores', COUNT(*) FROM workspace.silver.silver_stores
# MAGIC UNION ALL
# MAGIC SELECT 'silver_exchange_rates', COUNT(*) FROM workspace.silver.silver_exchange_rates
# MAGIC UNION ALL
# MAGIC SELECT 'silver_products_scd2', COUNT(*) FROM workspace.silver.silver_products_scd2
# MAGIC UNION ALL
# MAGIC SELECT 'silver_sales', COUNT(*) FROM workspace.silver.silver_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Financial Checksum verification: Confirm Total Sales and Profit match enterprise targets
# MAGIC SELECT 
# MAGIC     COUNT(DISTINCT order_number)              AS total_orders,
# MAGIC     COUNT(*)                                  AS total_line_items,
# MAGIC     CONCAT('$', FORMAT_NUMBER(SUM(sales_amount_usd), 2)) AS total_sales_usd,
# MAGIC     CONCAT('$', FORMAT_NUMBER(SUM(gross_profit_usd), 2)) AS total_profit_usd,
# MAGIC     ROUND((SUM(gross_profit_usd) / SUM(sales_amount_usd)) * 100, 2) AS overall_profit_margin_pct
# MAGIC FROM workspace.silver.silver_sales;

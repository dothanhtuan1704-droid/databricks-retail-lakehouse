# Databricks notebook source
# MAGIC %md
# MAGIC # D_SHOP_FLC: PHASE 4 - AUTOMATED DATA QUALITY & END-TO-END RECONCILIATION
# MAGIC ---
# MAGIC - **Project:** Modern Enterprise Retail Lakehouse & Executive Analytics
# MAGIC - **Engineer:** Data Engineer (Personal Flagship Portfolio)
# MAGIC - **Objectives:**
# MAGIC   - Execute automated End-to-End (E2E) row count & financial checksum reconciliation across Bronze, Silver, and Gold.
# MAGIC   - Verify Star Schema **Foreign Key Referential Integrity** (Zero orphan transactions).
# MAGIC   - Enforce **Business Domain Constraints** (Non-negative prices, logical delivery dates, valid margin).
# MAGIC   - Inspect **Delta Lake ACID Lineage** & **Liquid Clustering** table statistics.
# MAGIC   - Output an enterprise-grade **Automated Quality Audit Report** verifying lakehouse production-readiness.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. INITIALIZE DATA QUALITY SUITE & AUDIT LOGGING

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime

audit_results = []

def record_test(suite_name, test_name, status, expected_val, actual_val, details=""):
    """Appends automated test assertion outcomes into the consolidated audit suite."""
    audit_results.append({
        "suite": suite_name,
        "test_name": test_name,
        "status": status,
        "expected": str(expected_val),
        "actual": str(actual_val),
        "details": details,
        "checked_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    })
    badge = "[PASS]" if status == "PASSED" else "[FAIL]"
    print(f"{badge} {suite_name} :: {test_name} -> Expected: {expected_val} | Actual: {actual_val}")

print("[*] Initializing Lakehouse Automated Quality & Reconciliation Test Suite...\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. TEST SUITE 1: MEDALLION LAYER VOLUME RECONCILIATION
# MAGIC 
# MAGIC > **Volume Invariance Audit:**
# MAGIC > Verifies that zero records were unintentionally dropped between Bronze ingestion, Silver cleansing, and Gold dimensionalization.

# COMMAND ----------

# 2.1 Sales Volume Invariance
bronze_sales_count = spark.table("workspace.bronze.bronze_sales").count()
silver_sales_count = spark.table("workspace.silver.silver_sales").count()
gold_sales_count = spark.table("workspace.gold.fact_sales").count()

record_test(
    suite_name="Volume Reconciliation",
    test_name="Sales Line Items Consistency (Bronze vs Silver vs Gold)",
    status="PASSED" if (bronze_sales_count == silver_sales_count == gold_sales_count == 62884) else "FAILED",
    expected_val="62,884",
    actual_val=f"Bronze: {bronze_sales_count:,} | Silver: {silver_sales_count:,} | Gold: {gold_sales_count:,}",
    details="Guarantees exactly-once processing with zero data loss across Medallion layers"
)

# 2.2 Dimension Table Row Count Assertions
gold_stores_count = spark.table("workspace.gold.dim_store").count()
record_test(
    suite_name="Volume Reconciliation",
    test_name="Store Dimension Count",
    status="PASSED" if gold_stores_count == 67 else "FAILED",
    expected_val="67",
    actual_val=str(gold_stores_count),
    details="Stores catalog matches physical retail stores + Online store (Store 0)"
)

gold_customers_count = spark.table("workspace.gold.dim_customer").count()
record_test(
    suite_name="Volume Reconciliation",
    test_name="Customer Dimension Count",
    status="PASSED" if gold_customers_count > 0 else "FAILED",
    expected_val="> 0 (Cleansed Master)",
    actual_val=f"{gold_customers_count:,}",
    details="Unique customer profiles available for Customer 360 profiling"
)

gold_dates_count = spark.table("workspace.gold.dim_date").count()
record_test(
    suite_name="Volume Reconciliation",
    test_name="Date Dimension Spine Range (2015 to 2022)",
    status="PASSED" if gold_dates_count == 2922 else "FAILED",
    expected_val="2,922 days",
    actual_val=f"{gold_dates_count:,} days",
    details="Continuous enterprise calendar for Power BI Time Intelligence"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. TEST SUITE 2: EXECUTIVE FINANCIAL CHECKSUM VERIFICATION
# MAGIC 
# MAGIC > **Ground-Truth Financial Audit:**
# MAGIC > Validates core revenue, COGS, gross margin, and distinct orders against enterprise baseline KPIs.

# COMMAND ----------

financial_kpis = spark.sql("""
SELECT 
    COUNT(DISTINCT order_number)   AS total_orders,
    ROUND(SUM(sales_amount_usd), 2) AS total_sales,
    ROUND(SUM(total_cost_usd), 2)   AS total_cogs,
    ROUND(SUM(gross_profit_usd), 2) AS total_profit,
    ROUND((SUM(gross_profit_usd) / SUM(sales_amount_usd)) * 100, 2) AS margin_pct
FROM workspace.gold.fact_sales
""").collect()[0]

# Check distinct order count
record_test(
    suite_name="Financial Integrity",
    test_name="Distinct Orders Count",
    status="PASSED" if financial_kpis["total_orders"] == 26326 else "FAILED",
    expected_val="26,326",
    actual_val=f"{financial_kpis['total_orders']:,}",
    details="Total unique retail and e-commerce orders placed"
)

# Check total revenue (~$57.53M)
record_test(
    suite_name="Financial Integrity",
    test_name="Total Converted Sales (USD)",
    status="PASSED" if abs(financial_kpis["total_sales"] - 57531338.52) < 1.0 else "FAILED",
    expected_val="$57,531,338.52",
    actual_val=f"${financial_kpis['total_sales']:,.2f}",
    details="Global multi-currency revenue normalized to USD via daily exchange rates"
)

# Check profit margin percentage (~58.57%)
record_test(
    suite_name="Financial Integrity",
    test_name="Gross Profit Margin (%)",
    status="PASSED" if abs(financial_kpis["margin_pct"] - 58.57) < 0.1 else "FAILED",
    expected_val="58.57%",
    actual_val=f"{financial_kpis['margin_pct']}%",
    details="Reflects high-margin electronics and consumer tech catalog"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. TEST SUITE 3: STAR SCHEMA FOREIGN KEY REFERENTIAL INTEGRITY
# MAGIC 
# MAGIC > **Orphan Record Detection:**
# MAGIC > In dimensional modeling, a Fact record with an invalid Foreign Key breaks Power BI relationships.
# MAGIC > Orphan count must strictly be ZERO for all conformed dimensions.

# COMMAND ----------

# 4.1 Check Store FK Integrity
orphan_stores = spark.sql("""
SELECT COUNT(*) AS orphan_count
FROM workspace.gold.fact_sales f
LEFT JOIN workspace.gold.dim_store d ON f.store_key = d.store_key
WHERE d.store_key IS NULL
""").collect()[0]["orphan_count"]

record_test(
    suite_name="Referential Integrity",
    test_name="Store FK Integrity (Zero Orphans)",
    status="PASSED" if orphan_stores == 0 else "FAILED",
    expected_val="0 orphans",
    actual_val=f"{orphan_stores} orphans",
    details="All sales transactions map to valid retail or online store dimensions"
)

# 4.2 Check Customer FK Integrity
orphan_customers = spark.sql("""
SELECT COUNT(*) AS orphan_count
FROM workspace.gold.fact_sales f
LEFT JOIN workspace.gold.dim_customer d ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL
""").collect()[0]["orphan_count"]

record_test(
    suite_name="Referential Integrity",
    test_name="Customer FK Integrity (Zero Orphans)",
    status="PASSED" if orphan_customers == 0 else "FAILED",
    expected_val="0 orphans",
    actual_val=f"{orphan_customers} orphans",
    details="All transactions link to conformed customer dimension keys"
)

# 4.3 Check Product FK Integrity
orphan_products = spark.sql("""
SELECT COUNT(*) AS orphan_count
FROM workspace.gold.fact_sales f
LEFT JOIN workspace.gold.dim_product d ON f.product_key = d.product_key
WHERE d.product_key IS NULL
""").collect()[0]["orphan_count"]

record_test(
    suite_name="Referential Integrity",
    test_name="Product FK Integrity (Zero Orphans)",
    status="PASSED" if orphan_products == 0 else "FAILED",
    expected_val="0 orphans",
    actual_val=f"{orphan_products} orphans",
    details="All sales lines reference existing product catalog definitions"
)

# 4.4 Check Date FK Integrity
orphan_dates = spark.sql("""
SELECT COUNT(*) AS orphan_count
FROM workspace.gold.fact_sales f
LEFT JOIN workspace.gold.dim_date d ON f.order_date = d.full_date
WHERE d.full_date IS NULL
""").collect()[0]["orphan_count"]

record_test(
    suite_name="Referential Integrity",
    test_name="Date FK Integrity (Zero Orphans)",
    status="PASSED" if orphan_dates == 0 else "FAILED",
    expected_val="0 orphans",
    actual_val=f"{orphan_dates} orphans",
    details="All order dates fall inside enterprise date dimension boundaries"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. TEST SUITE 4: DOMAIN DATA QUALITY CONSTRAINTS
# MAGIC 
# MAGIC > **Business Rule Validation:**
# MAGIC > - No negative quantities or negative pricing.
# MAGIC > - Logical fulfillment: Delivery Date must never precede Order Date (`delivery_date >= order_date`).
# MAGIC > - Primary key uniqueness: `(order_number, line_item)` composite key must be 100% unique.

# COMMAND ----------

# 5.1 Primary Key Uniqueness
pk_duplicates = spark.sql("""
SELECT COUNT(*) AS duplicate_pks
FROM (
    SELECT order_number, line_item, COUNT(*) as cnt
    FROM workspace.gold.fact_sales
    GROUP BY order_number, line_item
    HAVING COUNT(*) > 1
)
""").collect()[0]["duplicate_pks"]

record_test(
    suite_name="Domain Constraints",
    test_name="Primary Key Uniqueness (order_number + line_item)",
    status="PASSED" if pk_duplicates == 0 else "FAILED",
    expected_val="0 duplicates",
    actual_val=f"{pk_duplicates} duplicates",
    details="No duplicate line items exist in fact table"
)

# 5.2 Pricing & Financial Sanity Constraints
negative_metrics = spark.sql("""
SELECT COUNT(*) AS invalid_count
FROM workspace.gold.fact_sales
WHERE quantity <= 0 
   OR unit_price_usd <= 0 
   OR unit_cost_usd <= 0 
   OR sales_amount_usd < 0
""").collect()[0]["invalid_count"]

record_test(
    suite_name="Domain Constraints",
    test_name="Positive Quantities & Pricing Sanity",
    status="PASSED" if negative_metrics == 0 else "FAILED",
    expected_val="0 invalid records",
    actual_val=f"{negative_metrics} invalid records",
    details="Guarantees valid economic units across all rows"
)

# 5.3 Order Fulfillment Temporal Logic
delivery_anomalies = spark.sql("""
SELECT COUNT(*) AS anomaly_count
FROM workspace.gold.fact_sales
WHERE delivery_date IS NOT NULL AND delivery_date < order_date
""").collect()[0]["anomaly_count"]

record_test(
    suite_name="Domain Constraints",
    test_name="Fulfillment Logic (delivery_date >= order_date)",
    status="PASSED" if delivery_anomalies == 0 else "FAILED",
    expected_val="0 anomalies",
    actual_val=f"{delivery_anomalies} anomalies",
    details="Fulfillment timeline preserves chronological integrity"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. TEST SUITE 5: DELTA LAKE ACID & LIQUID CLUSTERING VERIFICATION

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect Delta Lake ACID transaction history on Gold Fact Sales
# MAGIC DESCRIBE HISTORY workspace.gold.fact_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Liquid Clustering configuration and table optimization status
# MAGIC DESCRIBE DETAIL workspace.gold.fact_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. EXECUTIVE AUTOMATED UAT QUALITY REPORT

# COMMAND ----------

import pandas as pd

df_audit = spark.createDataFrame(audit_results)
df_audit.createOrReplaceTempView("tv_audit_summary")

print("=" * 80)
print(" D_SHOP_FLC: AUTOMATED LAKEHOUSE QUALITY AUDIT SUMMARY")
print("=" * 80)

total_tests = len(audit_results)
passed_tests = sum(1 for t in audit_results if t["status"] == "PASSED")
failed_tests = total_tests - passed_tests

print(f"Total Tests Executed: {total_tests}")
print(f"Passed: {passed_tests} | Failed: {failed_tests}")
print(f"Overall Lakehouse Health Score: {(passed_tests / total_tests) * 100:.1f}%\n")

if failed_tests == 0:
    print(">>> [VERDICT: PRODUCTION-READY] All architectural, referential, and financial assertions PASSED successfully!")
else:
    raise AssertionError(f"Quality Gate Tripped! {failed_tests} tests failed. Pipeline halted to prevent downstream BI corruption.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visual Interactive Audit Report
# MAGIC SELECT 
# MAGIC     suite,
# MAGIC     test_name,
# MAGIC     status,
# MAGIC     expected,
# MAGIC     actual,
# MAGIC     details,
# MAGIC     checked_at
# MAGIC FROM tv_audit_summary
# MAGIC ORDER BY suite, test_name;

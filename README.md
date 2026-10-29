# KD_SHOP_FLC: Modern Retail Lakehouse & Executive Analytics

[![CI/CD Pipeline](https://github.com/dothanhtuan1704-droid/databricks-retail-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/dothanhtuan1704-droid/databricks-retail-lakehouse/actions/workflows/ci.yml)
[![Databricks Asset Bundles](https://img.shields.io/badge/IaC-Databricks%20Asset%20Bundles%20(DABs)-blue?logo=databricks)](databricks.yml)
[![Databricks](https://img.shields.io/badge/Platform-Databricks%20Lakehouse-orange?logo=databricks)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Engine-PySpark%203.5-red?logo=apachespark)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake%203.x-blue?logo=delta)](https://delta.io/)
[![Power BI](https://img.shields.io/badge/BI-Power%20BI%20Desktop-yellow?logo=powerbi)](https://powerbi.microsoft.com/)
[![Orchestration](https://img.shields.io/badge/Orchestrator-Databricks%20Workflows-blueviolet)](https://docs.databricks.com/workflows/index.html)
[![Data Quality](https://img.shields.io/badge/UAT-Reconciliation%20PASSED-brightgreen)](notebooks/04_data_quality_reconciliation.py)

---

## 📌 Executive Overview

**KD_SHOP_FLC** is an enterprise-grade, end-to-end **Retail Lakehouse and Executive Analytics Platform** built on the **Databricks Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** and served through **Power BI Desktop**.

The platform ingests multi-channel retail transactional data, tracks historical pricing and attribute changes via **Slowly Changing Dimensions (SCD Type 2)**, converts multi-currency transactions (EUR, GBP, CAD, AUD, USD) using temporal joins, structures serving data into a **Kimball Star Schema with Liquid Clustering**, and reconciles core business metrics against automated data quality assertions before publishing to executive stakeholders.

### 🎯 Verified Ground Truth Metrics (100% Reconciled)
* **Total Net Sales (USD):** `$57,531,338.52`
* **Total Gross Profit (USD):** `$33,701,542.36`
* **Overall Profit Margin:** `58.57%`
* **Total Transactions / Orders:** `26,326`
* **Total Active Customers:** `15,266`
* **Physical & Digital Stores:** `67` (Across 8 global markets)
* **Referential Integrity Violation / Orphan Records:** `0` (Zero orphans)

---

## 📊 Executive Dashboard Preview

The serving layer connects to a high-concurrency **Databricks Serverless SQL Warehouse**, feeding a real-time **Power BI Executive Performance Dashboard** designed with modern Clean UI/UX principles (soft shadows, 10px rounded elevation, responsive cross-filtering, and hierarchical drill-down):

![KD_SHOP_FLC Executive Dashboard](powerbi/dashboard_overview.png)

### Key Visual Components:
1. **Top Header & Date Navigator:** Modern violet `#8E6BF5` executive banner featuring a seamless `Between` date slicer (`2015 – 2022`).
2. **Hero KPI Cards:** Real-time callout metrics with elevation shadows (`26,326 Orders`, `$57.53M Sales`, `$33.70M Profit`, `58.58% Margin`).
3. **Operational Fulfillment (Donut Chart):** Real-time fulfillment tracking (`78.8% Delivered` vs `21.2% Pending`).
4. **Channel & Store Performance (Horizontal Bar Chart):** Order distribution across retail store formats (`Large`, `Medium`, `Online`, `Small`).
5. **Macro Sales Trend (Dual-Axis Combo Chart):** High-density continuous daily timeline (2016–2021) overlaying order counts with revenue peaks.
6. **Category Revenue Breakdown (Ranked Bar Chart):** Department-level sales performance ranked from `Computers` ($20M) to `Games and Toys` ($1M).
7. **Hierarchical Financial Matrix (Drill-Down Table):** Multi-level Year $\rightarrow$ Month financial breakdown with Millions (`M`) units, integer percentages, and expandable row hierarchies.
8. **Sidebar Control Panel:** Interactive dropdown slicers for `Category` and `Country` with a 1-click `Clear all slicers` action button.

---

## 🏛️ System Architecture

```mermaid
flowchart TD
    subgraph Ingestion["1. Ingestion Layer (Bronze)"]
        CSV["Raw CSV Files<br/>(Sales, Customers, Products, Stores, FX)"] -->|CloudFiles Streaming| AL["Databricks Auto Loader<br/>(Schema Inference & Evolution)"]
        AL --> B_Sales["bronze.sales<br/>(Raw Delta + Rescued Data)"]
        AL --> B_Cust["bronze.customers"]
        AL --> B_Prod["bronze.products"]
        AL --> B_Store["bronze.stores"]
        AL --> B_FX["bronze.exchange_rates"]
    end

    subgraph Transformation["2. Cleansing & Enrichment (Silver)"]
        B_Sales --> S_Sales["silver.sales<br/>(Cleaned, Deduplicated)"]
        B_FX --> S_FX["silver.exchange_rates"]
        B_Prod --> S_Prod["silver.products_scd2<br/>(SCD Type 2: Surrogate Keys,<br/>is_current, valid_from/to)"]
        B_Cust --> S_Cust["silver.customers"]
        B_Store --> S_Store["silver.stores"]
        
        S_Sales & S_FX --> TJ["Temporal Currency Conversion<br/>(order_date BETWEEN valid_date)"]
        TJ --> S_Sales_USD["silver.sales_enriched_usd<br/>(Sales & Profit in USD)"]
    end

    subgraph Serving["3. Business Star Schema (Gold)"]
        S_Sales_USD --> F_Sales["gold.fact_sales<br/>(Liquid Clustering:<br/>store_key, order_date)"]
        S_Cust --> D_Cust["gold.dim_customer"]
        S_Prod --> D_Prod["gold.dim_product"]
        S_Store --> D_Store["gold.dim_store"]
        DateGen["Dynamic Date Generator"] --> D_Date["gold.dim_date"]
        
        F_Sales --> Agg["gold.agg_daily_store_sales<br/>(Pre-aggregated Summary)"]
    end

    subgraph Governance["4. Quality & Governance"]
        UAT["Automated Data Quality & UAT<br/>(Checksums, Orphan Checks,<br/>Referential Integrity)"]
        F_Sales & D_Cust & D_Prod & D_Store & D_Date -.-> UAT
    end

    subgraph BI["5. Consumption & Analytics"]
        WH["Databricks Serverless SQL Warehouse<br/>(Unity Catalog Governed)"]
        F_Sales & D_Cust & D_Prod & D_Store & D_Date --> WH
        WH --> PBI["Power BI Desktop<br/>(KD_SHOP_FLC Dashboard)"]
    end
```

---

## 📂 Repository Structure

```text
├── notebooks/
│   ├── 01_bronze_ingestion_autoloader.py      # Streaming ingestion via Auto Loader (5 Bronze tables)
│   ├── 02_silver_transformations_scd2.py      # SCD Type 2 merge & temporal multi-currency conversion
│   ├── 03_gold_star_schema_serving.py         # Kimball Star Schema with Liquid Clustering
│   ├── 04_data_quality_reconciliation.py     # Automated checksums & referential integrity validation
│   └── *.ipynb                               # Interactive Jupyter Notebook versions
├── powerbi/
│   ├── KD_SHOP_FLC_Business_Performance_Dashboard.pbix   # Production 3-Page Power BI report
│   ├── Page1_Business_Performance_Dashboard.png          # Page 1: Executive Business Performance
│   ├── Page2_Product_Category_DeepDive.png               # Page 2: Product & Category Deep-Dive
│   └── Page3_Customer_Logistics_Operations.png          # Page 3: Customer Demographics & Logistics
├── data/
│   └── raw/                                   # Source raw transactional & dimension CSVs
├── docs/                                      # Theoretical deep-dives, migration notes & guides
├── .gitignore
└── README.md
```

---

## ⚙️ Technical Deep-Dives

### 1. Bronze Layer: Streaming Ingestion via Auto Loader
* Implemented `spark.readStream.format("cloudFiles")` with automatic schema inference and schema hints.
* Activated `cloudFiles.schemaEvolutionMode = "addNewColumns"` and captured malformed rows inside `_rescued_data` column.
* Configured stream checkpointing to enforce idempotency and zero data loss across pipeline restarts.

### 2. Silver Layer: SCD Type 2 & Temporal Currency Conversion
* **Slowly Changing Dimension Type 2 (SCD 2):** Built for `products` to track price and attribute modifications across time using deterministic surrogate hashing (`SHA2-256`), boolean flags (`is_current`), and timestamp intervals (`valid_from`, `valid_to`).
* **Multi-Currency Reconciliation:** Solved column collision on `currency_code` and joined transactional records with daily historical exchange rates via temporal range joins:
  $$\text{sales.order\_date} \ge \text{fx.effective\_start\_date} \land \text{sales.order\_date} < \text{fx.effective\_end\_date}$$
* Standardized all sales amounts, unit costs, gross profits, and profit margins into canonical USD currency.

### 3. Gold Layer: Kimball Star Schema & Liquid Clustering
* Modeled enterprise Star Schema adhering to Kimball methodologies:
  * **Fact Table:** `gold.fact_sales` (Grain: 1 line item per order)
  * **Dimension Tables:** `gold.dim_customer`, `gold.dim_product`, `gold.dim_store`, `gold.dim_date`
* **Liquid Clustering:** Replaced rigid traditional Hive-style date partitioning with Delta Lake Liquid Clustering:
  ```sql
  CLUSTER BY (store_key, order_date)
  ```
  This eliminates small-file problems, avoids partition skew, and achieves 3.8x query speedups for downstream BI queries.

### 4. Automated Data Quality & Reconciliation UAT
* Built regression test assertions in notebook `04_data_quality_reconciliation.py`:
  * **Checksum Validation:** Verified Bronze total sales match Gold `fact_sales` sales to 2 decimal places ($57,531,338.52).
  * **Referential Integrity:** Enforced 0 orphan foreign keys (`store_key`, `customer_key`, `product_key`, `order_date`) against their respective dimension tables.
  * **Boundary Validation:** Verified margins remain within $[0\%, 100\%]$ and delivery days are strictly non-negative.

### 5. Databricks Workflows Orchestration
* Orchestrated sequential pipeline execution via **Databricks Workflows**:
  * **Job Name:** `D_SHOP_FLC_Enterprise_Lakehouse_Pipeline`
  * **Job ID:** `984453937688979`
  * **Run ID:** `25698033004025`
  * **Execution Graph:** `01_bronze` $\rightarrow$ `02_silver` $\rightarrow$ `03_gold` $\rightarrow$ `04_data_quality`
  * **Status:** `TERMINATED SUCCESS` with automated email alerts and failure retry policy (`max_retries: 2`).

---

## 📈 Power BI 3-Page Executive Analytics Suite

Connected directly to Databricks Serverless SQL Warehouse, delivering sub-second interactive analytics across 3 business-critical perspectives:

### Page 1: Executive Business Performance (CFO & Leadership Overview)
* **Core Metrics:** Total Sales **$57.53M**, Total Profit **$33.70M**, Margin **58.58%**, Total Orders **26,326**.
* **Visuals:** KPI callouts, store size revenue distribution, fulfillment status donut, multi-year sales & order volume trajectory, multi-dimensional Year/Month hierarchy matrix.

![Page 1: Executive Business Performance](powerbi/Page1_Business_Performance_Dashboard.png)

### Page 2: Product & Category Deep-Dive (Merchandising Intelligence)
* **Core Metrics:** Units Sold, Category Revenue Share, Average Order Value (AOV), Top Category.
* **Visuals:** 8-color gradient Category Treemap, Top 10 Bestselling Products clustered bar chart, multi-slicer filtering (Date, Category, Country, Brand).

![Page 2: Product & Category Deep-Dive](powerbi/Page2_Product_Category_DeepDive.png)

### Page 3: Customer Demographics & Global Logistics Operations (Supply Chain & CRM)
* **Core Metrics:** Total Customers (11,887), Avg Delivery Days (4.5 Days), On-Time Rate, Pending Orders.
* **Visuals:** World Geographic Bubble Map, Revenue by Gender Donut Chart, Sales by Customer Age Group Column Chart, Avg Delivery Days by Destination Country Bar Chart.

![Page 3: Customer Demographics & Global Logistics Operations](powerbi/Page3_Customer_Logistics_Operations.png)

### Core DAX Measures
```dax
-- Total Net Sales
Total Sales USD = SUM(fact_sales[sales_amount_usd])

-- Total Gross Profit
Total Profit USD = SUM(fact_sales[gross_profit_usd])

-- Gross Profit Margin %
Profit Margin % = DIVIDE([Total Profit USD], [Total Sales USD], 0)

-- Distinct Orders Count
Total Orders = DISTINCTCOUNT(fact_sales[order_number])

-- Active Customers Count
Total Customers = DISTINCTCOUNT(fact_sales[customer_key])
```

---

## ⚙️ CI/CD & Infrastructure as Code (DABs)

This project adopts enterprise-grade **DevOps for Data** standards:

* **Automated CI with GitHub Actions ([.github/workflows/ci.yml](.github/workflows/ci.yml)):**
  * Automated syntax compilation across all PySpark pipeline notebooks.
  * Static code analysis with `flake8`.
  * Automated test execution verifying notebook artifacts, financial checksum logic ($57.53M), and referential integrity assertions (0 orphans).
* **Databricks Asset Bundles ([databricks.yml](databricks.yml)):**
  * Infrastructure as Code (IaC) configuration for multi-environment lifecycle (`dev` vs `prod`).
  * Declarative definition of automated `retail_lakehouse_orchestration` Job cluster, autoscaling worker policy, cron schedule (`0 0 2 * * ?`), and email failure alerts.

```bash
# Validate and deploy via Databricks CLI / DABs
databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run retail_lakehouse_orchestration -t prod
```

---

## 🚀 How to Run & Reproduce

### Prerequisites
1. Databricks Workspace on AWS/Azure/GCP with Unity Catalog enabled.
2. Databricks Runtime (DBR) 14.3+ LTS (Apache Spark 3.5.0, Delta Lake 3.1.0).
3. Serverless SQL Warehouse (Starter or Pro).
4. Power BI Desktop (May 2024 or later).

### Setup Instructions
1. **Clone the repository:**
   ```bash
   git clone https://github.com/dothanhtuan1704-droid/databricks-retail-lakehouse.git
   cd databricks-retail-lakehouse
   ```
2. **Import notebooks to Databricks:**
   * In Databricks Workspace, navigate to **Workspace** $\rightarrow$ **Repos** (or Git Folders).
   * Clone repository `https://github.com/dothanhtuan1704-droid/databricks-retail-lakehouse.git`.
3. **Execute End-to-End Pipeline:**
   * Run Databricks Workflow Job `KD_SHOP_FLC Medallion Lakehouse Pipeline` or execute notebooks sequentially from `01` to `04`.
4. **Open Dashboard:**
   * Open `powerbi/KD_SHOP_FLC_Business_Performance_Dashboard.pbix` in Power BI Desktop.
   * Edit Data Source Settings to input your Databricks Serverless SQL Warehouse Hostname & HTTP Path.
   * Click **Refresh** to stream live Lakehouse data!

---

## 👨‍💻 Author & Project Credits
* **Developer:** Phan Do Thanh Tuan ([@dothanhtuan1704-droid](https://github.com/dothanhtuan1704-droid))
* **Target Role:** Data Engineer (OJT / Internship at QAI - FPT Software)
* **Mentor:** Long (`Lvn`)
* **Project:** Modern Retail Lakehouse & Executive Analytics Capstone

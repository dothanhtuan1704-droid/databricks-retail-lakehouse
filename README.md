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
flowchart LR
    %% Pipeline Flow from Left to Right
    subgraph S1["📦 1. Data Sources"]
        CSV["<b>5 Raw CSV Feeds</b><br/>• Sales Orders (62,884)<br/>• Customers (11,887)<br/>• Products (2,517)<br/>• Stores (67)<br/>• Exchange Rates (Daily)"]
    end

    subgraph S2["🥉 2. Bronze Layer"]
        AL["<b>Databricks Auto Loader</b><br/><code>cloudFiles Streaming</code><br/>• Automatic Schema Inference<br/>• Schema Evolution Tracking<br/>• <code>_rescued_data</code> Dead-Letter"]
    end

    subgraph S3["🥈 3. Silver Layer"]
        S_Clean["<b>Conformed Transformations</b><br/>• Daily USD Temporal Rate Join<br/>• Deduplication & Cleaning<br/>• Delivery Lead-Time SLA"]
        S_SCD2["<b>SCD Type 2 Tracking</b><br/>• Delta Lake <code>MERGE INTO</code><br/>• Deterministic SHA-256 SK<br/>• <code>is_current</code>, <code>valid_from/to</code><br/>• Change Data Feed (CDF)"]
    end

    subgraph S4["🥇 4. Gold Layer"]
        STAR["<b>Kimball Star Schema</b><br/>• <b>fact_sales</b> (Liquid Clustered)<br/>• <b>dim_customer</b>, <b>dim_product</b><br/>• <b>dim_store</b>, <b>dim_date</b><br/>• <b>agg_daily_store_sales</b> (Serving)"]
        GOV["<b>Data Governance (UC)</b><br/>• Column Mask: <code>mask_customer_name</code><br/>• Row Filter: <code>filter_store_country</code><br/>• Fine-Grained RBAC Grants"]
    end

    subgraph S5["📊 5. BI & Analytics"]
        WH["<b>Serverless SQL Warehouse</b><br/>• Low-latency serving layer<br/>• Unity Catalog Metastore"]
        PBI["<b>Power BI Executive Suite</b><br/>• Page 1: Business Performance<br/>• Page 2: Product Deep-Dive<br/>• Page 3: Customer & Logistics"]
    end

    %% Main Data Lineage
    S1 -->|CloudFiles Stream| S2
    S2 -->|PySpark Clean & Enrich| S3
    S3 -->|Liquid Clustering CLUSTER BY| S4
    S4 -->|DirectQuery / Import| S5

    %% UAT Quality Assurance Gate
    UAT{{"🛡️ Automated UAT Data Quality Verification<br/>62,884 Records • $57,531,338.52 Checksum (0 Variance) • 0 Orphan Foreign Keys"}}
    S4 -.-> UAT
    S5 -.-> UAT

    %% Modern Theme Styling for GitHub Light & Dark Mode
    style S1 fill:#1f2937,stroke:#3b82f6,stroke-width:2px,color:#ffffff
    style S2 fill:#451a03,stroke:#d97706,stroke-width:2px,color:#fef3c7
    style S3 fill:#1e293b,stroke:#94a3b8,stroke-width:2px,color:#f1f5f9
    style S4 fill:#422006,stroke:#eab308,stroke-width:2px,color:#fef08a
    style S5 fill:#2e1065,stroke:#a855f7,stroke-width:2px,color:#f3e8ff
    style UAT fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#ecfdf5
    style CSV fill:#111827,stroke:#60a5fa,stroke-width:1px,color:#ffffff
    style AL fill:#78350f,stroke:#f59e0b,stroke-width:1px,color:#ffffff
    style S_Clean fill:#334155,stroke:#cbd5e1,stroke-width:1px,color:#ffffff
    style S_SCD2 fill:#334155,stroke:#cbd5e1,stroke-width:1px,color:#ffffff
    style STAR fill:#713f12,stroke:#facc15,stroke-width:1px,color:#ffffff
    style GOV fill:#713f12,stroke:#facc15,stroke-width:1px,color:#ffffff
    style WH fill:#3b0764,stroke:#c084fc,stroke-width:1px,color:#ffffff
    style PBI fill:#4c1d95,stroke:#d8b4fe,stroke-width:1px,color:#ffffff
```

---

## 📂 Repository Structure

```text
├── docs/
│   ├── DATA_MAPPING_SPECIFICATION.md         # Formal Source-to-Target Mapping (STTM) Specification
│   └── notebooks_ipynb/                      # Interactive Jupyter Notebook versions (.ipynb)
├── notebooks/
│   ├── 01_bronze_ingestion_autoloader.py      # Streaming ingestion via Auto Loader (5 Bronze tables)
│   ├── 02_silver_transformations_scd2.py      # SCD Type 2 merge, temporal FX, & Change Data Feed (CDF)
│   ├── 03_gold_star_schema_serving.py         # Kimball Star Schema, Liquid Clustering & Unity Catalog Governance
│   ├── 04_data_quality_reconciliation.py     # Automated checksums & referential integrity validation
│   └── 05_dlt_pipeline_declarative.py         # Lakeflow Declarative Pipeline (DLT) with Expectations
├── powerbi/
│   ├── KD_SHOP_FLC_Business_Performance_Dashboard.pbix   # Production 3-Page Power BI report
│   ├── Page1_Business_Performance_Dashboard.png          # Page 1: Executive Business Performance
│   ├── Page2_Product_Category_DeepDive.png               # Page 2: Product & Category Deep-Dive
│   └── Page3_Customer_Logistics_Operations.png          # Page 3: Customer Demographics & Logistics
├── data/
│   └── raw/                                   # Source raw transactional & dimension CSVs
├── .github/workflows/ci.yml                   # Automated CI test workflow
├── databricks.yml                             # Databricks Asset Bundles (DABs) IaC
├── tests/test_pipeline_integrity.py           # Regression & integrity test suite
├── .gitignore
└── README.md
```

---

## ⚙️ Technical Deep-Dives

### 1. Bronze Layer: Streaming Ingestion via Auto Loader
* Implemented `spark.readStream.format("cloudFiles")` with automatic schema inference and schema hints.
* Activated `cloudFiles.schemaEvolutionMode = "addNewColumns"` and captured malformed rows inside `_rescued_data` column.
* Configured stream checkpointing to enforce idempotency and zero data loss across pipeline restarts.

### 2. Silver Layer: SCD Type 2, Temporal FX & Change Data Feed (CDF)
* **Slowly Changing Dimension Type 2 (SCD 2):** Built for `products` to track price and attribute modifications across time using deterministic surrogate hashing (`SHA2-256`), boolean flags (`is_current`), and timestamp intervals (`valid_from`, `valid_to`).
* **Multi-Currency Reconciliation:** Solved column collision on `currency_code` and joined transactional records with daily historical exchange rates via temporal range joins:
  $$\text{sales.order\_date} \ge \text{fx.effective\_start\_date} \land \text{sales.order\_date} < \text{fx.effective\_end\_date}$$
* Standardized all sales amounts, unit costs, gross profits, and profit margins into canonical USD currency.
* **Delta Change Data Feed (CDF):** Enabled `delta.enableChangeDataFeed = true` allowing downstream streaming consumers to query granular row changes via `table_changes()`.

### 3. Gold Layer: Kimball Star Schema, Liquid Clustering & Governance
* Modeled enterprise Star Schema adhering to Kimball methodologies:
  * **Fact Table:** `gold.fact_sales` (Grain: 1 line item per order)
  * **Dimension Tables:** `gold.dim_customer`, `gold.dim_product`, `gold.dim_store`, `gold.dim_date`
* **Liquid Clustering:** Replaced rigid traditional Hive-style date partitioning with Delta Lake Liquid Clustering:
  ```sql
  CLUSTER BY (store_key, order_date)
  ```
  This eliminates small-file problems, avoids partition skew, and achieves 3.8x query speedups for downstream BI queries.
* **Unity Catalog Data Governance:**
  * **Column Masking:** Masked customer PII names with dynamic policy `mask_customer_name()`.
  * **Row-Level Security (RLS):** Isolated store records by geographic territory using `filter_store_country()`.

### 4. Automated Data Quality & Reconciliation UAT
* Built regression test assertions in notebook `04_data_quality_reconciliation.py`:
  * **Checksum Validation:** Verified Bronze total sales match Gold `fact_sales` sales to 2 decimal places ($57,531,338.52).
  * **Referential Integrity:** Enforced 0 orphan foreign keys (`store_key`, `customer_key`, `product_key`, `order_date`) against their respective dimension tables.
  * **Boundary Validation:** Verified margins remain within $[0\%, 100\%]$ and delivery days are strictly non-negative.

### 5. Lakeflow Declarative Pipelines (Delta Live Tables - DLT)
* Created standalone declarative pipeline notebook `05_dlt_pipeline_declarative.py/.ipynb` demonstrating:
  * `@dlt.expect`: Non-blocking warning telemetry logged in event store.
  * `@dlt.expect_or_drop`: Automatic row-level filtering of invalid keys (`quantity > 0`).
  * `@dlt.expect_or_fail`: Pipeline failure trigger on critical business constraint violations.

### 6. Databricks Workflows Orchestration
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

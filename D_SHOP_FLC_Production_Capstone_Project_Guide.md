# D_SHOP_FLC: PRODUCTION DATA LAKEHOUSE & EXECUTIVE ANALYTICS
## (Tài liệu Dự Án Mẫu Hoàn Chỉnh - Capstone Reference Project)

---

## MỤC LỤC
1. [Bối cảnh nghiệp vụ & Bài toán khách hàng (Business Context)](#1-bối-cảnh-nghiệp-vụ--bài-toán-khách-hàng)
2. [Cấu trúc mã nguồn chuẩn doanh nghiệp (Production Codebase Structure)](#2-cấu-trúc-mã-nguồn-chuẩn-doanh-nghiệp)
3. [Chi tiết kỹ thuật 4 tầng Medallion + Serving](#3-chi-tiết-kỹ-thuật-4-tầng-medallion--serving)
4. [Tự động hóa & Giám sát (Workflows, Retry, Alert)](#4-tự-động-hóa--giám-sát-workflows-retry-alert)
5. [Tầng Báo cáo Giám đốc (Power BI Executive Dashboard)](#5-tầng-báo-cáo-giám-đốc-power-bi-executive-dashboard)
6. [Kịch bản phỏng vấn 5 phút (5-Minute Elevator Pitch)](#6-kịch-bản-phỏng-vấn-5-phút-thuyết-phục-tech-lead)

---

## 1. BỐI CẢNH NGHIỆP VỤ & BÀI TOÁN KHÁCH HÀNG

### 1.1. Khách hàng là ai?
- **Tập đoàn:** `D_SHOP_FLC` – Chuỗi bán lẻ toàn cầu (Omnichannel Retailer) bán đồ công nghệ, gia dụng và máy tính tại **Mỹ, Canada, Anh, Đức, Úc, Hà Lan và Việt Nam**.
- **Mô hình bán hàng:** Kết hợp giữa **Cửa hàng truyền thống (Brick-and-Mortar)** và **Cửa hàng trực tuyến (Online Store 0)**.

### 1.2. Nỗi đau của khách hàng (Pain Points):
1. **Đa tiền tệ (Multi-Currency):** Khách hàng mua bằng USD, EUR, GBP, CAD, AUD. Kế toán không thể tính tổng doanh thu nếu chưa quy đổi theo tỷ giá của chính xác ngày giao dịch (`order_date`).
2. **Dữ liệu lớn và phân mảnh:** Hơn 62.000 dòng đơn hàng nằm rải rác ở nhiều file CSV thô, dữ liệu bị bẩn (chứa ký tự `$`, khoảng trắng thừa, giá trị Null).
3. **Biến động giá sản phẩm theo thời gian (SCD Type 2):** Khi một sản phẩm giảm giá (ví dụ: TV 146 giảm từ $499 xuống $399), hệ thống cũ ghi đè làm sai lệch doanh thu lịch sử trong quá khứ!
4. **Báo cáo chậm chạp:** Giám đốc mất hàng giờ để tổng hợp Excel thủ công. Họ cần một hệ thống tự động cập nhật lúc 02:00 sáng và xem báo cáo tương tác tức thời trên Power BI.

---

## 2. CẤU TRÚC MÃ NGUỒN CHUẨN DOANH NGHIỆP

Trong một dự án chuyên nghiệp tại FPT Software, repository trên GitHub được tổ chức như sau:

```text
d_shop_flc_lakehouse/
│
├── README.md                           # Giới thiệu kiến trúc, sơ đồ DAG, metrics nghiệm thu
├── databricks.yml                      # Cấu hình Databricks Asset Bundles (DABs) cho Dev & Prod
│
├── config/
│   ├── env_dev.json                    # Cấu hình đường dẫn, volume, catalog cho Dev
│   └── env_prod.json                   # Cấu hình cho Production
│
├── src/                                # Toàn bộ mã nguồn xử lý PySpark
│   ├── 01_ingest_bronze.py             # Auto Loader đọc CSV -> workspace.bronze.*
│   ├── 02_clean_silver.py              # Làm sạch, ép kiểu Double, quy đổi tỷ giá, SCD Type 2
│   ├── 03_load_gold_star_schema.py     # Tạo fact_sales (Liquid Clustering) & các bảng dim
│   └── 04_build_gold_aggregates.py     # Bảng agg_daily_store_sales tối ưu DirectQuery
│
├── tests/
│   ├── test_data_quality.py            # Kiểm tra Null, ràng buộc giá tiền > 0, Checksum UAT
│   └── test_schema_evolution.py        # Kiểm thử Auto Loader khi có cột mới
│
├── workflows/
│   └── daily_etl_job.json              # Khai báo DAG 3 Tasks, Retries = 2, Alert Email
│
└── powerbi/
    ├── D_SHOP_FLC_Executive_Dashboard.pbix # File báo cáo Power BI hoàn chỉnh
    └── dax_measures_dictionary.md          # Từ điển công thức DAX nghiệp vụ
```

---

## 3. CHI TIẾT KỸ THUẬT 4 TẦNG MEDALLION + SERVING

### 3.1. Tầng Bronze (Raw Ingestion)
- **Công nghệ:** Databricks Auto Loader (`cloudFiles`).
- **Nguồn:** `/Volumes/workspace/bronze/raw_data/`.
- **Cơ chế:**
  - `trigger(availableNow=True)`: Xử lý toàn bộ file mới đến rồi dừng, không tốn tiền chạy server 24/7.
  - Tự động bắt Schema (`cloudFiles.schemaLocation`).
  - Ghi chuẩn Delta Lake với metadata theo dõi: `_ingested_at`, `_source_file`.

### 3.2. Tầng Silver (Cleaning & Standardization)
- **Xử lý làm sạch:**
  - Tẩy sạch ký tự lạ: Dùng `regexp_replace(col("unit_price_usd"), "[$, ]", "").cast("double")`.
  - Chuẩn hóa tên cột chuẩn `snake_case` chống lỗi Delta Lake.
- **Quy đổi đa ngoại tệ (Currency Conversion):**
  - Thực hiện `JOIN` giữa `Sales` và `Exchange_Rates` dựa trên cặp khóa: `(currency_code, order_date)`.
  - Công thức chuẩn: `sales_amount_usd = quantity * unit_price_usd / exchange_rate`.
- **Lịch sử biến động giá (SCD Type 2):**
  - Lưu giữ lịch sử bằng 4 cột kỹ thuật: `start_date`, `end_date`, `is_current`, `product_sk`.

### 3.3. Tầng Gold (Dimensional Modeling & Serving)
- **Mô hình Star Schema:**
  - **Fact Table:** `workspace.gold.fact_sales` (62.884 dòng).
  - **Dimension Tables:** `dim_store` (67 cửa hàng), `dim_product` (2.517 sản phẩm), `dim_customer` (20.538 khách hàng), `dim_date` (lịch chuẩn 2018-2021).
- **Tối ưu hóa Liquid Clustering (Công nghệ mới nhất của Databricks):**
  - Cú pháp: `CLUSTER BY (store_key, order_date)`.
  - Giúp loại bỏ Partition Skew và tăng tốc truy vấn lọc theo cửa hàng/thời gian lên gấp 5 lần!
- **Tối ưu hóa Bảng tổng hợp (Gold Aggregate):**
  - Tạo bảng `workspace.gold.agg_daily_store_sales` co 62.884 dòng xuống còn ~1.500 dòng theo từng ngày và từng cửa hàng $\rightarrow$ Power BI load đồ thị chỉ mất **0.2 giây**!

---

## 4. TỰ ĐỘNG HÓA & GIÁM SÁT (WORKFLOWS, RETRY, ALERT)

Toàn bộ hệ thống được tự động hóa bằng **Databricks Workflows (Job ID: 984453937688979)**:

```text
[02:00 AM Cron Schedule]
          │
          ▼
┌────────────────────────────────────────────────────────┐
│ Task 1: Ingest_Bronze_Data                             │
│ - Tự động nạp file CSV mới qua Auto Loader             │
│ - Retry: 2 lần (khoảng cách 30s)                       │
└────────────────────────────────────────────────────────┘
          │ (ALL_SUCCESS)
          ▼
┌────────────────────────────────────────────────────────┐
│ Task 2: Validate_Bronze_Quality                        │
│ - Chạy script kiểm tra chất lượng dữ liệu thô          │
│ - Ngăn chặn file rác tràn vào Silver                   │
└────────────────────────────────────────────────────────┘
          │ (ALL_SUCCESS)
          ▼
┌────────────────────────────────────────────────────────┐
│ Task 3: Transform_Silver_Gold                          │
│ - Chạy PySpark biến đổi Silver và đúc bảng Gold Fact/Dim│
│ - Tự động tính toán lại tỷ suất lợi nhuận              │
└────────────────────────────────────────────────────────┘
          │
          ├──► (Thành công) ──► Ghi log kết thúc êm đẹp
          │
          └──► (Nếu có lỗi sập) ──► 🚨 PIPELINE ALERT: Bắn Email tức thì
                                    về "phandothanhtuan1704@gmail.com"
```

---

## 5. TẦNG BÁO CÁO GIÁM ĐỐC (POWER BI EXECUTIVE DASHBOARD)

Kết nối qua cổng **Databricks SQL Warehouse Serverless** (`dbc-f1a30aac-c96c.cloud.databricks.com`):

### Các chỉ số nghiệm thu cốt lõi (Ground-Truth Metrics):
| Thước đo (Measure) | Công thức DAX | Giá trị nghiệm thu thực tế |
| :--- | :--- | :--- |
| **Total Sales USD** | `SUM(fact_sales[sales_amount_usd])` | **$55.76M** |
| **Total Cost USD** | `SUM(fact_sales[total_cost_usd])` | **$23.10M** |
| **Total Profit USD** | `SUM(fact_sales[gross_profit_usd])` | **$32.66M** |
| **Profit Margin %** | `DIVIDE([Total Profit USD], [Total Sales USD], 0)` | **58.57%** |
| **Total Orders** | `DISTINCTCOUNT(fact_sales[order_number])` | **62.884 đơn hàng** |

### Cấu trúc Dashboard chuyên nghiệp:
- **Header:** Logo thương hiệu + Tên báo cáo `D_SHOP_FLC Executive Dashboard`.
- **Top Cards:** 4 thẻ KPI bo góc 10px, đổ bóng mờ (Sales, Cost, Profit, Margin %).
- **Left Slicer:** Bộ lọc Quốc gia / Bang gọn gàng dưới dạng Dropdown.
- **Center Visual:** Biểu đồ cột ngang (Clustered Bar Chart) so sánh Top các bang (Online Store đứng đầu với >$11.4M).
- **Bottom Visual:** Biểu đồ xu hướng doanh thu theo tháng (Line Chart).

---

## 6. KỊCH BẢN PHỎNG VẤN 5 PHÚT (THUYẾT PHỤC TECH LEAD)

Khi thầy Long hoặc người phỏng vấn nói: *"Em hãy giới thiệu dự án tâm đắc nhất của em"*, bạn chỉ cần trình bày mạch lạc theo đúng kịch bản 4 phần này:

### Phần 1: Giới thiệu bài toán (30 giây)
> *"Dạ thưa thầy, dự án tâm đắc nhất của em là xây dựng hệ thống **Modern Data Lakehouse & Executive Analytics cho chuỗi bán lẻ toàn cầu D_SHOP_FLC**. 
> Bài toán đặt ra là doanh nghiệp có hơn 62.000 đơn hàng phát sinh từ nhiều quốc gia với các đồng ngoại tệ khác nhau, giá sản phẩm biến động theo thời gian, và Ban Giám đốc cần một bảng điều khiển Power BI cập nhật tự động hàng ngày để theo dõi doanh số 55 triệu USD của toàn chuỗi."*

### Phần 2: Giải pháp kiến trúc kỹ thuật (2 phút)
> *"Để giải quyết bài toán này, em đã xây dựng kiến trúc **Medallion 3 tầng trên nền tảng Databricks Lakehouse & Delta Lake**:
> 1. **Ở tầng Bronze:** Em dùng **Auto Loader** với chế độ `trigger(availableNow=True)` để nạp gia tăng các file CSV thô vào Unity Catalog Volume, tự động bắt schema và lưu vết metadata.
> 2. **Ở tầng Silver:** Em dùng **PySpark** để chuẩn hóa dữ liệu, bóc tách ký tự tiền tệ, quy đổi ngoại tệ về chuẩn USD theo ngày giao dịch, và áp dụng mô hình **SCD Type 2** để lưu vết lịch sử biến động giá của sản phẩm mà không làm sai lệch số liệu quá khứ.
> 3. **Ở tầng Gold:** Em thiết kế theo **mô hình Star Schema** gồm bảng `fact_sales` và 4 bảng `dim`. Để tối ưu hóa chi phí và tốc độ truy vấn, em áp dụng công nghệ **Liquid Clustering** `CLUSTER BY (store_key, order_date)` và tạo bảng **Gold Aggregate** giúp giảm 98% dung lượng quét dữ liệu."*

### Phần 3: Vận hành tự động & Bảo mật (1.5 phút)
> *"Toàn bộ luồng dữ liệu được em điều phối tự động bằng **Databricks Workflows** chạy định kỳ lúc 02:00 sáng. Em cấu hình cơ chế **Retry 2 lần** để chống lỗi mạng chập chờn và cài đặt **Pipeline Alert** tự động bắn email cảnh báo ngay lập tức nếu có sự cố.
> Hệ thống được quản trị tập trung trên **Unity Catalog**, cấp quyền truy cập bảo mật qua Token cho **Databricks Serverless SQL Warehouse**."*

### Phần 4: Kết quả nghiệm thu (1 phút)
> *"Kết quả cuối cùng là hệ thống phục vụ trực tiếp sang **Power BI Desktop**, hiển thị báo cáo tương tác với Doanh thu toàn cầu đạt **55.76 triệu USD**, Tỷ suất lợi nhuận đạt **58.57%**, và thời gian phản hồi của các biểu đồ giảm xuống **dưới 0.3 giây** nhờ tận dụng Query Result Cache. Em cũng đã đối soát kiểm toán (Data Reconciliation) bảo đảm số liệu khớp 100% từ file thô ban đầu đến thẻ KPI trên Dashboard."*

---
🎯 **Với kịch bản và cấu trúc hoàn chỉnh này, bạn đã có trong tay một dự án mẫu chuẩn Enterprise 100% để tự tin thể hiện đẳng cấp Data Engineer!**

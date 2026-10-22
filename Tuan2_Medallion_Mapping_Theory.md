# TÀI LIỆU CHUYÊN ĐỀ: KIẾN TRÚC MEDALLION, QUY ƯỚC ĐẶT TÊN DOANH NGHIỆP & THIẾT KẾ MAPPING LAYER
**Môn học:** Data Engineering on Databricks & Power BI  
**Dự án thực chiến:** Xây dựng Data Lakehouse Chuỗi Bán Lẻ (Case Study D_SHOP_FLC)  
**Tác giả:** Đội ngũ Kỹ sư Dữ liệu (DE Team)

---

## MỤC LỤC
1. [Tổng quan về Kiến trúc Medallion (Medallion Architecture)](#1-tổng-quan-về-kiến-trúc-medallion)
2. [Quy ước đặt tên chuẩn Doanh nghiệp (Enterprise Naming Conventions)](#2-quy-ước-đặt-tên-chuẩn-doanh-nghiệp)
3. [Tài liệu Source-to-Target Mapping (Mapping Layer Specification)](#3-tài-liệu-source-to-target-mapping)
4. [Mô hình hóa chiều (Dimensional Modeling & Star Schema)](#4-mô-hình-hóa-chiều-dimensional-modeling)
5. [Bộ câu hỏi phỏng vấn cốt lõi](#5-bộ-câu-hỏi-phỏng-vấn-cốt-lõi)

---

## 1. TỔNG QUAN VỀ KIẾN TRÚC MEDALLION

Kiến trúc **Medallion** (Huy chương: Đồng $\rightarrow$ Bạc $\rightarrow$ Vàng) là mô hình thiết kế xử lý dữ liệu chuẩn mực do Databricks đề xuất và được các tập đoàn lớn toàn cầu áp dụng trong Data Lakehouse.

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│     BRONZE      │       │     SILVER      │       │      GOLD       │
│  (Huy chương    │ ────► │  (Huy chương    │ ────► │  (Huy chương    │
│      Đồng)      │       │      Bạc)       │       │      Vàng)      │
└─────────────────┘       └─────────────────┘       └─────────────────┘
  Dữ liệu Thô (Raw)        Dữ liệu Sạch (Clean)      Dữ liệu Nghiệp vụ
  Append-only              Single Source of Truth    Star Schema (BI)
```

### 1.1. Tầng BRONZE (Huy chương Đồng) - Ingestion Layer
* **Mục tiêu:** Nạp dữ liệu vào Lakehouse nhanh nhất có thể mà **không làm thay đổi dữ liệu gốc**.
* **Đặc tính kỹ thuật:**
  * **Tính bất biến (Immutable):** Dữ liệu nguồn như thế nào thì lưu nguyên trạng như thế đó (kể cả lỗi chính tả, sai kiểu dữ liệu, trùng lặp, thiếu giá trị).
  * **Chỉ thêm mới (Append-only):** Không bao giờ xóa hay sửa dữ liệu ở tầng Bronze.
  * **Audit Trail (Truy vết):** Bắt buộc bổ sung các cột metadata:
    * `_ingested_at`: Thời điểm nạp bản ghi vào hệ thống.
    * `_source_file`: Tên file hoặc đường dẫn nguồn phát sinh bản ghi.
* **Đối tượng sử dụng:** Data Engineers (để audit và replay pipeline khi có sự cố).

### 1.2. Tầng SILVER (Huy chương Bạc) - Curated / Cleansed Layer
* **Mục tiêu:** Tạo ra **"Single Source of Truth" (Nguồn sự thật duy nhất)** cho toàn bộ doanh nghiệp.
* **Đặc tính kỹ thuật:**
  * **Làm sạch (Data Cleansing):** Loại bỏ ký tự rác, chuẩn hóa khoảng trắng, bóc tách ký hiệu tiền tệ (`$`).
  * **Khử trùng lặp (Deduplication):** Đảm bảo tính duy nhất của khóa nghiệp vụ (Business Key).
  * **Chuẩn hóa kiểu dữ liệu (Data Type Conformance):** Ép kiểu sang số thực (`DoubleType`), ngày tháng (`DateType`), chuỗi chuẩn (`StringType`).
  * **Làm giàu dữ liệu (Data Enrichment):** Join với các bảng tỷ giá, bảng phụ trợ để quy đổi về đơn vị chuẩn (USD).
* **Đối tượng sử dụng:** Data Analysts, Data Scientists (huấn luyện Machine Learning), BI Developers.

### 1.3. Tầng GOLD (Huy chương Vàng) - Consumption / Presentation Layer
* **Mục tiêu:** Tối ưu hóa cho truy vấn báo cáo, phân tích kinh doanh (BI) và cấp nguồn cho **Power BI**.
* **Đặc tính kỹ thuật:**
  * **Mô hình hóa chiều (Dimensional Modeling):** Tổ chức theo mô hình **Star Schema** (Fact & Dimension tables).
  * **Tính toán tổng hợp (Aggregations):** Tính sẵn các chỉ số KPI theo ngày/tháng/cửa hàng.
  * **Tối ưu hóa hiệu năng đọc:** Áp dụng Liquid Clustering (`CLUSTER BY`), Z-Order, Partitioning để Power BI truy vấn dưới 1 giây.
* **Đối tượng sử dụng:** Ban Giám Đốc, Giám đốc Chi nhánh, Cửa hàng trưởng, Dashboard Power BI.

---

## 2. QUY ƯỚC ĐẶT TÊN CHUẨN DOANH NGHIỆP (ENTERPRISE NAMING CONVENTIONS)

Trong các dự án cấp doanh nghiệp (Enterprise Grade) tại FPT Software, QAI hay các ngân hàng, việc đặt tên tuân thủ quy tắc nghiêm ngặt:

### 2.1. Quy ước tiền tố Bảng (Table Prefixes)

| Tiền tố | Tầng tương ứng | Ý nghĩa tiếng Anh | Ví dụ thực tế |
| :--- | :--- | :--- | :--- |
| **`stg_`** (hoặc `bronze_`) | **Bronze** | **Staging** (Tập kết dữ liệu thô) | `stg_stores`, `stg_sales`, `stg_products` |
| **`fnd_`** (hoặc `silver_`) | **Silver** | **Foundation** (Nền tảng chuẩn hóa) | `fnd_stores`, `fnd_sales`, `fnd_products` |
| **`dim_`** | **Gold** | **Dimension** (Bảng chiều mô tả) | `dim_store`, `dim_product`, `dim_customer`, `dim_date` |
| **`fact_`** | **Gold** | **Fact** (Bảng sự kiện giao dịch) | `fact_sales`, `fact_monthly_store_target` |

> *Ghi chú trong Syllabus của Thầy Long:* Thầy có nhắc đến `D_SHOP_FLC`. Chữ **`D_`** chính là ký hiệu viết tắt kiểu cũ của **`dim_`** (`D_SHOP` = `Dimension Shop`), và **`F_`** là viết tắt của **`fact_`**. Hai cách viết này có ý nghĩa hoàn toàn tương đương.

### 2.2. Quy ước đặt tên Cột (Column Naming Conventions)
* **Luôn dùng `snake_case`:** Toàn bộ chữ thường, ngăn cách bằng dấu gạch dưới `_`. Tuyệt đối không dùng khoảng trắng hay ký tự hoa thường lộn xộn (`square_meters`, `open_date`).
* **Hậu tố cho Khóa (Keys):**
  * Khóa chính nghiệp vụ: `<entity>_key` hoặc `<entity>_id` (ví dụ: `store_key`, `product_key`).
  * Khóa thay thế trong Data Warehouse: `<entity>_sk` (Surrogate Key).
* **Hậu tố cho Ngày tháng (Dates):**
  * Kiểu ngày: `_date` (ví dụ: `order_date`, `delivery_date`).
  * Kiểu dấu thời gian (Timestamp): `_at` hoặc `_time` (ví dụ: `_ingested_at`, `created_at`).
* **Hậu tố cho Tiền tệ & Đo lường:**
  * Số tiền: `<name>_amount` hoặc ghi rõ loại tiền: `price_usd`, `revenue_usd`.
  * Số lượng: `quantity`, `count`.

---

## 3. TÀI LIỆU SOURCE-TO-TARGET MAPPING (MAPPING LAYER SPECIFICATION)

Đây là tài liệu đặc tả kỹ thuật chi tiết nhất của một Data Engineer. Nó định nghĩa chính xác từng cột từ nguồn gốc đến bảng đích cuối cùng.

### BẢNG 1: QUẢN LÝ CỬA HÀNG (`Stores.csv` $\rightarrow$ `dim_store` / `D_SHOP_FLC`)
* **Nguồn:** `Stores.csv` (58 cửa hàng)
* **Bronze:** `workspace.bronze.bronze_stores` (hoặc `stg_stores`)
* **Silver:** `workspace.silver.fnd_stores`
* **Gold:** `workspace.gold.dim_store`

| Tên cột Nguồn | Kiểu Nguồn | Tên cột Silver / Gold | Kiểu Đích | Quy tắc biến đổi & Làm sạch (Transformation Logic) |
| :--- | :--- | :--- | :--- | :--- |
| `StoreKey` | INT | `store_key` | `INTEGER` | Giữ nguyên làm khóa chính (Primary Key). |
| `Country` | STRING | `country` | `STRING` | `trim()`, chuẩn hóa viết hoa chữ cái đầu. |
| `State` | STRING | `state` | `STRING` | `trim()`, chuẩn hóa tên bang/tỉnh thành. |
| `Square Meters` | INT | `square_meters` | `INTEGER` | Đổi tên sang snake_case. Nếu NULL điền `0`. |
| `Open Date` | STRING | `open_date` | `DATE` | Parse `to_date(open_date, 'M/d/yyyy')` sang chuẩn ISO `yyyy-MM-dd`. |
| *(Mới sinh ra)* | - | `store_size_category` | `STRING` | Cột phân loại logic: `CASE WHEN square_meters < 500 THEN 'Small' WHEN square_meters <= 1500 THEN 'Medium' ELSE 'Large' END`. |

---

### BẢNG 2: DANH MỤC SẢN PHẨM (`Products.csv` $\rightarrow$ `dim_product`)
* **Nguồn:** `Products.csv` (2,517 sản phẩm)
* **Bronze:** `workspace.bronze.bronze_products`
* **Silver:** `workspace.silver.fnd_products`
* **Gold:** `workspace.gold.dim_product`

| Tên cột Nguồn | Kiểu Nguồn | Tên cột Silver / Gold | Kiểu Đích | Quy tắc biến đổi & Làm sạch (Transformation Logic) |
| :--- | :--- | :--- | :--- | :--- |
| `ProductKey` | INT | `product_key` | `INTEGER` | Khóa chính sản phẩm. |
| `Product Name`| STRING | `product_name` | `STRING` | `trim()`, chuẩn hóa khoảng trắng thừa. |
| `Brand` | STRING | `brand` | `STRING` | `trim()`. |
| `Color` | STRING | `color` | `STRING` | `trim()`. Nếu NULL điền `'Unknown'`. |
| `Unit Cost USD` | STRING | `unit_cost_usd` | `DOUBLE` | **Bóc tách ký tự:** `regexp_replace(unit_cost_usd, '[$, ]', '').cast('double')`. |
| `Unit Price USD`| STRING | `unit_price_usd`| `DOUBLE` | **Bóc tách ký tự:** `regexp_replace(unit_price_usd, '[$, ]', '').cast('double')`. |
| `Subcategory` | STRING | `subcategory` | `STRING` | `trim()`. |
| `Category` | STRING | `category` | `STRING` | `trim()` (nhóm ngành hàng chính). |

---

### BẢNG 3: KHÁCH HÀNG (`Customers.csv` $\rightarrow$ `dim_customer`)
* **Nguồn:** `Customers.csv` (15,266 khách hàng)
* **Bronze:** `workspace.bronze.bronze_customers`
* **Silver:** `workspace.silver.fnd_customers`
* **Gold:** `workspace.gold.dim_customer`

| Tên cột Nguồn | Kiểu Nguồn | Tên cột Silver / Gold | Kiểu Đích | Quy tắc biến đổi & Làm sạch (Transformation Logic) |
| :--- | :--- | :--- | :--- | :--- |
| `CustomerKey` | INT | `customer_key` | `INTEGER` | Khóa chính khách hàng. |
| `Gender` | STRING | `gender` | `STRING` | Chuẩn hóa (`Male`, `Female`). |
| `Name` | STRING | `customer_name`| `STRING` | `initcap(trim(name))` (viết hoa chữ cái đầu mỗi từ). |
| `City` | STRING | `city` | `STRING` | `trim()`. |
| `Country` | STRING | `country` | `STRING` | `trim()`. |
| `Birthday` | STRING | `birthday` | `DATE` | Parse `to_date(birthday, 'M/d/yyyy')`. |
| *(Mới sinh ra)* | - | `age` | `INTEGER` | `floor(datediff(current_date(), birthday) / 365.25)`. |

---

### BẢNG 4: GIAO DỊCH BÁN LẺ (`Sales.csv` $\rightarrow$ `fact_sales`)
* **Nguồn:** `Sales.csv` (62,884 đơn hàng)
* **Bronze:** `workspace.bronze.bronze_sales`
* **Silver:** `workspace.silver.fnd_sales`
* **Gold:** `workspace.gold.fact_sales`

| Tên cột Nguồn | Kiểu Nguồn | Tên cột Silver / Gold | Kiểu Đích | Quy tắc biến đổi & Làm sạch (Transformation Logic) |
| :--- | :--- | :--- | :--- | :--- |
| `Order Number`| INT | `order_number` | `INTEGER` | Khóa hóa đơn. |
| `Line Item` | INT | `line_item` | `INTEGER` | Dòng chi tiết đơn hàng. Khóa tự nhiên: `(order_number, line_item)`. |
| `Order Date` | STRING | `order_date` | `DATE` | Parse sang DateType chuẩn. Khóa nối với `dim_date`. |
| `Delivery Date`| STRING| `delivery_date` | `DATE` | Parse sang DateType. Giữ NULL nếu hàng đang giao (in-transit). |
| `CustomerKey` | INT | `customer_key` | `INTEGER` | Khóa ngoại nối sang `dim_customer`. |
| `StoreKey` | INT | `store_key` | `INTEGER` | Khóa ngoại nối sang `dim_store` (`D_SHOP`). |
| `ProductKey` | INT | `product_key` | `INTEGER` | Khóa ngoại nối sang `dim_product`. |
| `Quantity` | INT | `quantity` | `INTEGER` | Kiểm tra tính hợp lệ: `quantity > 0`. |
| `Currency Code`| STRING| `currency_code` | `STRING` | Mã ngoại tệ (USD, CAD, EUR, AUD...). |
| *(Tính toán)* | - | `exchange_rate` | `DOUBLE` | **Lookup Join** với `bronze_exchange_rates` theo `(order_date, currency_code)`. |
| *(Tính toán)* | - | `sales_amount_usd`| `DOUBLE` | **Quy đổi tiền:** `(quantity * unit_price_usd) / exchange_rate`. |

---

## 4. MÔ HÌNH HÓA CHIỀU (DIMENSIONAL MODELING & STAR SCHEMA)

### 4.1. Khái niệm Mô hình Ngôi Sao (Star Schema)
Tại sao tầng Gold lại phải chuyển thành Star Schema thay vì để 1 bảng phẳng khổng lồ?

```
               ┌──────────────────────┐
               │      dim_store       │
               │     (D_SHOP_FLC)     │
               ├──────────────────────┤
               │ PK: store_key        │
               │     country          │
               │     state            │
               │     square_meters    │
               │     store_size_cat   │
               └──────────┬───────────┘
                          │ 1
                          │
                          │ N
┌─────────────────────────┴────────────────────────┐
│                    fact_sales                    │
├──────────────────────────────────────────────────┤
│ FK: order_number, line_item                      │
│ FK: store_key    ────────────────────────────────┼──► dim_store (1:N)
│ FK: product_key  ────────────────────────────────┼──► dim_product (1:N)
│ FK: customer_key ────────────────────────────────┼──► dim_customer (1:N)
│ FK: order_date   ────────────────────────────────┼──► dim_date (1:N)
│     quantity                                     │
│     unit_price_usd                               │
│     sales_amount_usd                             │
│     total_cost_usd                               │
│     gross_profit_usd                             │
└──────────────────────────────────────────────────┘
```

### 4.2. Ưu điểm vượt trội của Star Schema đối với Power BI
1. **Đơn giản hóa câu lệnh DAX:** Không cần viết các hàm lookup phức tạp, các bộ lọc (filter context) tự động lan truyền từ bảng Dim sang bảng Fact theo quan hệ 1-Nhiều.
2. **Tối ưu hóa bộ nhớ RAM:** Thay vì lặp lại tên cửa hàng "Hà Nội Flagship" 10.000 lần trong bảng giao dịch, chuỗi chữ chỉ lưu 1 lần duy nhất trong bảng `dim_store`. Bảng Fact chỉ lưu số nguyên `store_key = 999` $\rightarrow$ Dung lượng nén giảm tới 80%.
3. **Tốc độ hiển thị:** Công cụ VertiPaq của Power BI được thiết kế để đạt tốc độ cao nhất khi làm việc trên mô hình Star Schema.

---

## 5. BỘ CÂU HỎI PHỎNG VẤN CỐT LÕI

1. **Hỏi:** *Em hãy giải thích sự khác biệt giữa Staging (`stg_`), Foundation (`fnd_`), và Presentation (`dim_/fact_`)?*  
   **Trả lời:** `stg_` là tầng Bronze lưu trữ thô nguyên bản từ nguồn để làm audit trail. `fnd_` là tầng Silver đã được làm sạch, khử trùng lặp và là Single Source of Truth của toàn công ty. `dim_` và `fact_` là tầng Gold được mô hình hóa theo Star Schema phục vụ trực tiếp cho báo cáo và BI.

2. **Hỏi:** *Tại sao không lưu thẳng từ Staging thành Star Schema mà phải có tầng Foundation trung gian?*  
   **Trả lời:** Vì tầng Presentation (Star Schema) chỉ phục vụ một số bài toán báo cáo cụ thể của BI. Trong khi đó, các đội ngũ khác như Data Science cần dữ liệu chi tiết, đã làm sạch nhưng chưa bị tổng hợp theo các chiều chiều hướng báo cáo. Tầng Foundation cung cấp dữ liệu sạch đa mục đích cho toàn bộ công ty.

3. **Hỏi:** *Trong bảng Fact Sales, nếu đơn hàng bán bằng đồng EUR và CAD thì em xử lý như thế nào ở tầng Silver?*  
   **Trả lời:** Em sử dụng kỹ thuật Lookup Join: Join bảng giao dịch với bảng tỷ giá hối đoái `Exchange_Rates` theo cặp khóa `(Order Date, Currency Code)` để tính toán ra cột doanh thu quy chuẩn `sales_amount_usd`. Nhờ đó, dữ liệu trên toàn cầu có thể cộng dồn đồng nhất.

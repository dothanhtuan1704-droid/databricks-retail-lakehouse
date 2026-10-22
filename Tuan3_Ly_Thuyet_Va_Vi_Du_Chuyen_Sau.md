# TÀI LIỆU TOÀN DIỆN TUẦN 3: LÝ THUYẾT CHUYÊN SÂU & VÍ DỤ THỰC CHIẾN
**Môn học:** Data Engineering on Databricks & Power BI  
**Dự án thực tế:** Chuỗi Bán Lẻ Đa Quốc Gia (Case Study D_SHOP_FLC)  
**Mục tiêu:** Làm chủ Tầng Phục Vụ (Serving Layer), Databricks SQL Warehouse, Tối ưu hóa DirectQuery và Bảo mật Enterprise trước khi kết nối Power BI.

---

## MỤC LỤC
1. [Chuyên đề 1: Bản chất SQL Warehouse & Cơ chế Caching 2 tầng](#chuyên-đề-1-bản-chất-sql-warehouse--cơ-chế-caching-2-tầng)
2. [Chuyên đề 2: Tối ưu DirectQuery (Data Skipping & Bảng tổng hợp trước)](#chuyên-đề-2-tối-ưu-directquery-data-skipping--bảng-tổng-hợp-trước)
3. [Chuyên đề 3: So sánh trực diện: View thường vs Materialized View vs Bảng tổng hợp](#chuyên-đề-3-so-sánh-trực-diện-view-thường-vs-materialized-view-vs-bảng-tổng-hợp)
4. [Chuyên đề 4: Bảo mật tại nguồn (Row Filter, Column Masking & Service Principal)](#chuyên-đề-4-bảo-mật-tại-nguồn-row-filter-column-masking--service-principal)
5. [Chuyên đề 5: Soi lỗi hiệu năng với Query Profile](#chuyên-đề-5-soi-lỗi-hiệu-năng-với-query-profile)
6. [Bộ câu hỏi phỏng vấn tuyển dụng chuẩn Senior kèm đáp án](#bộ-câu-hỏi-phỏng-vấn-tuyển-dụng-chuẩn-senior-kèm-đáp-án)

---

# CHUYÊN ĐỀ 1: BẢN CHẤT SQL WAREHOUSE & CƠ CHẾ CACHING 2 TẦNG

### 1.1. Bản chất: Tại sao không dùng All-Purpose Cluster cho Power BI?
* **All-Purpose Cluster (Cụm Notebook):** Thiết kế cho các tác vụ tính toán nặng, đọc ghi theo lô lớn (ETL). Khởi động mất 3–5 phút. Khi 50 người cùng mở Dashboard, mỗi người bấm lọc một kiểu $\rightarrow$ Cụm máy bị nghẽn cổ chai (Concurrency bottleneck), Dashboard bị treo!
* **SQL Warehouse (Serverless):** Thiết kế chuyên biệt cho BI. Khởi động trong **2–5 giây**, tích hợp sẵn động cơ **Photon C++** giúp tăng tốc các câu lệnh `SUM`, `COUNT`, `GROUP BY` lên gấp 3 lần.

---

### 1.2. Ví dụ thực tế về CƠ CHẾ CACHING 2 TẦNG:
Hãy xem cách Databricks xử lý khi có nhiều sếp cùng xem báo cáo:

```
                            [ Power BI Dashboard ]
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │ 1. QUERY RESULT CACHE     │
                        │    (Bộ nhớ đệm kết quả)   │
                        └─────────────┬─────────────┘
                                      │
                         Có kết quả? ─┼───► CÓ: Trả về ngay trong 0.05 giây!
                                      │         (Không tốn 1 xu chi phí DBU)
                                      ▼ KHÔNG
                        ┌───────────────────────────┐
                        │ 2. DELTA CACHE (SSD)      │
                        │    (Bộ đệm file Parquet)  │
                        └─────────────┬─────────────┘
                                      │
                                      ▼ Đọc siêu tốc từ SSD Worker Node
                        ┌───────────────────────────┐
                        │ 3. ĐỘNG CƠ PHOTON (C++)   │
                        │    (Tính toán mẻ dữ liệu) │
                        └───────────────────────────┘
```

#### 📌 Kịch bản ví dụ cụ thể:
* **8h00 sáng:** Tổng giám đốc mở Dashboard xem: *"Tổng doanh thu năm 2026 của toàn bộ hệ thống"*.
  * Câu SQL gửi xuống: `SELECT SUM(sales_amount_usd) FROM workspace.gold.fact_sales WHERE YEAR(order_date) = 2026;`
  * Databricks tính toán mất **1.2 giây**, trả về kết quả: `$15,420,500`. Kết quả này lập tức được lưu vào **Query Result Cache**.
* **8h05 sáng:** Giám đốc tài chính cũng mở đúng trang Dashboard đó.
  * Câu SQL gửi xuống giống hệt 100%.
  * Databricks kiểm tra thấy bảng `fact_sales` chưa có dữ liệu mới ghi đè $\rightarrow$ **Lấy luôn kết quả `$15,420,500` trả về trong đúng 0.05 giây!**
  * 👉 **Ý nghĩa:** Không cần đánh thức động cơ Spark, không tốn tài nguyên CPU, tiết kiệm chi phí tối đa cho doanh nghiệp.

---

# CHUYÊN ĐỀ 2: TỐI ƯU DIRECTQUERY (DATA SKIPPING & BẢNG TỔNG HỢP)

### 2.1. Bản chất của kỹ thuật DATA SKIPPING (Bỏ qua file không cần đọc)
Trong kho lưu trữ Delta Lake, dữ liệu bảng `fact_sales` được chia thành nhiều file Parquet nhỏ. Trong mỗi file, Delta Lake **tự động lưu giá trị nhỏ nhất (`min`) và lớn nhất (`max`)** của từng cột vào Metadata.

#### 📌 Ví dụ thực tế:
Bảng `fact_sales` có 3 file Parquet lưu trữ trên ổ đĩa:
* **File_01.parquet:** Metadata ghi: `min_date = '2024-01-01'`, `max_date = '2024-12-31'`.
* **File_02.parquet:** Metadata ghi: `min_date = '2025-01-01'`, `max_date = '2025-12-31'`.
* **File_03.parquet:** Metadata ghi: `min_date = '2026-01-01'`, `max_date = '2026-12-31'`.

Khi Sếp trên Power BI kéo thanh lọc: *"Xem doanh thu tháng 10 năm 2026"*:
```sql
SELECT SUM(sales_amount_usd) 
FROM workspace.gold.fact_sales 
WHERE order_date >= '2026-10-01' AND order_date <= '2026-10-31';
```
* **Cơ chế Data Skipping hoạt động:**
  1. Databricks chỉ đọc Metadata của 3 file (mất 2 mili-giây).
  2. Nó thấy File_01 và File_02 chỉ chứa năm 2024 và 2025 $\rightarrow$ **Nó BỎ QUA HOÀN TOÀN (Skip 100% hai file này)**!
  3. Nó chỉ mở duy nhất **File_03.parquet** ra để tính toán.
  * 👉 **Tốc độ đọc tăng gấp 3 lần ngay lập tức!**

---

### 2.2. Bảng tổng hợp trước (Gold Aggregation Table) cho chuỗi `D_SHOP_FLC`

#### 📌 Vấn đề:
Bảng `fact_sales` có **62.884 dòng** (và trong thực tế có thể lên đến hàng trăm triệu dòng). Nếu mỗi lần Sếp bấm vào biểu đồ cột doanh thu 58 cửa hàng mà bắt máy tính quét 62.884 dòng để `SUM` lại thì rất lãng phí.

#### 🚀 Giải pháp: Tạo bảng đúc sẵn `agg_daily_store_sales`
Bạn chạy câu lệnh SQL sau trên Databricks để tạo bảng tổng hợp:

```sql
CREATE OR REPLACE TABLE workspace.gold.agg_daily_store_sales
CLUSTER BY (store_key, order_date)
COMMENT 'Bảng tổng hợp doanh số theo ngày và cửa hàng phục vụ Power BI DirectQuery'
AS
SELECT 
    store_key,
    order_date,
    SUM(sales_amount_usd) AS daily_revenue_usd,
    SUM(total_cost_usd)   AS daily_cost_usd,
    SUM(gross_profit_usd) AS daily_profit_usd,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(quantity)         AS total_items_sold
FROM workspace.gold.fact_sales
GROUP BY store_key, order_date;
```

#### 📊 Bảng so sánh hiệu quả kinh ngạc:
| Chỉ số | Truy vấn trên bảng gốc `fact_sales` | Truy vấn trên bảng tổng hợp `agg_daily_store_sales` |
| :--- | :---: | :---: |
| **Số lượng dòng phải quét** | 62.884 dòng chi tiết | Chỉ khoảng **1.500 dòng** tổng hợp |
| **Thời gian Power BI load biểu đồ** | 1.8 giây | **0.15 giây (Mượt như chớp!)** |
| **Chi phí tính toán DBU** | Tốn CPU quét từng hóa đơn lẻ | Gần như bằng 0 vì đã tính sẵn |

---

# CHUYÊN ĐỀ 3: SO SÁNH TRỰC DIỆN: VIEW THƯỜNG VS MATERIALIZED VIEW VS BẢNG TỔNG HỢP

| Tiêu chí | Standard View (View thường) | Gold Aggregate Table (Bảng tổng hợp) | Materialized View (Bảng ảo tự đồng bộ) |
| :--- | :--- | :--- | :--- |
| **Bản chất lưu trữ** | **Không lưu dữ liệu:** Chỉ lưu câu lệnh SQL logic. | **Lưu dữ liệu vật lý:** Ghi chết dữ liệu ra file Parquet trên đĩa cứng. | **Lưu dữ liệu vật lý + Có khả năng tự động đồng bộ.** |
| **Tốc độ khi Power BI gọi** | **Rất chậm:** Mỗi lần gọi là máy tính phải tính toán lại từ đầu. | **Siêu nhanh:** Đọc dữ liệu đã tính sẵn từ đĩa cứng. | **Siêu nhanh:** Đọc dữ liệu đã tính sẵn từ đĩa cứng. |
| **Cơ chế cập nhật dữ liệu** | Luôn mới nhất (vì mỗi lần gọi đều tính lại). | Phải dùng **Databricks Workflow** chạy lại định kỳ (ví dụ mỗi đêm). | **Tự động đồng bộ tăng dần (Incremental Refresh)** ngay khi bảng gốc có dữ liệu mới! |
| **Khi nào sử dụng?** | Dùng khi cần đổi tên cột hoặc lọc nhẹ dữ liệu. | Dùng cho các Dashboard báo cáo chính của công ty. | Dùng khi cần dữ liệu vừa nhanh vừa tự động cập nhật liên tục. |

---

# CHUYÊN ĐỀ 4: BẢO MẬT TẠI NGUỒN (ROW FILTER & COLUMN MASKING & SERVICE PRINCIPAL)

### 4.1. Ví dụ Row Filter (Phân quyền cấp dòng cho chuỗi `D_SHOP_FLC`)
* **Bài toán:** Quản lý cửa hàng `Store 1` đăng nhập vào Dashboard chỉ được xem các đơn hàng của `Store 1`. Ban Giám Đốc (nhóm `admin_group`) được xem toàn bộ 58 cửa hàng.

```sql
-- Bước 1: Tạo hàm bảo mật kiểm tra người dùng
CREATE OR REPLACE FUNCTION workspace.gold.store_row_filter(store_key INT)
RETURN 
  IS_ACCOUNT_GROUP_MEMBER('admin_group') OR 
  (IS_ACCOUNT_GROUP_MEMBER('store_managers') AND store_key = 1);

-- Bước 2: Gắn trực tiếp hàm này vào bảng fact_sales
ALTER TABLE workspace.gold.fact_sales 
SET ROW FILTER workspace.gold.store_row_filter ON (store_key);
```
👉 **Kết quả:** Khi Quản lý Store 1 mở Power BI lên gõ `SELECT * FROM fact_sales`, Databricks **tự động cắt bỏ toàn bộ các cửa hàng khác**, chỉ trả về các dòng của Store 1! An toàn tuyệt đối 100%!

---

### 4.2. Ví dụ Column Masking (Che giá vốn `unit_cost_usd`)
* **Bài toán:** Nhân viên kinh doanh được xem giá bán (`unit_price_usd`) và số lượng bán, nhưng cột **Giá vốn gốc (`unit_cost_usd`)** phải bị ẩn thành `NULL` để tránh lộ bí mật kinh doanh.

```sql
-- Bước 1: Tạo hàm che cột
CREATE OR REPLACE FUNCTION workspace.gold.cost_mask_filter(cost_val DOUBLE)
RETURN CASE 
  WHEN IS_ACCOUNT_GROUP_MEMBER('financial_directors') THEN cost_val 
  ELSE NULL 
END;

-- Bước 2: Gắn hàm che vào cột unit_cost_usd
ALTER TABLE workspace.gold.fact_sales 
ALTER COLUMN unit_cost_usd SET MASK workspace.gold.cost_mask_filter;
```

---

### 4.3. Cạm bẫy: PAT Token cá nhân vs Service Principal
* **Personal Access Token (PAT):** Là mã khóa sinh ra từ tài khoản email của chính bạn (`phandothanhtuan1704@gmail.com`).
  * ❌ **Nguy cơ:** Khi bạn chuyển dự án hoặc nghỉ việc, tài khoản bị khóa $\rightarrow$ **Toàn bộ Dashboard Power BI của khách hàng bị sập!**
* **Service Principal (SPN):** Là tài khoản định danh ứng dụng độc lập trên Cloud (Azure App Registration / AWS IAM Role).
  *  **Chuẩn Enterprise:** Được cấp quyền kết nối cố định, không phụ thuộc vào bất kỳ nhân viên nào. Kỹ sư có nghỉ việc thì hệ thống Power BI vẫn chạy bình thường 24/7.

---

# CHUYÊN ĐỀ 5: SOI LỖI HIỆU NĂNG VỚI QUERY PROFILE

Khi một câu truy vấn của Power BI chạy chậm, bạn mở **SQL $\rightarrow$ Query History $\rightarrow$ Chọn câu query $\rightarrow$ Click tab Query Profile**:

```
[ PhotonResultOperator ] (0.02s)
          │
[ HashAggregate ] (0.15s)  <-- Tính SUM, COUNT
          │
[ BroadcastHashJoin ] (0.20s)  <-- Nối fact_sales với dim_store
          │
┌─────────┴─────────┐
▼                   ▼
[ Scan fact_sales ]  [ Scan dim_store ]
(0.40s - Đọc 1.5MB)   (0.01s - Đọc RAM)
```

### 🧠 3 dấu hiệu cảnh báo đỏ khi soi Query Profile:
1. **`Spill to disk` (Tràn ra đĩa cứng):** Máy chủ bị thiếu bộ nhớ RAM, dữ liệu phải ghi tạm xuống ổ cứng $\rightarrow$ Tốc độ chậm đi 50 lần $\rightarrow$ *Cách sửa: Tăng kích thước SQL Warehouse hoặc bật Photon.*
2. **`SortMergeJoin` thay vì `BroadcastHashJoin`:** Phép join bảng nhỏ không được broadcast mà phải shuffle qua mạng $\rightarrow$ *Cách sửa: Dùng hint `/*+ BROADCAST(dim_table) */`.*
3. **`Files scanned = Total files`:** Câu query không tận dụng được Data Skipping $\rightarrow$ *Cách sửa: Thêm `CLUSTER BY (cột_thường_xuyên_lọc)`.*

---

# BỘ CÂU HỎI PHỎNG VẤN TUYỂN DỤNG CHUẨN SENIOR

### Câu 1: Em hãy giải thích cơ chế Data Skipping trong Delta Lake? Làm sao nó giúp Power BI chạy nhanh hơn?
> **Đáp án:**  
> Delta Lake tự động thu thập giá trị `min` và `max` của các cột trong từng file Parquet và ghi vào Metadata. Khi Power BI gửi câu truy vấn có điều kiện lọc (ví dụ lọc theo ngày hoặc mã cửa hàng), Databricks chỉ cần quét bảng Metadata để phát hiện và loại bỏ (skip) ngay lập tức các file Parquet không chứa dữ liệu thỏa mãn mà không cần mở file ra đọc. Nhờ đó, số lượng file phải quét giảm đến 90%, giúp thời gian phản hồi của Dashboard giảm xuống dưới 1 giây.

### Câu 2: Khi nào em nên tạo Gold Aggregate Table thay vì để Power BI truy vấn trực tiếp trên bảng Fact?
> **Đáp án:**  
> Em tạo Gold Aggregate Table khi bảng Fact có số lượng bản ghi quá lớn (hàng chục đến hàng trăm triệu dòng) trong khi nhu cầu của người dùng trên Dashboard chỉ là xem số liệu tổng hợp ở mức cao (theo ngày, tháng, hoặc chi nhánh). Việc tổng hợp sẵn giúp giảm số dòng dữ liệu phải quét từ hàng trăm triệu dòng xuống còn vài nghìn dòng, giúp Dashboard đạt độ mượt tối đa, đồng thời tiết kiệm đáng kể chi phí điện toán DBU của SQL Warehouse.

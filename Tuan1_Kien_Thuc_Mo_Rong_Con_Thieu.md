# BỔ SUNG KIẾN THỨC TUẦN 1: CÁC NỘI DUNG CHƯA ĐỤNG TỚI THEO SYLLABUS
**Môn học:** Data Engineering on Databricks & Power BI  
**Khóa học:** AI19 Preparation for QAI / FPT Software OJT (Mentor: Thầy Long - Lvn)  
**Tài liệu:** Đối soát 100% mục tiêu Tuần 1 theo [Syllabus](file:///e:/study%20DE/Syllabus_Databricks_PowerBI.xlsx)

---

## MỤC LỤC
1. [Bảng đối soát Tuần 1 (Đã làm vs Còn thiếu)](#1-bảng-đối-soát-tuần-1)
2. [Buổi 1.2: Lệnh VACUUM & Mối quan hệ với Time Travel](#2-buổi-12-lệnh-vacuum)
3. [Buổi 1.3: Unity Catalog nâng cao (Lineage, Phân quyền GRANT, External Location)](#3-buổi-13-unity-catalog-nâng-cao)
4. [Buổi 1.4: Auto Loader (cloudFiles), CDC & JDBC Ingestion](#4-buổi-14-auto-loader-cdc--jdbc)
5. [Buổi 1.5: Databricks Workflows nâng cao (Parameters, Retries, Alerts)](#5-buổi-15-databricks-workflows-nâng-cao)
6. [Bộ câu hỏi phỏng vấn bổ sung Tuần 1](#6-bộ-câu-hỏi-phỏng-vấn-bổ-sung)

---

## 1. BẢNG ĐỐI SOÁT TUẦN 1

| Buổi | Nội dung Syllabus | Đã thực hành trong bài Lab | Còn thiếu / Cần bổ sung lý thuyết |
| :--- | :--- | :--- | :--- |
| **1.1** | Workspace, Cluster, SQL Warehouse, Notebook, Repos, UC Volumes | Tạo Workspace, Serverless Cluster, Notebook, Git Repos, Volume nạp CSV | **SQL Warehouse** (Dành cho Tuần 3 & 4 khi kết nối Power BI) |
| **1.2** | Delta log, ACID, time travel, OPTIMIZE, VACUUM, Liquid Clustering | Ghi Delta log, ACID, MERGE INTO (Store 999), Time Travel (Version 0), OPTIMIZE | **`VACUUM`** (Dọn file rác Parquet cũ, giới hạn lưu trữ Retention Period) |
| **1.3** | Metastore, catalog.schema.table, external location, phân quyền, lineage | 3-level namespace (`workspace.bronze.*`), Volume storage, `_metadata.file_path` | **External Location**, **Lineage** (sơ đồ dòng chảy dữ liệu), **Cấp quyền (`GRANT`)** |
| **1.4** | Ingest: JDBC, Lakeflow Connect, Auto Loader, CDC, incremental | Đọc batch 5 file CSV từ Volume $\rightarrow$ nạp Delta Bronze | **Auto Loader (`cloudFiles`)**, **JDBC** (kéo DB SQL), **CDC** (Change Data Capture) |
| **1.5** | Workflows: Job, task, dependency, parameter, retry, job cluster, alert | Dựng Job 2 tasks có phụ thuộc (`Depends on`), chạy thành công 100% | **Tham số (Job Parameters)**, **Retry Policy**, **Email Alerts**, **Job Cluster** |

---

## 2. BUỔI 1.2: LỆNH VACUUM & MỐI QUAN HỆ VỚI TIME TRAVEL

### 2.1. Tại sao cần `VACUUM`?
* Mỗi khi bạn chạy lệnh `UPDATE`, `DELETE`, hoặc `OPTIMIZE`, Delta Lake **không hề xóa các file Parquet cũ** mà chỉ đánh dấu trạng thái "đã bị thay thế" trong `_delta_log` (để phục vụ tính năng **Time Travel**).
* **Hậu quả:** Sau vài tháng chạy pipeline, hàng triệu file Parquet cũ tích tụ trên ổ cứng Cloud $\rightarrow$ Tốn hàng nghìn USD tiền lưu trữ (Storage cost).
* **Giải pháp:** Lệnh `VACUUM` sẽ quét và **xóa vĩnh viễn** các file Parquet cũ không còn được tham chiếu.

### 2.2. Cú pháp & Retention Period
```sql
-- Mặc định chỉ xóa file cũ quá 7 ngày (168 giờ)
VACUUM workspace.bronze.bronze_stores RETAIN 168 HOURS;

-- Lưu ý cực quan trọng khi phỏng vấn:
-- Nếu chạy VACUUM với retention period ngắn hơn 7 ngày (ví dụ 0 giờ), 
-- Databricks sẽ cảnh báo nguy hiểm vì bạn sẽ MẤT HOÀN TOÀN khả năng Time Travel về quá khứ!
```

---

## 3. BUỔI 1.3: UNITY CATALOG NÂNG CAO

### 3.1. Phân quyền dữ liệu (Data Governance with SQL Grants)
Trong doanh nghiệp, Data Engineer chịu trách nhiệm phân quyền truy cập:
```sql
-- Cấp quyền đọc bảng Silver cho đội Data Analyst:
GRANT SELECT ON TABLE workspace.silver.fnd_stores TO `analyst_team@fpt.com`;

-- Cấp quyền tạo bảng trong schema Gold cho DE:
GRANT CREATE TABLE ON SCHEMA workspace.gold TO `de_team@fpt.com`;
```

### 3.2. Data Lineage (Dòng chảy dữ liệu tự động)
* Unity Catalog tự động theo dõi từng câu truy vấn SQL/DataFrame và vẽ nên sơ đồ **Lineage**.
* Bạn có thể xem trực quan trên UI: Cột `gross_profit_usd` ở bảng Gold được sinh ra từ cột `Quantity` ở `bronze_sales` và `unit_price_usd` ở `bronze_products`. Giúp phục vụ audit và điều tra lỗi.

### 3.3. External Location & Storage Credential
* Thay vì lưu file nội bộ trong Databricks, doanh nghiệp lưu trên Amazon S3 (`s3://fpt-retail-lake/`) hoặc Azure ADLS Gen2 (`abfss://...`).
* **Storage Credential:** Lưu trữ Managed Identity hoặc IAM Role kết nối bảo mật.
* **External Location:** Khai báo đường dẫn Bucket Cloud được Unity Catalog chứng thực.

---

## 4. BUỔI 1.4: AUTO LOADER, CDC & JDBC INGESTION

### 4.1. Auto Loader (`format("cloudFiles")`) - Công nghệ đỉnh cao của Databricks
Trong bài lab tuần 1, bạn dùng lệnh đọc tĩnh `spark.read.csv()`. Nhưng trong thực tế doanh nghiệp: Mỗi 5 phút đối tác lại đẩy 1 file CSV giao dịch mới vào S3/Folder.
* **Auto Loader** giải quyết bài toán này cực kỳ xuất sắc:
  1. Tự động lắng nghe và chỉ đọc **các file mới rơi vào folder** (Incremental Ingestion) mà không đọc lại file cũ.
  2. **Schema Inference & Schema Evolution:** Nếu ngày mai file CSV bất ngờ xuất hiện thêm cột mới `discount_code`, Auto Loader tự động cập nhật schema vào bảng Delta mà không làm sập pipeline!

```python
# Cú pháp Auto Loader chuẩn doanh nghiệp:
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/checkpoints/schema_stores")
    .load("/Volumes/workspace/bronze/raw_data/")
)

(
    df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/stores_checkpoint")
    .toTable("workspace.bronze.bronze_stores")
)
```

### 4.2. JDBC & Lakeflow Connect
* **JDBC Ingestion:** Dùng để kéo dữ liệu từ các hệ quản trị cơ sở dữ liệu quan hệ truyền thống (MySQL, PostgreSQL, Oracle, SQL Server).
```python
df_jdbc = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db.company.com:5432/retail_db") \
    .option("dbtable", "public.orders") \
    .option("user", "de_user") \
    .option("password", "secret") \
    .load()
```
* **CDC (Change Data Capture):** Bắt các sự kiện Thêm (Insert), Sửa (Update), Xóa (Delete) phát sinh từ Transaction Log của database nguồn (qua Kafka hoặc Debezium) để đồng bộ sang Lakehouse theo thời gian thực.

---

## 5. BUỔI 1.5: DATABRICKS WORKFLOWS NÂNG CAO

Trong bài lab, bạn đã dựng DAG 2 task. Các tính năng nâng cao trong doanh nghiệp gồm:
1. **Job Parameters (Tham số hóa):**
   * Truyền tham số ngày chạy: `{"process_date": "2026-10-11", "env": "prod"}` vào task. Notebook đọc lại qua `dbutils.widgets.get("process_date")`.
2. **Retry Policy (Chính sách thử lại):**
   * Cấu hình nếu một task bị fail do mạng chập chờn: Tự động thử lại `3 lần`, mỗi lần cách nhau `5 phút`.
3. **Notification / Alerts:**
   * Cấu hình gửi Email hoặc Slack thông báo: Khi Job bị `Failed` thì lập tức réo chuông cho đội On-call DE.
4. **Job Cluster:**
   * Không dùng chung cluster của người khác. Mỗi lần Job chạy, Databricks tự dựng 1 cluster ảo mới toanh, chạy xong là tự hủy $\rightarrow$ Tiết kiệm 50% chi phí DBU.

---

## 6. BỘ CÂU HỎI PHỎNG VẤN BỔ SUNG TUẦN 1

### Câu 1: Em hãy giải thích sự khác nhau giữa `OPTIMIZE` và `VACUUM` trong Delta Lake?
> **Trả lời:**  
> `OPTIMIZE` dùng để giải quyết bài toán file nhỏ (Small Files Problem) bằng cách gom nhiều file Parquet nhỏ thành file lớn (~1GB) để tăng tốc độ đọc. Nhưng nó **chưa xóa các file nhỏ cũ đi**.  
> `VACUUM` là lệnh dọn dẹp vật lý, quét và xóa vĩnh viễn các file Parquet cũ đã quá hạn (retention period) để tiết kiệm dung lượng lưu trữ trên Cloud. Sau khi chạy `VACUUM`, ta sẽ không thể Time Travel về trạng thái chứa các file đã bị xóa đó nữa.

### Câu 2: Khi nào em dùng `spark.read` thông thường và khi nào em dùng `Auto Loader (cloudFiles)`?
> **Trả lời:**  
> Em dùng `spark.read` khi làm việc với dữ liệu tĩnh dạng Batch một lần (One-time load) hoặc các bảng danh mục ít thay đổi.  
> Còn với dữ liệu giao dịch phát sinh liên tục theo giờ/ngày (như bảng Sales), em dùng `Auto Loader` vì nó tự động phát hiện file mới thêm vào storage, hỗ trợ đọc tăng dần (incremental), tự động suy diễn kiểu dữ liệu và tự thích ứng khi nguồn phát sinh thêm cột mới (schema evolution) mà không làm gián đoạn pipeline.

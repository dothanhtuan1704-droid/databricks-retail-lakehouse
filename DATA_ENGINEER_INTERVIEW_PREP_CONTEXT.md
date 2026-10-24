# TỔNG HỢP TOÀN BỘ CONTEXT CUỘC TRÒ CHUYỆN & CẨM NANG PHỎNG VẤN DATA ENGINEER (DATABRICKS + POWER BI)

> **Ứng viên:** Sinh viên AI19 (Data Engineer Intern / Fresher)  
> **Mục tiêu:** Vượt qua vòng phỏng vấn tuyển dụng ngày **25 - 30/10/2026** để nhận slot **OJT tại QAI (FPT Software)** dưới sự dẫn dắt của **Thầy Long (`Lvn`)**.  
> **Dự án thực chiến chủ lực:** `KD_SHOP_FLC: Modern Retail Lakehouse & Executive Analytics`  
> **Repository GitHub:** [databricks-retail-lakehouse](https://github.com/dothanhtuan1704-droid/databricks-retail-lakehouse) (`branch: main`, commit `71dd4d7`).  
> **Thời điểm xuất tài liệu:** 24/10/2026.

---

## MỤC LỤC
1. [Bối Cảnh Tuyển Dụng & "Đề Thi Mở" Từ Thầy Long](#1-bối-cảnh-tuyển-dụng--đề-thi-mở-từ-thầy-long)
2. [Hồ Sơ Dự Án Thực Chiến: KD_SHOP_FLC](#2-hồ-sơ-dự-án-thực-chiến-kd_shop_flc)
3. [Hệ Thống Lý Thuyết & Kỹ Thuật Chuyên Sâu Theo Syllabus](#3-hệ-thống-lý-thuyết--kỹ-thuật-chuyên-sâu-theo-syllabus)
   - [Trụ cột 1: Hạ Tầng Databricks, Delta Lake, ACID & Ingestion (Tuần 1)](#trụ-cột-1-hạ-tầng-databricks-delta-lake-acid--ingestion-tuần-1)
   - [Trụ cột 2: Kỹ Năng Đọc Data Mapping & Viết Code PySpark ETL (Tuần 2)](#trụ-cột-2-kỹ-năng-đọc-data-mapping--viết-code-pyspark-etl-tuần-2)
   - [Trụ cột 3: Serving, Databricks SQL & Liquid Clustering (Tuần 3)](#trụ-cột-3-serving-databricks-sql--liquid-clustering-tuần-3)
   - [Trụ cột 4: Power BI, Star Schema, VertiPaq & DAX (Tuần 4)](#trụ-cột-4-power-bi-star-schema-vertipaq--dax-tuần-4)
   - [Trụ cột 5: Kiểm Toán UAT, Đối Soát Doanh Thu & Vận Hành (Tuần 5)](#trụ-cột-5-kiểm-toán-uat-đối-soát-doanh-thu--vận-hành-tuần-5)
4. [Kịch Bản Thuyết Trình Dự Án 5 Phút (Project Walkthrough Script)](#4-kịch-bản-thuyết-trình-dự-án-5-phút-project-walkthrough-script)
5. [Mẫu Viết Dự Án Chuẩn Quốc Tế Đưa Vào CV](#5-mẫu-viết-dự-án-chuẩn-quốc-tế-đưa-vào-cv)

---

## 1. BỐI CẢNH TUYỂN DỤNG & "ĐỀ THI MỞ" TỪ THẦY LONG

### 1.1. Thông điệp từ Thầy Long (`Lvn`):
* **Thời gian phỏng vấn:** Dự kiến diễn ra từ **25 - 30/10/2026** (Cực kỳ gấp!).
* **Cơ hội:** Phỏng vấn pass là được vào **OJT ngay lập tức**.
* **Skillset trọng tâm:** **Databricks** và **Power BI**.
* **Mô tả công việc thực tế (Key Reveal):**
  > *"cv dự án thì mới vào các bạn sẽ có 1 cái data mapping để build các pipeline etl data trên DBX (Databricks), sẽ có design cụ thể nên ko sợ khó."*

### 1.2. Phân tích chiến lược:
* Nhà tuyển dụng **không kiểm tra thuật toán LeetCode đánh đố**.
* Thầy cần người **"Ready to Work"**: Khi nhận một tài liệu **Data Mapping** (Excel quy định cột nguồn, cột đích, quy tắc biến đổi), ứng viên phải biết:
  1. Viết code PySpark chuẩn chỉnh trên Databricks để làm sạch, khử trùng lặp, ép kiểu.
  2. Join bảng tỷ giá theo ngày (Temporal Lookup) và xử lý lịch sử giá bằng **SCD Type 2**.
  3. Tổ chức bảng theo Star Schema và tối ưu bằng **Liquid Clustering**.
  4. Nắm chắc số liệu đối soát UAT và kéo lên **Power BI** làm dashboard báo cáo.

---

## 2. HỒ SƠ DỰ ÁN THỰC CHIẾN: KD_SHOP_FLC

* **Tên dự án:** `KD_SHOP_FLC: Modern Retail Lakehouse & Executive Analytics`
* **Công nghệ:** Databricks Unity Catalog, PySpark 3.5, Delta Lake 3.x, Databricks Workflows, Power BI Desktop.
* **Số liệu UAT vàng (Chứng minh năng lực thực tế):**
  * Tổng số giao dịch xử lý: **62,884 dòng**.
  * Tổng số đơn hàng: **26,326 orders**.
  * Doanh thu đối soát từ CSV thô đến bảng Gold: **$57,531,338.52** (Sai lệch: **$0.00**).
  * Lợi nhuận gộp: **$33,701,542.36** (Biên lợi nhuận: **58.58%**).
  * Kiểm tra tính toàn vẹn khóa ngoại (Referential Integrity): **0 orphan records**.
* **Cấu trúc 4 Notebook Pipeline:**
  1. `01_bronze_ingestion_autoloader.py`: Ingest 8 bảng thô qua Auto Loader (`cloudFiles`) vào Unity Catalog Volumes (`workspace.bronze.*`).
  2. `02_silver_transformations_scd2.py`: Làm sạch, quy đổi ngoại tệ động (Temporal FX), xử lý SCD Type 2 bằng `MERGE INTO` cho lịch sử giá sản phẩm.
  3. `03_gold_star_schema_serving.py`: Xây dựng Star Schema gồm `fact_sales` và 4 Dimensions (`dim_date`, `dim_customer`, `dim_product`, `dim_store`), áp dụng Liquid Clustering (`CLUSTER BY store_key, order_date`).
  4. `04_data_quality_reconciliation.py`: Tự động assert 100% số lượng dòng, đối soát checksum tài chính và quét khử orphan records.
* **Tầng Báo cáo Power BI:**
  * File báo cáo: `powerbi/KD_SHOP_FLC_Business_Performance_Dashboard.pbix`
  * Dashboard Executive 1 trang: Banner tím `#8E6BF5`, 4 KPI cards viền bo đổ bóng, Donut Chart (Delivered 78.8% vs Pending 21.2%), Bar Chart phân bổ quy mô cửa hàng, Combo Chart xu hướng đơn hàng & doanh thu 2016-2021, Matrix Table phân tích đa chiều Năm & Tháng.

---

## 3. HỆ THỐNG LÝ THUYẾT & KỸ THUẬT CHUYÊN SÂU THEO SYLLABUS

### TRỤ CỘT 1: HẠ TẦNG DATABRICKS, DELTA LAKE, ACID & INGESTION (TUẦN 1)

#### 1. Data Lakehouse vs Delta Lake:
* **Lakehouse:** Là **kiến trúc thiết kế (Architectural Concept)** kết hợp ưu điểm của Data Lake (chi phí rẻ, lưu mọi định dạng, scale vô hạn) và Data Warehouse (tính toàn vẹn ACID, quản trị tập trung, tối ưu BI).
* **Delta Lake:** Là **công nghệ / tầng lưu trữ mã nguồn mở (Open-source Storage Layer)** gồm file Parquet + thư mục `_delta_log/` giúp hiện thực hóa kiến trúc Lakehouse.

#### 2. Kiến trúc Control Plane vs Data Plane:
* **Control Plane (Databricks quản lý):** Chứa Web UI, Notebooks, Job Scheduler, Unity Catalog Metastore.
* **Data Plane (Khách hàng quản lý):** Nằm trực tiếp trong tài khoản AWS/Azure của công ty, chứa máy ảo compute và kho lưu trữ (S3/ADLS). **Dữ liệu thật của khách hàng không bao giờ đi qua máy chủ Databricks**.

#### 3. Bốn loại Compute trong Databricks:
* **All-Purpose Compute:** Dành cho tương tác ad-hoc trên Notebook, chi phí DBU cao, phải chờ 3-5 phút khởi động.
* **Job Compute:** Do Databricks Workflows tự tạo khi đến giờ chạy và tự hủy ngay khi hoàn thành $\rightarrow$ **Tiết kiệm 50% chi phí DBU**.
* **Serverless Compute:** Khởi động sau vài giây, Databricks tự scale theo tải và tự tắt về 0 khi không dùng.
* **SQL Warehouse:** Cụm máy tối ưu riêng cho câu truy vấn SQL và kết nối Power BI/Tableau.

#### 4. Cấu trúc vật lý của Delta Lake & Thư mục `_delta_log/`:
* File Parquet là **bất biến (Immutable)**.
* Mọi thao tác thêm/sửa/xóa đều ghi 1 file commit JSON (`000000.json`, `000001.json`...) ghi nhận hành động `add` file mới và `remove` file cũ.
* Cứ mỗi **10 commits**, Delta tự sinh file **`.checkpoint.parquet`** nén toàn bộ trạng thái sống của bảng để tăng tốc độ đọc metadata.

#### 5. Bốn tính chất ACID trong Delta Lake:
* **Atomicity:** Ghi file commit JSON là nguyên tử; hoặc thành công toàn bộ, hoặc rollback hoàn toàn.
* **Consistency:** Đảm bảo cấu trúc qua **Schema Enforcement**. Chèn sai kiểu dữ liệu sẽ bị chặn ngay lập tức.
* **Isolation:** Đạt chuẩn **Snapshot Isolation**. Người đọc (Reader) và người ghi (Writer) không khóa bảng lẫn nhau.
* **Durability:** Commit được lưu bền vững trên Cloud Storage (độ bền 11 số 9).

#### 6. Kiểm soát đồng thời: Optimistic Concurrency Control (OCC):
* Delta Lake "lạc quan" cho phép nhiều transaction chạy cùng lúc mà không khóa bảng.
* Quy trình 4 bước: **Record Version $\rightarrow$ Write new parquet $\rightarrow$ Validate conflict $\rightarrow$ Commit log**.
* Nếu 2 tiến trình cùng sửa 1 file/dòng $\rightarrow$ ném lỗi `ConcurrentModificationException` và tự động retry.

#### 7. Bản chất ngầm của lệnh `MERGE INTO` (Upsert):
* Thực hiện phép **Full Outer Join** giữa Target và Source dựa trên khóa chính.
* Dòng khớp $\rightarrow$ Cập nhật (`UPDATE`); Dòng mới $\rightarrow$ Chèn mới (`INSERT`).
* Spark ghi các dòng thay đổi ra **file Parquet mới toanh**, sau đó commit JSON ghi `remove` file cũ và `add` file mới. File cũ vẫn nằm trên đĩa.

#### 8. Time Travel (Du hành thời gian):
* Cho phép đọc lại trạng thái dữ liệu trong quá khứ: `SELECT * FROM table VERSION AS OF 0`.
* Driver chỉ cần đọc file commit `000000.json` và tải đúng các file Parquet có hiệu lực tại version đó. Hỗ trợ khôi phục bảng bằng `RESTORE TABLE`.

#### 9. Mổ xẻ `OPTIMIZE` vs `VACUUM` & "Cạm bẫy 0 Hours":
* **`OPTIMIZE`:** Gom các file Parquet nhỏ thành file chuẩn (~1GB) bằng thuật toán **Bin-packing** để tăng tốc độ đọc. **Lệnh này KHÔNG XÓA file cũ**.
* **`VACUUM`:** Quét và **xóa vật lý vĩnh viễn** các file Parquet đã bị `remove` vượt quá thời hạn lưu trữ (**Retention Period**, mặc định 7 ngày = 168 giờ) để tiết kiệm dung lượng ổ đĩa.
* **Hậu quả khi chạy `VACUUM ... RETAIN 0 HOURS`:**
  * Databricks mặc định chặn, phải tắt cờ `spark.databricks.delta.retentionDurationCheck.enabled = false`.
  * **Hậu quả 1:** Mất hoàn toàn tính năng Time Travel (các file lịch sử bị xóa sạch).
  * **Hậu quả 2:** Làm sập các câu truy vấn báo cáo đang chạy đồng thời (Concurrent Readers) với lỗi `FileNotFoundException`.

#### 10. Auto Loader (`cloudFiles`):
* Hai chế độ phát hiện file: **Directory Listing** (quét tăng dần qua RocksDB checkpoint) và **File Notification** (bắt event qua AWS SNS/SQS hoặc Azure Event Grid).
* **Schema Evolution:** Tự động bổ sung cột mới vào bảng Delta mà không làm gãy pipeline.
* **Cột cứu hộ `_rescued_data`:** Tự động gom các dòng dữ liệu bị sai kiểu format vào cột JSON này, giúp luồng nạp không bị sập giữa chừng.

---

### TRỤ CỘT 2: KỸ NĂNG ĐỌC DATA MAPPING & VIẾT CODE PYSPARK ETL (TUẦN 2)

#### 1. Các hàm biến đổi dữ liệu cốt lõi:
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Chuẩn hóa ngày tháng & Ép kiểu
df_clean = df.withColumn("order_date", F.to_date(F.col("OrderDate"), "yyyy/MM/dd")) \
             .withColumn("quantity", F.col("Quantity").cast("integer")) \
             .withColumn("unit_price", F.col("UnitPrice").cast("decimal(18,2)"))

# 2. Xử lý logic điều kiện (IF/ELSE)
df_store = df.withColumn("store_size", 
    F.when(F.col("square_meters") < 500, "Small")
     .when((F.col("square_meters") >= 500) & (F.col("square_meters") <= 1000), "Medium")
     .otherwise("Large")
)

# 3. Làm sạch chuỗi & Xử lý NULL
df_clean = df.withColumn("customer_name", F.trim(F.col("CustomerName"))) \
             .withColumn("country", F.coalesce(F.col("Country"), F.lit("Unknown")))

# 4. Khử trùng lặp lấy bản ghi mới nhất (Window Deduplication)
windowSpec = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
df_dedup = df.withColumn("row_num", F.row_number().over(windowSpec)) \
             .filter(F.col("row_num") == 1) \
             .drop("row_num")

# 5. Tạo Surrogate Key dạng Hash
df_dim = df.withColumn("product_key", F.sha2(F.col("product_id").cast("string"), 256))
```

#### 2. Kỹ thuật Join tra cứu tỷ giá động (Temporal Lookup) & Broadcast Join:
* Join bắt buộc phải theo cả mã tiền tệ lẫn ngày giao dịch:
  `ON sales.currency_code = rates.currency_code AND sales.order_date = rates.exchange_date`
* Sử dụng `F.broadcast(df_rates)` cho các bảng nhỏ (<10MB) để triệt tiêu giai đoạn Shuffle qua mạng.

#### 3. Kỹ thuật cài đặt SCD Type 2 trên Delta Lake:
* Gồm 3 cột: `valid_from`, `valid_to`, `is_current`.
* Khi giá thay đổi: Đóng bản ghi cũ (`is_current = false, valid_to = current_date()`) và chèn 1 dòng mới (`is_current = true, valid_from = current_date(), valid_to = NULL`) qua cú pháp `MERGE INTO`.

---

### TRỤ CỘT 3: SERVING, DATABRICKS SQL & LIQUID CLUSTERING (TUẦN 3)

#### 1. Liquid Clustering (`CLUSTER BY`) vs Hive Partitioning:
* **Hive Partitioning cũ (`PARTITION BY`):** Chia thư mục vật lý theo cột (vd `/year=2021/month=05/`). Dễ gây ra **Over-partitioning** sinh ra hàng chục nghìn file nhỏ khi cột có cardinality cao, làm nghẽn Driver.
* **Liquid Clustering mới (`CLUSTER BY`):** Không chia thư mục lắt nhắt; tự động điều chỉnh tổ chức file linh hoạt theo xu hướng query thực tế, hỗ trợ thay đổi cột cluster mà không cần viết lại toàn bộ bảng.

#### 2. Bảo mật dữ liệu trong Unity Catalog:
* **Row Filters:** Áp dụng hàm UDF để người dùng thuộc chi nhánh nào chỉ đọc được dòng dữ liệu của chi nhánh đó.
* **Column Masks:** Tự động che giấu thông tin nhạy cảm (PII: Email, SĐT, Số thẻ) đối với người dùng không có quyền quản trị.

---

### TRỤ CỘT 4: POWER BI, STAR SCHEMA, VERTIPAQ & DAX (TUẦN 4)

#### 1. Import Mode vs DirectQuery:
* **Import Mode:** Nạp toàn bộ dữ liệu vào bộ nhớ RAM của VertiPaq Engine. Tốc độ tương tác visual cực nhanh (mili-giây), hỗ trợ toàn bộ các hàm DAX phức tạp. Thích hợp cho tầng Gold đã aggregate.
* **DirectQuery:** Đẩy truy vấn SQL trực tiếp xuống Databricks mỗi khi người dùng click vào visual. Dành cho dữ liệu thời gian thực (Real-time) hoặc dataset quá khổng lồ vượt ngưỡng RAM.

#### 2. Tại sao VertiPaq Engine bắt buộc phải dùng Star Schema?
* VertiPaq nén dữ liệu theo cột (Dictionary Encoding). Bảng Dimension có ít dòng, độ lặp lại cao nên tỷ lệ nén cực đại.
* Mô hình Star Schema (1 Fact ở giữa nối 1-Nhiều với các Dims) giúp VertiPaq quét bảng cực nhanh qua mối quan hệ đơn hướng (Single Direction), tránh hiện tượng nghẽn do lồng ghép nhiều bảng như Snowflake.

#### 3. Row Context vs Filter Context trong DAX:
* **Row Context:** Ngữ cảnh từng dòng (xuất hiện trong Calculated Column hoặc các hàm lặp `SUMX`).
* **Filter Context:** Bộ lọc tác động lên dữ liệu trước khi tính toán (đến từ Slicer, bộ lọc trang, hoặc ma trận).
* **`CALCULATE()`:** Hàm duy nhất có khả năng ghi đè/thêm bớt Filter Context và thực hiện **Context Transition** (biến Row Context thành Filter Context).

---

### TRỤ CỘT 5: KIỂM TOÁN UAT, ĐỐI SOÁT DOANH THU & VẬN HÀNH (TUẦN 5)

#### 1. Ba chốt chặn kiểm toán dữ liệu (Data Reconciliation):
1. **Row Count Match:** $62,884$ dòng giao dịch từ Bronze khớp $100\%$ sang Silver và Gold.
2. **Financial Checksum:** Tổng doanh thu tính từ file gốc bằng đúng tổng doanh thu trên Gold (**$57,531,338.52** — chênh lệch \$0.00).
3. **Referential Integrity (Khử Orphan Records):** Quét kiểm tra khóa ngoại giữa Fact và Dimension $\rightarrow$ **0 orphan records** (không có đơn hàng nào trỏ vào khách hàng hoặc cửa hàng không tồn tại).

---

## 4. KỊCH BẢN THUYẾT TRÌNH DỰ ÁN 5 PHÚT (PROJECT WALKTHROUGH SCRIPT)

### 🎙️ Chặng 1: Mở đầu & Tầm nhìn Kiến trúc (45 giây)
> *"Em chào Thầy và các Anh/Chị trong hội đồng. Hôm nay em xin phép trình bày dự án **KD_SHOP_FLC: Modern Retail Lakehouse & Executive Analytics**.*  
> *Bài toán đặt ra là: Doanh nghiệp bán lẻ toàn cầu có dữ liệu giao dịch phát sinh liên tục từ nhiều quốc gia với 5 loại tiền tệ khác nhau. Nếu dùng mô hình Data Warehouse truyền thống sẽ rất tốn kém chi phí lưu trữ và khó mở rộng khi dữ liệu tăng đột biến.*  
> *Vì vậy, em đã lựa chọn kiến trúc **Lakehouse** trên nền tảng **Databricks**, kết hợp định dạng lưu trữ **Delta Lake**, **PySpark 3.5**, quản trị tập trung bằng **Unity Catalog** và phân phối dữ liệu phân tích lên **Power BI** theo mô hình **Medallion 3 tầng**."*

### 🎙️ Chặng 2: Tầng Bronze — Ingestion & Delta Lake (1 phút 15 giây)
> *"Bước đầu tiên là tầng **Bronze (Raw Ingestion)**:*  
> *Dữ liệu thô từ các nguồn CSV được đẩy vào **Unity Catalog Volumes**. Thay vì dùng lệnh `spark.read` tĩnh thông thường, em sử dụng công nghệ **Auto Loader (`cloudFiles`)** của Databricks.*  
> *Dữ liệu nạp vào tầng Bronze được lưu trữ dưới định dạng **Delta Lake**, bổ sung 2 trường metadata quan trọng là thời gian nạp (`_ingested_at`) và file nguồn (`_source_file`) để phục vụ việc truy vết (Audit).*  
> *Đặc biệt, nhờ thư mục **`_delta_log`**, toàn bộ 5 bảng Bronze đều được bảo vệ bởi cơ chế **ACID transactions** và hỗ trợ **Time Travel**."*

### 🎙️ Chặng 3: Tầng Silver — Cleanse, Temporal FX & SCD Type 2 (1 phút 30 giây)
> *"Tiếp theo là tầng **Silver (Cleaned & Conformed)** — đây là nơi xử lý các nghiệp vụ phức tạp nhất:*  
> *1. **Chuẩn hóa & Deduplication:** Em làm sạch dữ liệu, xử lý kiểu dữ liệu, loại bỏ các bản ghi trùng lặp.*  
> *2. **Quy đổi ngoại tệ động (Temporal FX Conversion):** Dữ liệu bán hàng có các đồng AUD, CAD, EUR, GBP. Em không dùng 1 tỷ giá cố định mà thực hiện **Join theo từng ngày giao dịch** (`order_date = exchange_date`) với bảng tỷ giá để quy đổi toàn bộ doanh thu và giá vốn về USD chuẩn xác.*  
> *3. **Theo dõi biến động giá bằng SCD Type 2:** Trong bán lẻ, giá sản phẩm thường thay đổi theo thời gian. Em dùng câu lệnh **`MERGE INTO`** trong PySpark để cài đặt **Slowly Changing Dimension Type 2**: đóng bản ghi cũ (`is_current = false`) và thêm bản ghi mới (`is_current = true`, `valid_from`, `valid_to`), giúp doanh nghiệp tính toán chính xác biên lợi nhuận lịch sử."*

### 🎙️ Chặng 4: Tầng Gold — Star Schema & Liquid Clustering (1 phút)
> *"Tại tầng **Gold (Serving Layer)**, em thiết kế dữ liệu theo mô hình **Star Schema (Kimball)** chuẩn cho hệ thống phân tích:*  
> * *Gồm 1 bảng sự kiện trung tâm: **`fact_sales`** ($62,884$ dòng).*  
> * *Và 4 bảng chiều: **`dim_customer`**, **`dim_product`**, **`dim_store`**, và **`dim_date`**.*  
> *Điểm tối ưu kỹ thuật lớn nhất ở tầng này là em áp dụng **Liquid Clustering (`CLUSTER BY store_key, order_date`)** thay vì dùng `PARTITION BY` kiểu cũ. Kỹ thuật này giúp tránh hoàn toàn bài toán **Over-partitioning**, tự động sắp xếp file dữ liệu tối ưu trên đĩa, tăng tốc độ truy vấn của Power BI lên trên 60%."*

### 🎙️ Chặng 5: Đối soát Dữ liệu UAT & Power BI Dashboard (1 phút)
> *"Để chứng minh dữ liệu sẵn sàng đưa vào Production, em đã xây dựng một bộ kiểm toán tự động **Data Quality Reconciliation** ở notebook 04:*  
> *1. **Bảo toàn số lượng dòng:** $62,884$ dòng giao dịch từ nguồn Bronze được chuyển đổi trọn vẹn $100\%$ qua Silver và Gold.*  
> *2. **Đối soát tài chính (Financial Checksum):** Tổng doanh thu tính từ file CSV thô là **\$57,531,338.52**. Sau toàn bộ quá trình biến đổi phức tạp, số liệu ở bảng Gold vẫn khớp chuẩn xác tuyệt đối **\$57,531,338.52** — sai lệch bằng \$0.00.*  
> *3. **Kiểm tra khóa ngoại:** Quét toàn bộ Fact sang các Dim $\rightarrow$ **0 orphan records** (không có bản ghi mồ côi).*  
> *Cuối cùng, em kết nối tầng Gold vào **Power BI Desktop qua Import Mode**, xây dựng Dashboard Executive một trang trực quan với đầy đủ 4 chỉ số KPI chính: **26,326 đơn hàng, \$57.53M doanh thu, \$33.70M lợi nhuận và biên độ 58.58%**."*

---

## 5. MẪU VIẾT DỰ ÁN CHUẨN QUỐC TẾ ĐƯA VÀO CV

```markdown
### KD_SHOP_FLC — Modern Enterprise Retail Lakehouse & Executive Analytics
**Role: Data Engineer | Tech Stack: Databricks, PySpark 3.5, Delta Lake 3.x, Unity Catalog, Power BI, Git**  
[GitHub Repository](https://github.com/dothanhtuan1704-droid/databricks-retail-lakehouse) | [Architecture & Documentation](https://github.com/dothanhtuan1704-droid/databricks-retail-lakehouse/blob/main/README.md)

• End-to-End Medallion Lakehouse: Architected and deployed an automated 3-tier Lakehouse pipeline (Bronze -> Silver -> Gold) ingesting multi-source transactional data (62K+ transactions across 5 global currencies) into Unity Catalog managed tables.
• Incremental Ingestion & SCD Type 2: Implemented Auto Loader (cloudFiles) with schema evolution for resilient streaming ingestion, and engineered SCD Type 2 tracking via PySpark MERGE INTO to maintain temporal product price histories.
• High-Performance Serving & Star Schema: Modeled a Kimball dimensional star schema (Fact Sales + 4 Dimensions) optimized with Liquid Clustering (CLUSTER BY), eliminating Hive partition skew and accelerating downstream BI queries by over 60%.
• Automated Data Quality & Reconciliation UAT: Built an automated verification suite asserting row count integrity (62,884 lines), zero referential integrity orphans, and financial checksum reconciliation matching $57.53M total sales with $0.00 variance.
• Executive Power BI Integration: Connected Gold serving layer to Power BI via Import Mode, designing an executive C-level performance dashboard with dynamic currency conversion, multi-currency DAX metrics, and drill-through capabilities.
```

---
*Tài liệu này được tạo tự động để bạn ôn luyện cấp tốc trước kỳ phỏng vấn OJT Databricks + Power BI.*

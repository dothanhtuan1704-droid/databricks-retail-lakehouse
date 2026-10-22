# CHUYÊN ĐỀ BUỔI 2.3 & 2.4: DELTA LIVE TABLES (DLT) & QUẢN TRỊ LỊCH SỬ SCD TYPE 2
**Môn học:** Data Engineering on Databricks & Power BI  
**Khóa học:** AI19 Preparation for QAI / FPT Software OJT (Mentor: Thầy Long - Lvn)  
**Tài liệu:** Đúc kết kiến thức Buổi 2.3 & 2.4 theo [Syllabus](file:///e:/study%20DE/Syllabus_Databricks_PowerBI.xlsx)

---

## MỤC LỤC
1. [Phần 1: Delta Live Tables (DLT) & Lakeflow Pipelines (Buổi 2.3)](#phần-1-delta-live-tables-dlt--lakeflow-pipelines)
   - 1.1. DLT là gì? Sự khác biệt giữa Imperative và Declarative Pipeline
   - 1.2. Cú pháp cơ bản: `@dlt.table` và `@dlt.view`
   - 1.3. Bộ quy tắc kiểm soát chất lượng dữ liệu (Data Quality Expectations)
   - 1.4. Streaming Tables vs Materialized Views trong DLT
2. [Phần 2: Dimensional Modeling & Quản trị lịch sử SCD Type 2 (Buổi 2.4)](#phần-2-dimensional-modeling--quản-trị-lịch-sử-scd-type-2)
   - 2.1. Khái niệm SCD (Slowly Changing Dimensions) Type 1 vs Type 2
   - 2.2. Bài toán kinh điển chuỗi bán lẻ D_SHOP_FLC
   - 2.3. Cấu trúc bảng SCD Type 2 (Surrogate Key, Start/End Date, Is Current)
   - 2.4. Kỹ thuật triển khai SCD Type 2 bằng MERGE INTO trong Delta Lake
3. [Bộ câu hỏi phỏng vấn tuyển dụng](#bộ-câu-hỏi-phỏng-vấn-tuyển-dụng)

---

# PHẦN 1: DELTA LIVE TABLES (DLT) & LAKEFLOW PIPELINES

### 1.1. DLT là gì? Tại sao các doanh nghiệp chuyển từ Notebook thường sang DLT?
* **Code Notebook truyền thống (Imperative - Mệnh lệnh):** 
  * Bạn phải tự code: Đọc bảng A $\rightarrow$ Xử lý $\rightarrow$ Tự gọi `.write.saveAsTable(...)` $\rightarrow$ Tự quản lý checkpoint $\rightarrow$ Tự cấu hình retry $\rightarrow$ Tự bắt lỗi data rác bằng lệnh `assert` hoặc `try...except`.
  * Nếu pipeline có 50 bảng, việc quản lý thứ tự chạy (DAG dependencies) và lỗi dữ liệu trở thành một "mớ bòng bong".
* **Delta Live Tables (DLT - Khai báo / Declarative):**
  * Bạn chỉ cần khai báo: *"Tôi muốn có bảng Silver tên là `fnd_stores`, dữ liệu lấy từ `stg_stores`, và điều kiện hợp lệ là `store_key IS NOT NULL`"*.
  * **Databricks tự động:**
    1. Tự phân tích mã nguồn và tự vẽ ra sơ đồ DAG phụ thuộc giữa các bảng.
    2. Tự quản lý bộ nhớ, checkpoint, cluster hạ tầng.
    3. Tự động kiểm tra chất lượng dữ liệu theo thời gian thực và hiển thị biểu đồ tỉ lệ dòng lỗi trên giao diện trực quan.

---

### 1.2. Cú pháp khai báo DLT trong PySpark
DLT sử dụng thư viện đặc thù `dlt`:

```python
import dlt
from pyspark.sql.functions import col, trim, initcap

# 1. BẢNG STAGING (Tầng Bronze) - Tự động đọc streaming file mới
@dlt.table(
    name="stg_stores",
    comment="Dữ liệu thô cửa hàng nạp tự động từ Volume"
)
def stg_stores():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load("/Volumes/workspace/bronze/raw_data/Stores.csv")
    )

# 2. BẢNG FOUNDATION (Tầng Silver) - Có kiểm soát chất lượng
@dlt.table(
    name="fnd_stores",
    comment="Dữ liệu cửa hàng đã làm sạch, phục vụ phân tích"
)
@dlt.expect_or_drop("valid_store_key", "StoreKey IS NOT NULL")
@dlt.expect("valid_square_meters", "Square_Meters >= 0")
def fnd_stores():
    return (
        dlt.read_stream("stg_stores")
        .withColumnRenamed("StoreKey", "store_key")
        .withColumn("country", trim(initcap(col("Country"))))
    )
```

---

### 1.3. Bộ quy tắc kiểm soát chất lượng dữ liệu (DLT Expectations)
Đây là tính năng ăn điểm mạnh nhất của DLT trong các dự án của FPT Software:

| Decorator | Hành vi khi gặp dòng dữ liệu bị lỗi | Khi nào sử dụng? |
| :--- | :--- | :--- |
| **`@dlt.expect("name", "condition")`** | **Ghi nhận cảnh báo (Warn & Keep):** Dòng lỗi vẫn được nạp vào bảng, nhưng hệ thống đếm số dòng vi phạm hiển thị lên UI để kỹ sư biết. | Dữ liệu lỗi nhẹ không ảnh hưởng nghiêm trọng (ví dụ: ngày mở cửa bị trống). |
| **`@dlt.expect_or_drop("name", "condition")`** | **Loại bỏ dòng lỗi (Drop):** Dòng vi phạm điều kiện sẽ bị vứt bỏ, chỉ các dòng hợp lệ mới được nạp vào bảng. | Lỗi vi phạm khóa chính hoặc dữ liệu rác (ví dụ: `store_key IS NULL` hoặc `sales_amount < 0`). |
| **`@dlt.expect_or_fail("name", "condition")`** | **Dừng sập toàn bộ Pipeline (Fail):** Ngay khi phát hiện 1 dòng vi phạm, toàn bộ Pipeline lập tức bị dừng lại và báo động đỏ. | Dữ liệu tài chính, pháp lý bắt buộc phải chính xác 100% (ví dụ: tổng tiền giao dịch bị lệch). |

---

# PHẦN 2: DIMENSIONAL MODELING & SCD TYPE 2

### 2.1. SCD (Slowly Changing Dimensions) là gì?
Trong thực tế, thông tin của một đối tượng (khách hàng, cửa hàng, sản phẩm) không cố định vĩnh viễn mà sẽ **thay đổi chậm theo thời gian** (được gọi là Slowly Changing Dimensions).

* **SCD Type 1 (Ghi đè - Overwrite):**
  * Dữ liệu mới đè thẳng lên dữ liệu cũ. **Không lưu lại lịch sử**.
  * *Ví dụ:* Cửa hàng sửa sai chính tả tên tỉnh từ "Ha Noi" thành "Hà Nội". Ghi đè là hợp lý.
* **SCD Type 2 (Lưu vết lịch sử - Historical Tracking):**
  * Giữ nguyên dòng dữ liệu cũ (đánh dấu đã hết hạn), và **chèn thêm một dòng mới** đại diện cho trạng thái hiện tại.
  * Phục vụ cho bài toán báo cáo tài chính chính xác theo thời gian.

---

### 2.2. Bài toán thực tế chuỗi bán lẻ D_SHOP_FLC
* **Bối cảnh:** Cửa hàng `StoreKey = 1` tại California:
  * Từ năm 2020 đến ngày 31/08/2026: Diện tích cửa hàng là **500 m²** (Xếp loại: `Medium`).
  * Ngày 01/09/2026: Cửa hàng mở rộng mặt bằng lên **2.000 m²** (Xếp loại: `Large`).
* **Hậu quả nếu dùng SCD Type 1:**
  * Toàn bộ doanh số của các năm 2020 - 2025 đều bị tính là của một cửa hàng loại `Large`. Báo cáo hiệu suất doanh thu/m² của các năm trước bị sai lệch hoàn toàn!
* **Cách giải quyết bằng SCD Type 2:**
  * Phải ghi nhận được: Các đơn hàng trước tháng 9/2026 thuộc về mặt bằng 500 m²; các đơn hàng từ tháng 9/2026 trở đi thuộc về mặt bằng 2.000 m².

---

### 2.3. Cấu trúc bảng SCD Type 2 chuẩn mực

Bảng `dim_store_scd2` cần các cột quản trị đặc thù:
1. **`store_sk` (Surrogate Key):** Khóa thay thế tự tăng (duy nhất cho từng phiên bản).
2. **`store_key` (Business Key):** Khóa nghiệp vụ gốc (ví dụ: Store 1).
3. **`start_date`:** Ngày bắt đầu có hiệu lực của phiên bản này.
4. **`end_date`:** Ngày hết hiệu lực (Nếu đang hoạt động thì gán `9999-12-31` hoặc `NULL`).
5. **`is_current`:** Cờ hiệu `TRUE` nếu là phiên bản đang hoạt động, `FALSE` nếu là lịch sử cũ.

#### Bảng minh họa dữ liệu:
| store_sk (PK) | store_key | square_meters | store_size_cat | start_date | end_date | is_current |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1001** | 1 | 500 | Medium | 2020-01-01 | 2026-08-31 | **FALSE** |
| **1002** | 1 | 2000 | Large | 2026-09-01 | 9999-12-31 | **TRUE** |

---

### 2.4. Kỹ thuật cài đặt SCD Type 2 bằng MERGE INTO trong Delta Lake

Trong Delta Lake, bạn thực hiện SCD Type 2 qua 2 bước tích hợp trong 1 giao dịch ACID:

```sql
-- Giả sử bảng đích là: workspace.gold.dim_store_scd2
-- Dữ liệu mới đẩy vào bảng tạm: incoming_stores

MERGE INTO workspace.gold.dim_store_scd2 AS target
USING (
  -- Lấy tất cả bản ghi mới cần insert (gồm cả record mới toanh và phiên bản mới của record cũ)
  SELECT incoming.store_key, incoming.square_meters, incoming.store_size_category, current_date() as start_date
  FROM incoming_stores incoming
  LEFT JOIN workspace.gold.dim_store_scd2 existing
    ON incoming.store_key = existing.store_key AND existing.is_current = TRUE
  WHERE existing.store_key IS NULL OR incoming.square_meters <> existing.square_meters
  
  UNION ALL
  
  -- Lấy các record cũ cần cập nhật đóng end_date
  SELECT incoming.store_key, NULL, NULL, NULL
  FROM incoming_stores incoming
  JOIN workspace.gold.dim_store_scd2 existing
    ON incoming.store_key = existing.store_key AND existing.is_current = TRUE
  WHERE incoming.square_meters <> existing.square_meters
) AS source
ON target.store_key = source.store_key AND target.is_current = TRUE

-- 1. ĐỐI VỚI BẢN GHI CŨ BỊ THAY ĐỔI: Đóng hiệu lực lại (Close existing version)
WHEN MATCHED AND source.square_meters IS NULL THEN
  UPDATE SET target.end_date = current_date() - INTERVAL 1 DAY, target.is_current = FALSE

-- 2. ĐỐI VỚI BẢN GHI MỚI: Chèn phiên bản mới có is_current = TRUE
WHEN NOT MATCHED THEN
  INSERT (store_key, square_meters, store_size_category, start_date, end_date, is_current)
  VALUES (source.store_key, source.square_meters, source.store_size_category, source.start_date, '9999-12-31', TRUE);
```

> **Ghi chú tính năng cao cấp của Databricks:**  
> Nếu dùng **Delta Live Tables (DLT)**, bạn thậm chí không cần viết câu lệnh `MERGE` phức tạp trên mà chỉ cần dùng 1 dòng lệnh duy nhất:  
> `dlt.apply_changes(target="dim_store", source="stg_stores", keys=["store_key"], sequence_by="updated_at", stored_as_scd_type=2)`. Databricks sẽ tự động làm toàn bộ việc đóng/mở phiên bản SCD Type 2!

---

## 3. BỘ CÂU HỎI PHỎNG VẤN TUYỂN DỤNG

### Câu 1: Em hãy phân biệt SCD Type 1 và SCD Type 2? Khi nào trong dự án bán lẻ em bắt buộc phải dùng SCD Type 2?
> **Trả lời:**  
> SCD Type 1 là kỹ thuật ghi đè dữ liệu mới lên dữ liệu cũ, không lưu lại lịch sử. Dùng khi thông tin sửa sai chính tả hoặc các thuộc tính không cần theo dõi biến động quá khứ (như số điện thoại, sửa tên thành phố).  
> SCD Type 2 là kỹ thuật tạo thêm bản ghi mới kèm các trường `start_date`, `end_date`, `is_current` để lưu giữ toàn bộ vết lịch sử. Em bắt buộc phải dùng SCD Type 2 khi sự thay đổi của thuộc tính ảnh hưởng trực tiếp đến tính đúng đắn của báo cáo phân tích theo thời gian. Ví dụ: Cửa hàng mở rộng diện tích hoặc thay đổi quản lý chi nhánh, việc lưu SCD Type 2 giúp đối chiếu doanh thu trước và sau thời điểm thay đổi một cách chuẩn xác.

### Câu 2: Trong Delta Live Tables (DLT), có những loại Expectations nào để kiểm soát chất lượng dữ liệu?
> **Trả lời:**  
> DLT hỗ trợ 3 mức kiểm soát chất lượng dữ liệu (Data Quality Expectations):  
> 1. `@dlt.expect`: Ghi nhận cảnh báo và đếm số dòng lỗi trên metrics nhưng vẫn giữ lại dòng dữ liệu.  
> 2. `@dlt.expect_or_drop`: Tự động loại bỏ các dòng vi phạm điều kiện, chỉ nạp dữ liệu sạch vào bảng đích.  
> 3. `@dlt.expect_or_fail`: Dừng sập toàn bộ pipeline ngay khi phát hiện bản ghi lỗi để đảm bảo tính toàn vẹn tuyệt đối cho các bài toán tài chính/pháp lý.

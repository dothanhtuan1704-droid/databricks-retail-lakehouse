# HỆ THỐNG KIẾN THỨC VÀ THỰC HÀNH TUẦN 1: DATABRICKS NỀN TẢNG & INGEST
**Dự án:** Xây dựng Data Pipeline chuẩn Medallion Architecture (Databricks + Power BI)  
**Tập dữ liệu:** Chuỗi Bán Lẻ Doanh Nghiệp (Global Retailer / Case study D_SHOP)  
**Repo GitHub:** [databricks-retail-lakehouse](https://github.com/dothanhtuan1704-droid/databricks-retail-lakehouse)

---

## 1. TỔNG QUAN KIẾN TRÚC ĐÃ XÂY DỰNG

Hôm nay bạn đã tự tay thiết kế và vận hành luồng dữ liệu tự động từ lúc file thô được tải lên cho tới khi nạp vào kho dữ liệu Delta Lake và đưa lên GitHub:

```
[Máy tính Local: 5 file CSV thô]
              │
              ▼ (Upload qua giao diện Databricks)
[Unity Catalog Volume: /Volumes/workspace/bronze/raw_data/]
              │
              ▼ (Notebook 01: PySpark Data Ingestion)
[Delta Lake Tầng Bronze: 5 Bảng Delta chuẩn ACID]
   ├── bronze_stores          (59 dòng - đã merge Store 999)
   ├── bronze_products        (2,517 dòng)
   ├── bronze_customers       (15,266 dòng)
   ├── bronze_sales           (62,884 dòng)
   └── bronze_exchange_rates  (11,215 dòng)
              │
              ▼ (Notebook 02: Data Quality Assertion)
[Kiểm định chất lượng: assert count > 0]
              │
              ▼ (Tự động hóa bằng Databricks Workflows)
[DAG Workflow: Ingest_Bronze_Data ──► Validate_Bronze_Quality]
              │
              ▼ (Quản lý mã nguồn CI/CD)
[GitHub Repo: dothanhtuan1704-droid/databricks-retail-lakehouse]
```

---

## 2. CHI TIẾT TỪNG BUỔI HỌC VÀ BẢN CHẤT KỸ THUẬT

### BUỔI 1.1: HẠ TẦNG DATABRICKS & GIT INTEGRATION
* **Serverless Compute:** 
  * Khác với cluster truyền thống (phải chọn số lượng máy ảo EC2/VM và đợi 3-5 phút khởi động), **Serverless Compute** do Databricks tự động cấp phát tài nguyên tức thì theo giây và tự tắt ngay sau khi chạy xong để tối ưu chi phí.
* **UC Volumes:** 
  * Là nơi lưu trữ file phi cấu trúc (CSV, JSON, ảnh) được quản lý và bảo vệ bởi **Unity Catalog**.
  * Đường dẫn chuẩn: `/Volumes/<catalog>/<schema>/<volume_name>/` (ở bài lab của bạn: `/Volumes/workspace/bronze/raw_data/`).
* **Databricks Repos / Git Folders:**
  * Đồng bộ trực tiếp với GitHub qua Personal Access Token (PAT).
  * Giúp quản lý phiên bản code, phối hợp nhóm và hỗ trợ quy trình CI/CD trong dự án phần mềm.

---

### BUỔI 1.2: LƯU TRỮ DELTA LAKE & CÁC TÍNH NĂNG ĐỘC QUYỀN
Delta Lake là "trái tim" của hệ thống Lakehouse. Bạn đã thực hành 4 tính năng then chốt:

#### 1. Thư mục `_delta_log` và Tính chất ACID
* Khi một bảng Delta được tạo, nó lưu trữ dữ liệu dưới dạng các file **Parquet**, kèm theo một thư mục ẩn tên là **`_delta_log`**.
* Mọi hành động thêm/sửa/xóa đều được ghi nhận thành các file commit JSON (`000000.json`, `000001.json`...).
* Lệnh `%sql DESCRIBE HISTORY workspace.bronze.bronze_stores` cho phép kiểm tra chi tiết ai làm gì, vào thời điểm nào, version nào.

#### 2. Kỹ thuật Upsert với câu lệnh `MERGE INTO`
* Doanh nghiệp không ghi đè toàn bộ bảng khi có dữ liệu mới. Thay vào đó dùng `MERGE INTO`:
  * **WHEN MATCHED:** Cập nhật thông tin mới (Ví dụ: Store 1 mở rộng diện tích từ 595 lên 850 m²).
  * **WHEN NOT MATCHED:** Thêm mới dòng chưa từng tồn tại (Ví dụ: Store 999 vừa mở ở Hà Nội).
* Cú pháp mẫu bạn đã chạy thành công:
  ```sql
  MERGE INTO workspace.bronze.bronze_stores AS target
  USING stores_incoming_updates AS source
  ON target.storekey = source.storekey
  WHEN MATCHED THEN
    UPDATE SET target.square_meters = source.square_meters, target._ingested_at = source._ingested_at
  WHEN NOT MATCHED THEN
    INSERT *;
  ```

#### 3. Time Travel (Du hành thời gian)
* Cho phép đọc lại trạng thái dữ liệu trong quá khứ mà không cần phục hồi file backup:
  ```sql
  -- Xem lại dữ liệu ở Version 0 (trước khi chạy lệnh MERGE)
  SELECT * FROM workspace.bronze.bronze_stores VERSION AS OF 0 WHERE storekey IN (1, 999);
  ```
* Kết quả: Store 1 quay lại 595 m² và Store 999 hoàn toàn biến mất khỏi kết quả!

#### 4. Bảo trì bảng bằng `OPTIMIZE` (Giải quyết Small Files Problem)
* Khi nạp hoặc cập nhật dữ liệu nhiều lần, hệ thống sinh ra rất nhiều file Parquet nhỏ, khiến tốc độ đọc bị chậm.
* Lệnh `OPTIMIZE workspace.bronze.bronze_stores` tự động gom các file nhỏ này thành file kích thước tối ưu (~1GB). Kết quả chạy thực tế của bạn đã xóa 3 file nhỏ và gom thành 1 file lớn (`numFilesRemoved: 3, numFilesAdded: 1`).

---

### BUỔI 1.3: QUẢN TRỊ DỮ LIỆU TẬP TRUNG (UNITY CATALOG)
* **Mô hình định danh 3 cấp (3-Level Namespace):**
  * Quy tắc gọi bảng bắt buộc: `<catalog>.<schema>.<table>`
  * Dự án của bạn: `workspace.bronze.bronze_stores`, `workspace.bronze.bronze_sales`...
* **Bảo mật và Metadata hiện đại:**
  * Trong Unity Catalog, hàm cũ `F.input_file_name()` bị chặn để đảm bảo an toàn. 
  * Thay vào đó, bạn đã dùng cột metadata chuẩn của Databricks: **`F.col("_metadata.file_path")`**.

---

### BUỔI 1.4: NẠP DỮ LIỆU TẦNG BRONZE (RAW INGESTION)
* **Nguyên tắc vàng của Tầng Bronze:** 
  * *Không bao giờ sửa đổi dữ liệu gốc ở Bronze.* Lưu dữ liệu thô nhất có thể để làm "bảo hiểm" khi cần replay pipeline.
* **Hai cột Audit Metadata bắt buộc:**
  1. `_ingested_at = F.current_timestamp()`: Đánh dấu thời điểm bản ghi được nạp vào hồ.
  2. `_source_file = F.col("_metadata.file_path")`: Ghi lại file nguồn gốc sinh ra bản ghi này.
* **Chuẩn hóa tên cột (Snake_case):**
  * Do định dạng Parquet của Delta Lake cấm khoảng trắng trong tên cột (như `Square Meters`, `Open Date`), bạn đã viết đoạn mã tự động chuẩn hóa toàn bộ tên cột sang chữ thường và thay khoảng trắng bằng dấu gạch dưới (`square_meters`, `open_date`).

---

### BUỔI 1.5: TỰ ĐỘNG HÓA VỚI DATABRICKS WORKFLOWS (ORCHESTRATION)
* **Khái niệm DAG (Directed Acyclic Graph):**
  * Đồ thị có hướng một chiều kết nối các tác vụ theo thứ tự logic.
* **Thiết lập Workflow của bạn:**
  * **Task 1 (`Ingest_Bronze_Data`):** Chạy Notebook chính đọc 5 file CSV thô, chuẩn hóa cột, ghi ra Delta Table.
  * **Task 2 (`Validate_Bronze_Quality`):** Chạy Notebook phụ kiểm tra số dòng các bảng (`assert count > 0`). Task này có quan hệ **`Depends on: Ingest_Bronze_Data`** (chỉ chạy khi Task 1 thành công).
* **Kết quả:** Pipeline chạy trơn tru trong 1 phút 31 giây, trạng thái `Succeeded` xanh 100%, cluster tự động terminate để tiết kiệm tài nguyên.

---

## 3. BỘ 5 CÂU HỎI VẤN ĐÁP KINH ĐIỂN VỀ TUẦN 1

| Câu hỏi phỏng vấn | Điểm mấu chốt cần trả lời |
| :--- | :--- |
| **1. Delta Lake khác gì so với lưu trữ Parquet truyền thống?** | Parquet chỉ là file định dạng columnar lưu trữ tĩnh. Delta Lake là lớp trừu tượng phía trên Parquet, có thêm thư mục `_delta_log` giúp đạt tính chất **ACID Transactions**, hỗ trợ Time Travel, Upsert (`MERGE INTO`), Schema Enforcement và Data Compaction. |
| **2. Tại sao tầng Bronze cần thêm `_ingested_at` và `_source_file`?** | Phục vụ mục đích Audit Trail (truy vết). Khi dữ liệu tầng Gold/Power BI có sai lệch, Data Engineer có thể lần ngược lại tầng Bronze xem dữ liệu được nạp vào lúc mấy giờ và từ file nguồn nào. |
| **3. Khi nào và tại sao cần chạy lệnh `OPTIMIZE`?** | Chạy định kỳ sau các đợt streaming nạp dữ liệu lớn hoặc sau nhiều lần chạy `MERGE INTO`. Nó giải quyết vấn đề **Small Files Problem** bằng cách gom nhiều file Parquet nhỏ thành file tối ưu ~1GB, giúp giảm thời gian quét metadata và tăng tốc độ đọc của Spark. |
| **4. Nếu lỡ tay xóa nhầm hoặc cập nhật sai một bảng Delta, em xử lý thế nào?** | Sử dụng tính năng **Time Travel**. Dùng lệnh `DESCRIBE HISTORY` để tìm version trước khi bị lỗi, sau đó chạy `SELECT * FROM table VERSION AS OF <prev_version>` hoặc dùng lệnh `RESTORE TABLE target_table TO VERSION AS OF <prev_version>` để khôi phục bảng ngay lập tức mà không cần restore backup. |
| **5. Phân biệt All-Purpose Cluster và Job Cluster trong Databricks?** | All-Purpose Cluster dùng cho Data Engineer ngồi code tương tác, phân tích, debug trên Notebook (chi phí DBU đắt hơn). Job Cluster (hoặc Serverless Job) là cluster tạm thời được tạo ra tự động khi có lịch chạy Workflow, chạy xong tác vụ là tự hủy ngay lập tức, chi phí rẻ hơn nhiều và tối ưu hóa tài nguyên doanh nghiệp. |

---

## 4. LỘ TRÌNH TIẾP THEO: TUẦN 2 – TẦNG SILVER (LÀM SẠCH & CHUẨN HÓA)
Mục tiêu buổi tới:
1. Tạo schema **`workspace.silver`**.
2. **Xử lý giá tiền:** Làm sạch cột `unit_cost_usd` và `unit_price_usd` (loại bỏ ký tự `$`, khoảng trắng, ép sang kiểu `DoubleType`).
3. **Quy đổi ngoại tệ:** Join bảng `bronze_sales` với `bronze_exchange_rates` theo ngày và mã tiền tệ để quy đổi 100% doanh thu về đồng USD chuẩn.
4. **PySpark Window Functions:** Luyện viết các hàm phân tích cửa hàng và sản phẩm bán chạy nhất (`row_number()`, `dense_rank()`).

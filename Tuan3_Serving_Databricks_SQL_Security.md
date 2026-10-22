# CHUYÊN ĐỀ TUẦN 3: TẦNG PHỤC VỤ (SERVING LAYER), DATABRICKS SQL & BẢO MẬT
**Môn học:** Data Engineering on Databricks & Power BI  
**Khóa học:** AI19 Preparation for QAI / FPT Software OJT (Mentor: Thầy Long - Lvn)  
**Tài liệu:** Đúc kết kiến thức Tuần 3 (Buổi 3.1 đến 3.5) theo [Syllabus](file:///e:/study%20DE/Syllabus_Databricks_PowerBI.xlsx)

---

## MỤC LỤC
1. [Tại sao cần Tầng Serving & Bản chất Databricks SQL Warehouse (Buổi 3.1)](#1-tại-sao-cần-tầng-serving--bản-chất-sql-warehouse)
2. [Cơ chế Caching & Theo dõi Query History (Buổi 3.1)](#2-cơ-chế-caching--theo-dõi-query-history)
3. [Chuyển đổi SQL truyền thống sang Databricks SQL (Buổi 3.2)](#3-chuyển-đổi-sql-truyền-thống-sang-databricks-sql)
4. [Tối ưu hóa tốc độ truy vấn phục vụ Power BI (Buổi 3.3)](#4-tối-ưu-hóa-tốc-độ-truy-vấn-phục-vụ-power-bi)
5. [Bảo mật tại nguồn: Row Filter & Column Masking (Buổi 3.4)](#5-bảo-mật-tại-nguồn-row-filter--column-masking)
6. [Quản trị môi trường Dev/UAT/Prod & CI/CD (Buổi 3.5)](#6-quản-trị-môi-trường-devuatprod--cicd)
7. [Bộ câu hỏi phỏng vấn tuyển dụng](#7-bộ-câu-hỏi-phỏng-vấn-tuyển-dụng)

---

# 1. TẠI SAO CẦN TẦNG SERVING & BẢN CHẤT SQL WAREHOUSE (BUỔI 3.1)

### 1.1. Vấn đề: Tại sao không cho Power BI kết nối thẳng vào Notebook Cluster?
* **Notebook Cluster (All-Purpose Cluster):**
  * Thiết kế cho Data Engineer ngồi code tương tác, chạy các tác vụ ETL nặng theo lô (Batch).
  * Khởi động chậm (3 - 5 phút).
  * Khi có **50 người dùng Power BI cùng mở Dashboard một lúc**, mỗi người bấm lọc 1 biểu đồ $\rightarrow$ Hàng trăm câu query SQL ngắn ập đến cùng lúc $\rightarrow$ Notebook Cluster bị nghẽn cổ chai (Concurrency bottleneck), Dashboard bị đơ và giật lag!

### 1.2. Giải pháp: Databricks SQL Warehouse (Cụm máy chủ chuyên biệt cho BI)
* **SQL Warehouse là gì?** Là một cụm máy chủ tính toán độc lập, được Databricks tối ưu hóa 100% chỉ để **chạy các câu lệnh truy vấn SQL siêu tốc**.
* **Đặc tính kỹ thuật vượt trội:**
  1. **Khởi động tức thì (Serverless):** Khởi động chỉ trong **2 - 5 giây** khi có truy vấn từ Power BI (không phải chờ đợi 5 phút).
  2. **Động cơ Photon C++ (Vectorized Execution):** Mặc định tích hợp sẵn động cơ Photon, giúp các hàm `SUM`, `AVG`, `GROUP BY`, `JOIN` chạy nhanh gấp 2 - 5 lần so với Spark truyền thống.
  3. **Auto-scaling & Multi-cluster Load Balancing:** Khi có nhiều người cùng truy vấn, SQL Warehouse tự động bật thêm cụm máy phụ để gánh tải, khi hết giờ làm việc thì tự động tắt để tiết kiệm chi phí DBU.

---

# 2. CƠ CHẾ CACHING & QUERY HISTORY (BUỔI 3.1)

### 2.1. Hai tầng Caching "thần thánh" trong Databricks SQL
Khi Power BI gửi query sang Databricks, hệ thống đi qua 2 tầng đệm:

```
Power BI Query ──► [ 1. Query Result Cache ] (Có kết quả sẵn trong RAM/Cloud?)
                          │ 
                          ├─► CÓ: Trả về ngay trong 0.05s (Không tốn DBU tính toán!)
                          │
                          └─► KHÔNG: Đẩy xuống [ 2. Delta Cache (SSD) ] ──► Tính toán mới
```

1. **Query Result Cache (Bộ nhớ đệm kết quả):**
   * Nếu Tổng giám đốc mở dashboard xem "Doanh thu năm 2026", 5 phút sau Giám đốc tài chính cũng mở đúng trang đó:
   * Databricks nhận diện câu SQL giống hệt nhau và dữ liệu gốc chưa đổi $\rightarrow$ **Trả về ngay lập tức kết quả đã lưu sẵn trong vài chục mili-giây** mà không cần chạy lại Spark engine!
2. **Delta Cache (Disk Cache trên ổ SSD của Worker):**
   * Lưu các khối file Parquet đã giải nén vào ổ SSD siêu tốc của máy chủ để các lần quét sau không phải đọc lại từ S3/ADLS.

### 2.2. Query History (Nhật ký truy vết & Đo hiệu năng)
* Nằm tại menu bên trái: **SQL $\rightarrow$ Query History**.
* Cho phép kỹ sư DE soi từng câu query do Power BI gửi sang:
  * Thời gian chạy mất mấy giây?
  * Đọc bao nhiêu MB dữ liệu?
  * Quét bao nhiêu dòng?
  * Có bị nghẽn ở bước Shuffle nào không?

---

# 3. CHUYỂN ĐỔI SQL TRUYỀN THỐNG SANG DATABRICKS SQL (BUỔI 3.2)

Khi doanh nghiệp di chuyển các báo cáo cũ từ Oracle, SQL Server, MySQL sang Databricks Lakehouse:

| Hệ thống cũ (Oracle / SQL Server / MySQL) | Databricks SQL (Delta Lake) | Lý do chuyển đổi |
| :--- | :--- | :--- |
| `PARTITION BY (store_id)` (Partition thư mục vật lý) | **`CLUSTER BY (store_id, order_date)`** (Liquid Clustering) | Khắc phục triệt để vấn đề Small Files và Over-partitioning. |
| `DATETIME`, `DATETIME2` | `TIMESTAMP` | Chuẩn hóa múi giờ UTC theo ISO 8601. |
| `NVARCHAR(255)`, `VARCHAR` | `STRING` | Không cần giới hạn độ dài chuỗi, tối ưu nén Parquet. |
| `MONEY`, `FLOAT` | `DECIMAL(18,2)` hoặc `DOUBLE` | Tránh sai số dấu phẩy động trong báo cáo tài chính. |
| `ISNULL(col, 0)` hoặc `NVL(col, 0)` | `COALESCE(col, 0)` | Chuẩn SQL ANSI quốc tế. |

---

# 4. TỐI ƯU HÓA TỐC ĐỘ TRUY VẤN PHỤC VỤ POWER BI (BUỔI 3.3)

### 4.1. Vấn đề của DirectQuery:
Khi Power BI dùng chế độ **DirectQuery** (truy vấn trực tiếp xuống Databricks mỗi khi người dùng click chuột):
* Nếu mỗi thao tác click chuột trên Dashboard đều bắt Databricks quét lại **hàng chục triệu dòng** trong bảng `fact_sales` $\rightarrow$ Dashboard sẽ bị quay tròn 5 - 10 giây!

### 4.2. Giải pháp 1: Gold Aggregate Tables (Bảng tổng hợp sẵn)
Thay vì để Power BI quét bảng chi tiết từng đơn hàng, DE tạo thêm một bảng tổng hợp sẵn theo ngày và cửa hàng:

```sql
CREATE OR REPLACE TABLE workspace.gold.agg_daily_store_sales
CLUSTER BY (store_key, order_date)
AS
SELECT 
    store_key,
    order_date,
    SUM(sales_amount_usd) AS daily_revenue_usd,
    SUM(total_cost_usd)   AS daily_cost_usd,
    SUM(gross_profit_usd) AS daily_profit_usd,
    COUNT(DISTINCT order_number) AS total_orders
FROM workspace.gold.fact_sales
GROUP BY store_key, order_date;
```
* **Hiệu quả:** Bảng này chỉ có vài chục nghìn dòng (thay vì hàng chục triệu dòng) $\rightarrow$ Power BI vẽ biểu đồ doanh thu theo ngày **chỉ mất 0.2 giây**!

### 4.3. Giải pháp 2: Materialized Views (Bảng ảo tự cập nhật)
* Khác với View thông thường (mỗi lần gọi lại phải chạy tính toán từ đầu), **Materialized View** lưu sẵn kết quả ra đĩa cứng và **tự động đồng bộ (incremental refresh)** khi bảng gốc có đơn hàng mới phát sinh.

---

# 5. BẢO MẬT TẠI NGUỒN: ROW FILTER & COLUMN MASKING (BUỔI 3.4)

Đây là tính năng bảo mật cấp độ cao nhất của Unity Catalog, giúp bảo vệ dữ liệu ngay tại gốc Databricks trước khi cấp sang Power BI:

### 5.1. Row Filter (Bảo mật cấp dòng - RLS tại nguồn)
* **Yêu cầu kinh doanh:** Quản lý chi nhánh cửa hàng nào chỉ được nhìn thấy đơn hàng của cửa hàng đó (Quản lý Store 1 không được xem doanh thu Store 2).
* **Cú pháp thiết lập trên Databricks:**
```sql
-- 1. Tạo hàm kiểm tra quyền của người dùng hiện tại
CREATE OR REPLACE FUNCTION workspace.gold.store_security_filter(store_key INT)
RETURN 
  IS_ACCOUNT_GROUP_MEMBER('admin_group') OR 
  store_key = 1; -- Hoặc map động với email người dùng

-- 2. Gắn hàm bảo mật trực tiếp vào bảng Fact
ALTER TABLE workspace.gold.fact_sales 
SET ROW FILTER workspace.gold.store_security_filter ON (store_key);
```

### 5.2. Column Masking (Ẩn cột dữ liệu nhạy cảm)
* **Yêu cầu:** Nhân viên phân tích được xem tổng doanh thu, nhưng cột **Giá vốn (`unit_cost_usd`)** và **Lợi nhuận (`gross_profit_usd`)** phải bị che đi để tránh lộ bí mật kinh doanh.
```sql
-- 1. Tạo hàm che dữ liệu
CREATE OR REPLACE FUNCTION workspace.gold.mask_financial_data(val DOUBLE)
RETURN CASE 
  WHEN IS_ACCOUNT_GROUP_MEMBER('c_level_executives') THEN val 
  ELSE NULL 
END;

-- 2. Áp dụng che cột vào bảng
ALTER TABLE workspace.gold.fact_sales 
ALTER COLUMN unit_cost_usd SET MASK workspace.gold.mask_financial_data;
```

---

# 6. QUẢN TRỊ MÔI TRƯỜNG DEV/UAT/PROD & CI/CD (BUỔI 3.5)

Trong môi trường doanh nghiệp chuẩn (FPT Software):
* **Tuyệt đối không được code trực tiếp trên dữ liệu thật của khách hàng (Production)!**

### Mô hình 3 môi trường phân tách:
1. **Môi trường DEV (Development):** Nơi kỹ sư DE viết code thử nghiệm, dùng mẫu dữ liệu nhỏ (Sample Data).
2. **Môi trường UAT (User Acceptance Testing / Staging):** Nơi triển khai bản thử nghiệm cho khách hàng và đội kiểm thử (Tester) vào đối soát số liệu.
3. **Môi trường PROD (Production):** Môi trường thực tế phục vụ trực tiếp cho Ban Giám Đốc và toàn bộ hệ thống kinh doanh.

* **Databricks Asset Bundles (DAB) & Git:** Cho phép đóng gói toàn bộ Notebook, Workflow, DLT Pipeline thành các gói phần mềm (Infrastructure-as-Code) để tự động deploy từ Dev $\rightarrow$ UAT $\rightarrow$ Prod qua GitHub Actions mà không cần bấm chuột thủ công.

---

# 7. BỘ CÂU HỎI PHỎNG VẤN TUYỂN DỤNG CỐT LÕI

### Câu 1: Tại sao khi kết nối Power BI với Databricks, ta nên kết nối qua SQL Warehouse mà không kết nối thẳng vào All-Purpose Cluster?
> **Trả lời:**  
> Vì SQL Warehouse được thiết kế chuyên biệt cho BI với 3 ưu điểm vượt trội:  
> 1. Tích hợp sẵn động cơ Photon C++ tối ưu cho các truy vấn tổng hợp SQL.  
> 2. Khởi động tức thì (Serverless) chỉ trong vài giây và có cơ chế Auto-scaling phân tải khi có nhiều người dùng cùng truy cập báo cáo.  
> 3. Tận dụng 2 tầng Caching (Query Result Cache và Delta Cache) giúp các câu query lặp lại trả về dưới 0.1 giây mà không tốn chi phí DBU.

### Câu 2: Em hãy phân biệt Row Filter và Column Masking trong Unity Catalog?
> **Trả lời:**  
> Cả hai đều là cơ chế kiểm soát bảo mật dữ liệu tại nguồn (Fine-grained Access Control):  
> - **Row Filter:** Lọc các dòng dữ liệu dựa trên quyền hạn của người dùng (ví dụ: quản lý chi nhánh chỉ xem được các dòng thuộc chi nhánh của mình).  
> - **Column Masking:** Ẩn hoặc làm mờ nội dung của một cột nhạy cảm cụ thể (ví dụ: che số thẻ tín dụng thành dấu sao, hoặc ẩn giá vốn đối với nhân viên thường) trong khi vẫn cho phép họ xem các cột khác trên cùng dòng đó.

# CHUYÊN ĐỀ BUỔI 2.5: HIỆU NĂNG & CHI PHÍ SPARK (SPARK PERFORMANCE & COST OPTIMIZATION)
**Môn học:** Data Engineering on Databricks & Power BI  
**Khóa học:** AI19 Preparation for QAI / FPT Software OJT (Mentor: Thầy Long - Lvn)  
**Tài liệu:** Đúc kết kiến thức Buổi 2.5 theo [Syllabus](file:///e:/study%20DE/Syllabus_Databricks_PowerBI.xlsx)

---

## MỤC LỤC
1. [Tại sao Hiệu năng & Chi phí là thước đo sống còn của Data Engineer?](#1-tại-sao-hiệu-năng--chi-phí-là-thước-đo-sống-còn)
2. [Partitioning trong Spark & Công thức vàng](#2-partitioning-trong-spark--công-thức-vàng)
3. [Hiện tượng Data Skew & Các giải pháp xử lý](#3-hiện-tượng-data-skew--các-giải-pháp-xử-lý)
4. [Broadcast Join & Chiến lược khử Shuffle](#4-broadcast-join--chiến-lược-khử-shuffle)
5. [AQE (Adaptive Query Execution) trong Spark 3.x+](#5-aqe-adaptive-query-execution)
6. [Photon Engine - Động cơ C++ độc quyền của Databricks](#6-photon-engine---động-cơ-c-độc-quyền-của-databricks)
7. [Cluster Sizing & Quản trị chi phí DBU (Databricks Unit)](#7-cluster-sizing--quản-trị-chi-phí-dbu)
8. [Bộ câu hỏi phỏng vấn tuyển dụng QAI/FPT Software](#8-bộ-câu-hỏi-phỏng-vấn-tuyển-dụng)

---

## 1. TẠI SAO HIỆU NĂNG & CHI PHÍ LÀ THƯỚC ĐO SỐNG CÒN?
* Ở mức độ **Junior/Mới học**: Chạy ra kết quả đúng (Data đúng logic) là xong.
* Ở mức độ **Data Engineer Doanh nghiệp (FPT Software / Dự án Thầy Long)**:
  * Dữ liệu không phải vài nghìn dòng mà là **hàng chục triệu đến hàng tỷ bản ghi**.
  * Chạy sai cách $\rightarrow$ Job chạy mất 8 tiếng thay vì 15 phút.
  * Tốn tiền Cloud: Databricks tính phí bằng **DBU** theo giây; máy ảo Cloud tính theo giờ. Một query viết dở có thể đốt hàng nghìn USD của khách hàng.
  * Phỏng vấn DE luôn xoay quanh: *"Làm sao em biết job chạy chậm ở đâu? Làm sao em tối ưu?"*

---

## 2. PARTITIONING TRONG SPARK & CÔNG THỨC VÀNG

### 2.1. Partition là gì?
Spark xử lý phân tán theo nguyên tắc chia để trị. Một DataFrame được chia thành nhiều khối nhỏ gọi là **Partition**. Mỗi CPU Core trên Worker Node sẽ nhận **1 Partition** để tính toán tại 1 thời điểm.

```
DataFrame (10 GB) ──► Chia thành 50 Partitions (mỗi partition 200 MB)
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   CPU Core 1       CPU Core 2       CPU Core 3 (Chạy song song)
```

### 2.2. Hai sai lầm kinh điển về số lượng Partition
1. **Quá ít Partition (Under-partitioning):**
   * Dữ liệu mỗi partition quá lớn $\rightarrow$ Tràn RAM Worker $\rightarrow$ Lỗi `java.lang.OutOfMemoryError: Java heap space` hoặc Disk Spill (tràn ra đĩa cứng khiến tốc độ giảm 100 lần).
2. **Quá nhiều Partition (Over-partitioning):**
   * Dữ liệu 10MB nhưng chia thành 2000 partitions $\rightarrow$ Mỗi partition vài KB.
   * Spark tốn toàn bộ thời gian cho việc quản lý task overhead, mở kết nối mạng, metadata thay vì tính toán.

### 2.3. Công thức vàng (Best Practices):
* **Kích thước lý tưởng của 1 partition:** Từ **100 MB đến 200 MB**.
* **Tham số quan trọng:** `spark.sql.shuffle.partitions` (mặc định là 200).
  * Với dữ liệu nhỏ (mấy chục MB trong bài tập): Nên giảm xuống `spark.conf.set("spark.sql.shuffle.partitions", 4)` hoặc `8`.
  * Với dữ liệu lớn (hàng trăm GB): Tăng lên 1000 - 2000.

### 2.4. Phân biệt `repartition()` và `coalesce()`
| Tiêu chí | `repartition(n)` | `coalesce(n)` |
| :--- | :--- | :--- |
| **Bản chất** | Full Shuffle (xáo trộn dữ liệu qua card mạng giữa các node) | Giảm partition bằng cách gộp các partition trên cùng node (No full shuffle) |
| **Tăng hay giảm** | Cả tăng số partition và giảm số partition | **Chỉ giảm** số partition |
| **Chi phí** | **Rất đắt** (High Network I/O) | **Rất rẻ** |
| **Khi nào dùng?** | Khi muốn cân bằng lại dữ liệu bị lệch (Fix Skew) | Khi gom nhỏ trước khi ghi ra file: `df.coalesce(1).write.csv(...)` |

---

## 3. HIỆN TƯỢNG DATA SKEW & CÁC GIẢI PHÁP XỬ LÝ

### 3.1. Data Skew là gì?
Data Skew (Lệch dữ liệu) là tình trạng dữ liệu phân bố **không đều** giữa các partition.
* **Triệu chứng trên Spark UI:** 99 Task hoàn thành trong 10 giây, nhưng 1 Task cuối cùng chạy đơ suốt 2 tiếng và thường kết thúc bằng `OOM (OutOfMemory)`.
* **Nguyên nhân:** Khóa Join hoặc Khóa GroupBy bị lệch nặng. Ví dụ:
  * Trong bảng bán lẻ: Khách vãng lai (`customer_key = NULL` hoặc `0`) chiếm 70% toàn bộ đơn hàng.
  * Một chi nhánh lớn tại Hà Nội chiếm 80% doanh số toàn quốc.

### 3.2. Ba cách khắc phục Data Skew
1. **Lọc riêng giá trị NULL / Khách vãng lai:**
   * Tách bảng giao dịch thành 2 luồng: Luồng có Customer ID hợp lệ $\rightarrow$ Join bình thường. Luồng NULL $\rightarrow$ Không cần join, gán thẳng `'Khách vãng lai'`, sau đó `unionByName` lại.
2. **Kỹ thuật Salting (Thêm muối ngẫu nhiên):**
   * Thêm một số ngẫu nhiên từ `0` đến `k-1` vào khóa bị lệch ở bảng lớn: `new_key = concat(store_key, '_', floor(rand() * 5))`.
   * Nhân bản bảng nhỏ lên `k` lần tương ứng.
   * Nhờ đó, 1 partition khổng lồ được xé nhỏ thành `k` partition con chạy song song trên các core khác nhau.
3. **Sử dụng Skew Join Hint (Spark 3+):**
   ```sql
   SELECT /*+ SKEW('fact_sales', 'store_key') */ *
   FROM fact_sales s JOIN dim_store d ON s.store_key = d.store_key
   ```

---

## 4. BROADCAST JOIN & CHIẾN LƯỢC KHỬ SHUFFLE

### 4.1. Shuffle - Kẻ thù số 1 của hiệu năng Spark
Trong một phép Join thông thường (Sort Merge Join):
* Spark phải băm (hash) các dòng theo khóa Join, ghi ra đĩa cứng của Worker, gửi qua mạng Internet/LAN đến Worker đích để ghép cặp.
* Quá trình này gọi là **Shuffle** $\rightarrow$ Tốn CPU, Disk I/O, và Network I/O khủng khiếp.

### 4.2. Cơ chế Broadcast Hash Join (BHJ)
* Nếu một trong hai bảng tham gia phép Join có kích thước nhỏ (ví dụ bảng `dim_product` có 2,500 dòng ~ 2 MB):
* Thay vì shuffle bảng `fact_sales` (10 GB), Spark Driver sẽ **copy toàn bộ bảng nhỏ gửi vào bộ nhớ RAM của từng Worker Node**.
* Mỗi Worker tự tra cứu bảng nhỏ ngay trong RAM tại chỗ mà **KHÔNG CẦN TRUYỀN DỮ LIỆU BẢNG FACT QUA MẠNG**.
* **Network I/O của bảng Fact = 0!** Tốc độ tăng 10 - 50 lần.

```python
from pyspark.sql.functions import broadcast

# Code thực tế chúng ta đã áp dụng trong notebook 03:
df_sales_enriched = df_sales.join(
    broadcast(df_products), 
    on="product_key", 
    how="left"
)
```

> **Ngưỡng mặc định:** Spark tự động broadcast nếu bảng nhỏ hơn `spark.sql.autoBroadcastJoinThreshold = 10MB` (10485760 bytes).

---

## 5. AQE (ADAPTIVE QUERY EXECUTION) TRONG SPARK 3.X+

Trước Spark 3.0, kế hoạch thực thi (Execution Plan) được xác định cố định từ trước khi chạy (Static Planning) dựa vào thống kê cũ.  
Từ Spark 3.0+ trên Databricks, **AQE được bật mặc định**: Spark sẽ theo dõi thống kê thực tế lúc đang chạy (Runtime Statistics) giữa các Stage để tự động điều chỉnh kế hoạch!

### 3 tính năng thần thánh của AQE:
1. **Dynamically Coalescing Shuffle Partitions:**
   * Sau khi shuffle, nếu thấy dữ liệu nhỏ (ví dụ mỗi partition chỉ có vài KB), AQE tự động gộp chúng lại thành số lượng partition hợp lý, tránh lãng phí task.
2. **Dynamically Switching Join Strategies:**
   * Ban đầu dự định dùng Sort Merge Join vì tưởng bảng lớn. Khi chạy xong Stage 1 lọc dữ liệu, thấy bảng chỉ còn 5MB $\rightarrow$ AQE tự động chuyển sang Broadcast Join ngay lập tức.
3. **Dynamically Optimizing Skew Joins:**
   * AQE tự động phát hiện partition nào có dung lượng lớn bất thường so với mức trung bình và tự động chia nhỏ partition lệch đó ra mà bạn không cần phải code thủ công kỹ thuật Salting.

---

## 6. PHOTON ENGINE - ĐỘNG CƠ C++ ĐỘC QUYỀN CỦA DATABRICKS

### 6.1. Photon là gì?
* Apache Spark truyền thống được viết bằng Scala/Java, chạy trên máy ảo **JVM (Java Virtual Machine)**. Điểm yếu của JVM là tốn bộ nhớ cho Garbage Collection (GC) và không tận dụng triệt để kiến trúc CPU hiện đại.
* Databricks đã viết lại từ đầu toàn bộ động cơ thực thi SQL/DataFrame bằng **ngôn ngữ C++**, gọi là **Photon Engine**.

### 6.2. Tại sao Photon lại siêu nhanh?
* **Vectorized Execution:** Xử lý dữ liệu theo từng khối hàng nghìn dòng cùng lúc (Data batching) trên thanh ghi CPU, thay vì xử lý từng dòng (`tuple-at-a-time`).
* Tận dụng tập lệnh phần cứng **SIMD (Single Instruction, Multiple Data)** của CPU Intel/AMD.
* Không bị nghẽn bộ nhớ bởi cơ chế thu gom rác (JVM Garbage Collection overhead).
* Thích hợp nhất cho: Các câu lệnh SQL aggregation (`SUM`, `AVG`, `GROUP BY`), Hash Join, và truy vấn BI trên tầng Gold.

---

## 7. CLUSTER SIZING & QUẢN TRỊ CHI PHÍ DBU

### 7.1. DBU (Databricks Unit) là gì?
* DBU là đơn vị chuẩn đo lường năng lực tính toán tiêu thụ trên Databricks theo từng giây.
* Tổng chi phí chạy Databricks = `(Số DBU tiêu thụ * Giá DBU của gói tier) + Chi phí thuê máy ảo Cloud (Azure/AWS VM)`.

### 7.2. Chiến lược Cluster Sizing chuẩn Doanh nghiệp
1. **Môi trường Dev / Testing / Học tập:**
   * Sử dụng **Single Node** (1 máy ảo duy nhất làm cả Driver và Worker) hoặc **Serverless Compute**.
   * Bật **Auto-termination** sau **15 - 20 phút** không hoạt động để tránh quên tắt cụm máy đốt tiền qua đêm.
2. **Môi trường Production ETL Pipeline:**
   * Dùng **Job Cluster** (Automated Cluster) thay vì All-Purpose Cluster (Interactive Cluster).
     * *Lý do:* Giá DBU của Job Cluster rẻ hơn **gần một nửa** so với All-Purpose Cluster! Khi Job chạy xong, cluster tự động bị hủy (terminated).
   * Bật **Auto-scaling:** Cho phép mở rộng từ min workers $\rightarrow$ max workers chỉ khi tải cao.
3. **Môi trường BI / Serving (Cấp nguồn cho Power BI):**
   * Sử dụng **Databricks SQL Warehouse** (Serverless). Tự khởi động trong 2 giây khi có query từ dashboard và tự tắt khi dashboard nhàn rỗi.

---

## 8. BỘ CÂU HỎI PHỎNG VẤN TUYỂN DỤNG (QAI / FPT SOFTWARE)

### Câu 1: Em hãy giải thích sự khác nhau giữa `repartition` và `coalesce`? Khi nào dùng cái nào?
> **Trả lời mẫu:**  
> `repartition` gây ra Full Shuffle qua mạng, có thể dùng để vừa tăng vừa giảm số partition, thường dùng khi muốn phân bố lại dữ liệu bị lệch (fix skew). Còn `coalesce` chỉ dùng để giảm số partition bằng cách gộp các partition sẵn có trên cùng node mà không gây shuffle toàn diện, nên chi phí cực rẻ. Trước khi ghi dữ liệu ra tầng Silver/Gold thành ít file, ta luôn ưu tiên dùng `coalesce`.

### Câu 2: Trong dự án bán lẻ vừa qua, em xử lý phép Join giữa bảng Giao dịch (Sales) và bảng Sản phẩm (Products) như thế nào để tối ưu hiệu năng?
> **Trả lời mẫu:**  
> Bảng Sales có số lượng bản ghi rất lớn, trong khi bảng Products là bảng danh mục nhỏ (khoảng 2.500 bản ghi, dung lượng vài MB). Do đó, em sử dụng kỹ thuật **Broadcast Hash Join** bằng hàm `broadcast(df_products)` của PySpark. Kỹ thuật này giúp đưa toàn bộ bảng Products vào RAM của từng Worker, triệt tiêu hoàn toàn chi phí Shuffle qua mạng cho bảng Sales, giúp tốc độ thực thi tăng lên nhiều lần.

### Câu 3: Làm sao em nhận biết một Job Spark bị Data Skew trên Spark UI và hướng giải quyết?
> **Trả lời mẫu:**  
> Khi nhìn vào Spark UI tại tab Stages / Tasks, nếu thấy thời gian chạy (duration) của 99% các task rất ngắn (vài giây), nhưng có một vài task chạy kéo dài hàng chục phút hoặc bị lỗi OutOfMemory, đó là dấu hiệu rõ ràng của Data Skew. Hướng giải quyết của em là: 
> 1. Kiểm tra các giá trị NULL trên khóa join để filter hoặc tách nhánh xử lý riêng.
> 2. Bật tính năng AQE (`spark.sql.adaptive.skewJoin.enabled = true`) trên Databricks để Spark tự xé nhỏ partition lệch.
> 3. Áp dụng kỹ thuật Salting nếu cần can thiệp thủ công.

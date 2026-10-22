# CHUYÊN ĐỀ BUỔI 2.2: CHUYỂN ĐỔI SPARK SCALA SANG PYSPARK & TỐI ƯU UDF
**Môn học:** Data Engineering on Databricks & Power BI  
**Khóa học:** AI19 Preparation for QAI / FPT Software OJT (Mentor: Thầy Long - Lvn)  
**Tài liệu:** Đúc kết kiến thức Buổi 2.2 theo [Syllabus](file:///e:/study%20DE/Syllabus_Databricks_PowerBI.xlsx)

---

## MỤC LỤC
1. [Tại sao Doanh nghiệp (FPT/QAI) cần Port code từ Scala sang PySpark?](#1-tại-sao-cần-port-từ-scala-sang-pyspark)
2. [So sánh Kiến trúc: Spark Scala vs PySpark (Bản chất kỹ thuật)](#2-so-sánh-kiến-trúc-spark-scala-vs-pyspark)
3. [Bảng tra cứu khác biệt Cú pháp & API (Cheatsheet)](#3-bảng-tra-cứu-khác-biệt-cú-pháp--api)
4. [Tử huyệt UDF: Python UDF vs Pandas Vectorized UDF](#4-tử-huyệt-udf)
5. [BÀI LAB THỰC HÀNH: Port 1 Job Scala mẫu sang PySpark chuẩn Enterprise](#5-bài-lab-thực-hành-port-1-job-scala)
6. [Bộ câu hỏi phỏng vấn tuyển dụng](#6-bộ-câu-hỏi-phỏng-vấn-tuyển-dụng)

---

## 1. TẠI SAO CẦN PORT TỪ SCALA SANG PYSPARK?
* **Lịch sử:** Apache Spark được phát triển gốc bằng ngôn ngữ **Scala**. Trong giai đoạn 2014 - 2020, hầu hết các hệ thống Big Data ngân hàng, bán lẻ, viễn thông (On-premise Cloudera/Hadoop) đều viết bằng Scala vì hiệu năng cao và tính an toàn kiểu dữ liệu (Type Safety).
* **Thực tế hiện nay:**
  * Các doanh nghiệp di chuyển (migrate) lên đám mây và **Databricks**.
  * Hệ sinh thái Data Science / AI / Machine Learning (Pandas, Scikit-learn, PyTorch, LangChain) hoàn toàn thống trị bởi **Python**.
  * Tuyển dụng kỹ sư giỏi Scala cực kỳ khó và chi phí đắt đỏ, trong khi đội ngũ Data Engineer / AI biết Python rất dồi dào.
  * **Công việc thực tế tại QAI / FPT:** Nhận một repository gồm hàng chục file `.scala` cũ của khách hàng $\rightarrow$ Đọc hiểu logic $\rightarrow$ Port sang PySpark / Databricks SQL để tích hợp vào Lakehouse hiện đại.

---

## 2. SO SÁNH KIẾN TRÚC: SPARK SCALA VS PYSPARK

```
SPARK SCALA (Native JVM):
[ Scala Application ] ──► [ Spark Core Engine (JVM) ]  (Tốc độ tối đa, không trung gian)

PYSPARK (Qua Py4J Bridge):
[ Python Driver ] ──(Py4J Gateway)──► [ Spark Core (JVM) ]
                                            │
                                            ▼ (Catalyst Optimizer & Tungsten Engine)
```

* **Điểm tương đồng:** Cả Scala DataFrame và PySpark DataFrame **đều được dịch ra cùng một Logical & Physical Execution Plan** thông qua bộ tối ưu **Catalyst Optimizer** và thực thi trên **Tungsten Engine** của JVM. Do đó, nếu dùng các hàm chuẩn (`select`, `filter`, `groupBy`, `join`), **tốc độ của PySpark và Scala là tương đương nhau (99%)**!
* **Khác biệt lớn nhất:** Nằm ở kiểu dữ liệu tĩnh (`Dataset[T]` trong Scala) và cơ chế xử lý hàm tự định nghĩa (**UDF**).

---

## 3. BẢNG TRA CỨU KHÁC BIỆT CÚ PHÁP & API (CHEATSHEET)

| Nghiệp vụ / Thao tác | Spark Scala | PySpark (Python) | Ghi chú quan trọng |
| :--- | :--- | :--- | :--- |
| **Khai báo cột** | `$"col_name"` hoặc `'col_name` | `col("col_name")` hoặc `F.col("col_name")` | Scala có implicit conversions `import spark.implicits._` |
| **Khai báo biến/hằng**| `val df = ...` (hằng số)<br>`var x = ...` (biến) | `df = ...` (biến tự do) | Scala phân biệt bất biến (`val`) và biến đổi (`var`) |
| **Danh sách (List)** | `Seq(1, 2, 3)` hoặc `List(1, 2, 3)` | `[1, 2, 3]` | Scala dùng kiểu `Seq` cho danh sách truyền vào Spark |
| **Khởi tạo StructType**| `new StructType().add("id", IntegerType)` | `StructType([StructField("id", IntegerType())])` | PySpark thêm dấu ngoặc tròn `()` khi khởi tạo kiểu dữ liệu |
| **Lọc dữ liệu** | `df.filter($"age" > 18)` | `df.filter(F.col("age") > 18)` | Giống nhau về mặt hàm |
| **Điều kiện nhánh** | `when($"col" === 1, "A").otherwise("B")` | `when(F.col("col") == 1, "A").otherwise("B")` | Scala so sánh bằng dùng `===`, Python dùng `==` |
| **Giá trị rỗng** | `null` hoặc `None` (Option) | `None` | Scala có kiểu an toàn `Option[T]` (Some/None) |
| **Dataset vs DataFrame**| `val ds: Dataset[Store] = df.as[Store]` | Chỉ có `DataFrame` (tương đương `Dataset[Row]`) | Python không có kiểm tra kiểu tĩnh lúc compile |

---

## 4. TỬ HUYỆT UDF: PYTHON UDF VS PANDAS VECTORIZED UDF

Đây là câu hỏi phỏng vấn số 1 phân loại ứng viên Senior và Junior:

### 4.1. Tại sao Scala UDF nhanh mà Python UDF thông thường lại "rùa bò"?
* **Scala UDF:** Được biên dịch thẳng thành Java Bytecode và chạy trực tiếp bên trong tiến trình JVM của Worker Node. Tốc độ cực nhanh.
* **Standard Python UDF (`@udf`):**
  1. Dữ liệu từ JVM phải serialize (băm nhỏ) gửi qua socket sang một tiến trình con Python Worker (`Python Daemon`).
  2. Python Worker xử lý từng dòng một (`row-by-row`).
  3. Xử lý xong lại serialize gửi ngược về JVM.
  4. **Hậu quả:** Tốn 70 - 80% thời gian cho việc truyền dữ liệu qua lại (Serialization Overhead), và Catalyst Optimizer bị vô hiệu hóa hoàn toàn trên cột đó!

### 4.2. Giải pháp: Pandas UDF (Vectorized UDF với Apache Arrow)
Từ Spark 2.3+, Databricks tích hợp **Apache Arrow** cho phép chia sẻ bộ nhớ chung (In-memory zero-copy) giữa JVM và Python:
* Xử lý theo từng khối hàng nghìn dòng (Series-to-Series) thay vì từng dòng một.
* Tốc độ nhanh gấp 10 - 50 lần so với Python UDF thông thường, tiệm cận tốc độ của Scala!

```python
# Ví dụ so sánh cách viết:

# ❌ TRÁNH DÙNG: Standard Python UDF (Chậm, tốn CPU)
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

@udf(returnType=DoubleType())
def standard_discount_udf(price):
    if price is None:
        return 0.0
    return price * 0.9

#  CHUẨN SENIOR: Pandas UDF (Vectorized - Nhanh gấp 50 lần)
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf(DoubleType())
def vectorized_discount_udf(price_series: pd.Series) -> pd.Series:
    return price_series * 0.9
```

---

## 5. BÀI LAB THỰC HÀNH: PORT 1 JOB SCALA MẪU SANG PYSPARK

Giả sử khách hàng bàn giao cho bạn một đoạn mã nghiệp vụ Scala cũ dùng để làm sạch và phân loại chuỗi cửa hàng (`D_SHOP_FLC`):

### 5.1. Code gốc viết bằng Spark Scala (`StoreETL.scala`)
```scala
package com.retail.etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Định nghĩa case class cho Type Safety
case class StoreInput(StoreKey: Int, Country: String, State: String, Square_Meters: Option[Int], Open_Date: String)

object StoreETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StoreETL").getOrCreate()
    import spark.implicits._

    // 1. Đọc dữ liệu với strongly-typed Dataset
    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/mnt/raw/Stores.csv")

    // 2. Chuyển đổi và làm sạch
    val cleanDf = rawDf
      .withColumnRenamed("StoreKey", "store_key")
      .withColumnRenamed("Country", "country")
      .withColumnRenamed("State", "state")
      .withColumnRenamed("Square Meters", "square_meters")
      .withColumn("country", trim(initcap($"country")))
      .withColumn("state", trim(upper($"state")))
      .withColumn("square_meters", coalesce($"square_meters", lit(0)))
      .withColumn("open_date", to_date($"Open Date", "M/d/yyyy"))
      .withColumn("store_size_category", 
        when($"square_meters" < 500, "Small")
        .when($"square_meters" <= 1500, "Medium")
        .otherwise("Large")
      )
      .filter($"country".isin("United States", "Canada", "Germany", "Vietnam"))

    // 3. Ghi ra bảng Silver
    cleanDf.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("workspace.silver.fnd_stores")
  }
}
```

---

### 5.2. Bản Port chuẩn mực sang PySpark (`store_etl.py`)
```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# Khởi tạo Spark Session (trên Databricks biến spark đã có sẵn)
spark = SparkSession.builder.appName("StoreETL_PySpark").getOrCreate()

# 1. Đọc dữ liệu từ Unity Catalog Volume
raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/bronze/raw_data/Stores.csv")
)

# 2. Chuyển đổi và làm sạch logic nghiệp vụ tương đương
clean_df = (
    raw_df
    # Đổi tên cột sang snake_case
    .withColumnRenamed("StoreKey", "store_key")
    .withColumnRenamed("Country", "country")
    .withColumnRenamed("State", "state")
    .withColumnRenamed("Square Meters", "square_meters")
    # Biến đổi logic: $"col" trong Scala được đổi thành F.col("col") trong PySpark
    .withColumn("country", F.trim(F.initcap(F.col("country"))))
    .withColumn("state", F.trim(F.upper(F.col("state"))))
    .withColumn("square_meters", F.coalesce(F.col("square_meters"), F.lit(0)))
    .withColumn("open_date", F.to_date(F.col("Open Date"), "M/d/yyyy"))
    # Phân loại quy mô cửa hàng: Scala dùng === còn Python dùng ==
    .withColumn(
        "store_size_category",
        F.when(F.col("square_meters") < 500, "Small")
        .when(F.col("square_meters") <= 1500, "Medium")
        .otherwise("Large")
    )
    # isin trong Scala nhận danh sách tham số rời rạc hoặc Seq, Python nhận list [...]
    .filter(F.col("country").isin(["United States", "Canada", "Germany", "Vietnam"]))
)

# 3. Ghi ra bảng Delta tầng Silver
(
    clean_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.silver.fnd_stores")
)

print("Đã port thành công job Scala sang PySpark!")
```

---

## 6. BỘ CÂU HỎI PHỎNG VẤN TUYỂN DỤNG (QAI / FPT SOFTWARE)

### Câu 1: Em hãy giải thích sự khác biệt cốt lõi giữa Spark Scala và PySpark? PySpark có chạy chậm hơn Scala không?
> **Trả lời mẫu:**  
> Về mặt API bậc cao (DataFrame và Spark SQL), cả Scala và PySpark đều được dịch thành chung một Logical Plan và tối ưu bởi Catalyst Optimizer, sau đó thực thi trên Tungsten Engine của JVM. Do đó hiệu năng của PySpark và Scala là **hoàn toàn tương đương nhau**.  
> Khác biệt chính là: Scala là ngôn ngữ kiểu tĩnh hỗ trợ `Dataset[T]`, còn PySpark làm việc trên kiểu động `DataFrame`. Ngoài ra, nếu sử dụng hàm tự định nghĩa UDF, Scala chạy trực tiếp trong JVM nên nhanh hơn UDF Python thông thường vốn bị nghẽn ở bước Serialization qua Py4J.

### Câu 2: Khi phải port một đoạn code UDF phức tạp từ Scala sang PySpark mà Spark SQL không hỗ trợ hàm có sẵn, em sẽ làm như thế nào để đảm bảo hiệu năng?
> **Trả lời mẫu:**  
> Em sẽ tuyệt đối tránh dùng Python UDF thông thường (`@udf`) vì nó xử lý row-by-row và tốn chi phí Serialization. Thay vào đó, em sử dụng **Pandas UDF (Vectorized UDF)** kết hợp với thư viện **Apache Arrow**. Pandas UDF cho phép tính toán trên toàn bộ batch dữ liệu dạng Pandas Series/DataFrame với cơ chế Zero-copy memory, giúp tốc độ nhanh gấp hàng chục lần so với UDF truyền thống.

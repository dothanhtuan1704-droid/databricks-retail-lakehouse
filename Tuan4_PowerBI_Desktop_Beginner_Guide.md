# CẨM NANG TOÀN DIỆN TUẦN 4: POWER BI DESKTOP CHO NGƯỜI MỚI BẮT ĐẦU
**Môn học:** Data Engineering on Databricks & Power BI  
**Dự án thực tế:** Xây dựng Dashboard Quản trị Chuỗi Bán Lẻ (Case Study D_SHOP_FLC)  
**Tài liệu:** Đúc kết kiến thức Tuần 4 (Buổi 4.1 đến 4.5) theo [Syllabus](file:///e:/study%20DE/Syllabus_Databricks_PowerBI.xlsx)

---

## MỤC LỤC
1. [Power BI Desktop là gì? Bản quyền và Cách cài đặt](#1-power-bi-desktop-là-gì-bản-quyền-và-cách-cài-đặt)
2. [Làm quen 3 màn hình cốt lõi của Power BI Desktop](#2-làm-quen-3-màn-hình-cốt-lõi-của-power-bi-desktop)
3. [So sánh sống còn: Import Mode vs DirectQuery Mode](#3-so-sánh-sống-còn-import-mode-vs-directquery-mode)
4. [Hướng dẫn từng bước kết nối Databricks vào Power BI](#4-hướng-dẫn-từng-bước-kết-nối-databricks-vào-power-bi)
5. [Dựng mô hình quan hệ Star Schema (Semantic Model)](#5-dựng-mô-hình-quan-hệ-star-schema-semantic-model)
6. [Nhập môn ngôn ngữ DAX (Viết các chỉ số KPI doanh thu)](#6-nhập-môn-ngôn-ngữ-dax-viết-các-chỉ-số-kpi)
7. [Thiết kế Dashboard D_SHOP_FLC chuyên nghiệp](#7-thiết-kế-dashboard-d_shop_flc-chuyên-nghiệp)
8. [Bộ câu hỏi phỏng vấn tuyển dụng](#8-bộ-câu-hỏi-phỏng-vấn-tuyển-dụng)

---

# 1. POWER BI DESKTOP LÀ GÌ? BẢN QUYỀN VÀ CÁCH CÀI ĐẶT

### 1.1. Power BI Desktop là gì?
* **Power BI Desktop** là phần mềm thiết kế báo cáo thông minh (Business Intelligence) số 1 thế giới của Microsoft.
* **Chi phí bản quyền:** Bản **Power BI Desktop** cài trên máy tính Windows là **HOÀN TOÀN MIỄN PHÍ 100%** (Bạn không cần mua bản quyền Pro hay Premium để học và xây dựng dashboard).
* **Nhiệm vụ của Power BI:** Kéo dữ liệu từ kho Databricks về $\rightarrow$ Xây dựng mối quan hệ giữa các bảng $\rightarrow$ Vẽ biểu đồ tương tác, thẻ KPI trực quan cho Ban Giám Đốc.

### 1.2. Cách cài đặt nhanh nhất trên Windows:
* **Cách 1 (Khuyên dùng - Cực nhanh):**
  1. Bấm nút **Start** trên bàn phím (hoặc gõ tìm kiếm Windows): Mở ứng dụng **Microsoft Store**.
  2. Gõ tìm kiếm: **`Power BI Desktop`**.
  3. Bấm nút **Get / Install (Cài đặt)**. *(Microsoft Store sẽ tự động tải và cài đặt trong 2-3 phút, tự động cập nhật hàng tháng).*
* **Cách 2:** Tải trực tiếp file cài đặt `.exe` từ trang chủ Microsoft:  
  `https://aka.ms/pbidesktopstore` hoặc `https://www.microsoft.com/download/details.aspx?id=58494` (chọn file `PBIDesktopSetup_x64.exe`).

---

# 2. LÀM QUEN 3 MÀN HÌNH CỐT LÕI CỦA POWER BI DESKTOP

Khi mở Power BI Desktop lên, bạn nhìn sang **thanh menu dọc ở mép trái màn hình**, bạn sẽ thấy 3 biểu tượng đại diện cho 3 khu vực làm việc:

```
┌──────┐
│  📊  │  1. REPORT VIEW (Màn hình Báo cáo):
│      │     Nơi bạn vẽ các biểu đồ cột, biểu đồ đường, thẻ KPI và kéo thả giao diện.
├──────┤
│  📋  │  2. TABLE VIEW (Màn hình Dữ liệu):
│      │     Nơi bạn xem trực quan các dòng và các cột dữ liệu y hệt như Excel.
├──────┤
│  🔗  │  3. MODEL VIEW (Màn hình Mô hình hóa - Semantic Model):
│      │     Nơi hiển thị các bảng dưới dạng các khối hộp và bạn dùng chuột kéo dây
│      │     để nối bảng Fact với các bảng Dim (Star Schema)!
└──────┘
```

---

# 3. SO SÁNH SỐNG CÒN: IMPORT MODE VS DIRECTQUERY MODE

Đây là câu hỏi phỏng vấn số 1 của Thầy Long và mọi nhà tuyển dụng khi nói về kết nối Databricks $\rightarrow$ Power BI:

| Tiêu chí | **IMPORT MODE** (Nạp vào RAM) | **DIRECTQUERY MODE** (Truy vấn trực tiếp) |
| :--- | :--- | :--- |
| **Cơ chế hoạt động** | Tải toàn bộ dữ liệu từ Databricks về **lưu vào bộ nhớ RAM** của máy tính (công cụ nén VertiPaq). | **Không tải dữ liệu về máy**. Dữ liệu vẫn nằm nguyên trên Databricks. Mỗi khi người dùng bấm lọc biểu đồ, Power BI tự sinh câu lệnh SQL bắn sang Databricks lấy kết quả. |
| **Tốc độ hiển thị** | **Cực nhanh** (vì đọc trực tiếp từ RAM máy tính). | Phụ thuộc vào tốc độ của cụm **Databricks SQL Warehouse** và mạng Internet. |
| **Độ tươi mới của dữ liệu** | **Không thời gian thực:** Muốn có số liệu mới thì phải bấm nút "Refresh" định kỳ. | **Thời gian thực (Real-time):** Dữ liệu trên Databricks đổi là trên Dashboard đổi ngay! |
| **Giới hạn kích thước** | Bị giới hạn dung lượng RAM (tối đa 1GB cho tài khoản thường). | **Không giới hạn dung lượng:** Bảng có 1 tỷ dòng vẫn xử lý mượt mà! |
| **Khi nào sử dụng?** | Dùng cho bảng tổng hợp nhỏ (`agg_daily_store_sales`), bảng Dim. | Dùng cho bảng giao dịch chi tiết khổng lồ hàng trăm triệu dòng (`fact_sales`). |

---

# 4. HƯỚNG DẪN TỪNG BƯỚC KẾT NỐI DATABRICKS VÀO POWER BI

Sau khi cài đặt xong Power BI Desktop, bạn thực hiện đúng 5 bước sau để thông mạng sang Databricks:

### Bước 1: Mở cổng kết nối Databricks
1. Mở phần mềm **Power BI Desktop**.
2. Trên thanh công cụ (Ribbon) ở trên cùng: Click vào **`Get data`** $\rightarrow$ chọn **`More...`**.
3. Ở ô tìm kiếm, gõ chữ: **`Databricks`** $\rightarrow$ Chọn **Azure Databricks** (hoặc **Databricks**) $\rightarrow$ Bấm nút **`Connect`**.

### Bước 2: Điền thông số máy chủ (Đã lấy ở Tuần 3)
1. Ô **`Server Hostname`**: Dán tên máy chủ của bạn vào (ví dụ: `dbc-f1a30aac-c96c.cloud.databricks.com`).
2. Ô **`HTTP Path`**: Dán đường dẫn của bạn vào (ví dụ: `/sql/1.0/warehouses/4164...`).
3. Mục **`Data Connectivity mode`**: 
   * Tích chọn **`DirectQuery`** (để giữ đúng chuẩn kết nối dữ liệu lớn của Syllabus) hoặc **`Import`**.
4. Bấm **`OK`**.

### Bước 3: Xác thực bảo mật (Authentication)
1. Ở bảng thông báo xác thực bên trái: Click chọn tab **`Personal Access Token`**.
2. Ô **`Token`**: Dán chuỗi mã Token bí mật mà bạn đã tạo ở Tuần 3 vào.
3. Bấm **`Connect`**.

### Bước 4: Chọn các bảng dữ liệu tầng Gold
Cửa sổ **Navigator (Điều hướng)** sẽ hiện ra cây thư mục:
* Mở rộng: `workspace` $\rightarrow$ `gold`.
* Tích chọn 5 bảng cốt lõi của Star Schema:
  1. ☑️ **`dim_store`** (Cửa hàng / Case study D_SHOP)
  2. ☑️ **`dim_product`** (Sản phẩm)
  3. ☑️ **`dim_customer`** (Khách hàng)
  4. ☑️ **`dim_date`** (Lịch ngày tháng)
  5. ☑️ **`fact_sales`** (Bảng sự kiện giao dịch) *(hoặc bảng tổng hợp `agg_daily_store_sales`)*.
* Bấm nút màu vàng: **`Load`**!

---

# 5. DỰNG MÔ HÌNH QUAN HỆ STAR SCHEMA (SEMANTIC MODEL)

Sau khi bấm Load, bạn click vào biểu tượng thứ 3 ở thanh menu bên trái: **`Model View` (Mô hình hóa)**:
* Bạn sẽ thấy 5 khối hộp đại diện cho 5 bảng.
* Hãy xếp bảng **`fact_sales` nằm ở chính giữa**, và 4 bảng **`dim_*` nằm xung quanh hình ngôi sao**!

```
               ┌────────────────┐
               │   dim_store    │
               │  (D_SHOP_FLC)  │
               └───────┬────────┘
                       │ 1
                       │
                       │ *
┌─────────────┐ *      ▼      * ┌─────────────┐
│ dim_product │◄─── fact_sales ───►│dim_customer │
└─────────────┘                 └─────────────┘
                       ▲ *
                       │
                       │ 1
               ┌───────┴────────┐
               │    dim_date    │
               └────────────────┘
```

### Cách nối dây quan hệ 1-Nhiều (1 to Many):
* Dùng chuột kéo cột **`store_key`** từ bảng `dim_store` $\rightarrow$ Thả đè lên cột **`store_key`** của bảng `fact_sales`.
* Dùng chuột kéo cột **`product_key`** từ `dim_product` $\rightarrow$ Thả đè lên `product_key` của `fact_sales`.
* Dùng chuột kéo cột **`customer_key`** từ `dim_customer` $\rightarrow$ Thả đè lên `customer_key` của `fact_sales`.
* Dùng chuột kéo cột **`date`** từ `dim_date` $\rightarrow$ Thả đè lên cột **`order_date`** của `fact_sales`.

👉 **Kết quả:** Các đường dây nối có số **`1`** ở phía bảng Dim và dấu sao **`*`** ở phía bảng Fact sẽ hiện ra! Đây chính là **Star Schema chuẩn chỉnh** của dự án!

---

# 6. NHẬP MÔN NGÔN NGỮ DAX (VIẾT CÁC CHỈ SỐ KPI)

**DAX (Data Analysis Expressions)** là ngôn ngữ công thức tính toán của Power BI (rất giống công thức hàm Excel nhưng mạnh mẽ hơn nhiều lần).

Để tạo một chỉ số KPI (gọi là Measure):
1. Click chuột phải vào bảng `fact_sales` $\rightarrow$ Chọn **`New measure`**.
2. Viết 3 công thức vàng sau:

### Measure 1: Tổng Doanh Thu (Total Sales USD)
```dax
Total Sales USD = SUM(fact_sales[sales_amount_usd])
```

### Measure 2: Tổng Lợi Nhuận Gộp (Gross Profit USD)
```dax
Total Profit USD = SUM(fact_sales[gross_profit_usd])
```

### Measure 3: Tỷ Suất Lợi Nhuận (Profit Margin %)
```dax
Profit Margin % = DIVIDE([Total Profit USD], [Total Sales USD], 0)
```

---

# 7. THIẾT KẾ DASHBOARD `D_SHOP_FLC` CHUYÊN NGHIỆP (BUỔI 4.4)

Quay trở lại màn hình đầu tiên: **`Report View` (icon biểu đồ)** để vẽ Dashboard:

1. **Bộ 3 Thẻ số KPI (Card visual):**
   * Chọn biểu tượng **Card (1 2 3)** trong bảng Visualizations.
   * Kéo thả `[Total Sales USD]` vào $\rightarrow$ Hiển thị con số doanh thu khổng lồ nổi bật.
   * Tương tự tạo thêm 2 thẻ cho `[Total Profit USD]` và `[Profit Margin %]`.
2. **Biểu đồ Cột - Doanh số theo Cửa hàng (Clustered bar chart):**
   * Trục Y (Y-axis): Kéo `dim_store[state]` hoặc `dim_store[store_key]`.
   * Trục X (X-axis): Kéo `[Total Sales USD]`.
   * 👉 Bạn sẽ thấy ngay bảng xếp hạng cửa hàng Nevada, Kansas... y hệt như câu query SQL vừa nãy!
3. **Biểu đồ Đường - Xu hướng theo Tháng (Line chart):**
   * Trục X: Kéo `dim_date[month]`.
   * Trục Y: Kéo `[Total Sales USD]`.
   * 👉 Hiển thị đồ thị doanh thu lên xuống theo từng tháng trong năm.
4. **Thanh lọc Quốc gia (Slicer):**
   * Chọn biểu tượng **Slicer (Hình cái phễu)** $\rightarrow$ Kéo cột `dim_store[country]` vào.
   * 👉 Khi click vào chữ "United States", toàn bộ biểu đồ trên trang lập tức tự động đổi số liệu lọc riêng cho nước Mỹ!

---

# 8. BỘ CÂU HỎI PHỎNG VẤN TUYỂN DỤNG CỐT LÕI VỀ POWER BI

### Câu 1: Sự khác biệt lớn nhất giữa Import Mode và DirectQuery Mode trong Power BI là gì? Khi nào em bắt buộc phải dùng DirectQuery?
> **Đáp án:**  
> - **Import Mode** kéo toàn bộ dữ liệu về lưu nén vào bộ nhớ RAM máy tính. Tốc độ tương tác rất nhanh nhưng bị giới hạn kích thước RAM và dữ liệu không phải thời gian thực.  
> - **DirectQuery** không kéo dữ liệu về máy mà gửi trực tiếp câu lệnh SQL sang cụm Databricks SQL Warehouse mỗi khi người dùng tương tác với biểu đồ. Em bắt buộc phải dùng DirectQuery khi tập dữ liệu giao dịch quá lớn (hàng chục đến hàng trăm triệu dòng) vượt quá giới hạn RAM của Power BI, hoặc khi doanh nghiệp yêu cầu dữ liệu hiển thị trên Dashboard phải cập nhật theo thời gian thực (Real-time).

### Câu 2: Trong Power BI, tại sao ta nên viết Measure bằng DAX thay vì kéo thả trực tiếp cột số từ bảng vào biểu đồ?
> **Đáp án:**  
> Viết Measure bằng DAX mang lại 3 ưu điểm vượt trội:  
> 1. Có thể tái sử dụng trong nhiều biểu đồ khác nhau mà không phải cấu hình lại.  
> 2. Cho phép xử lý logic phức tạp (như tính toán theo ngữ cảnh thời gian Time Intelligence, tính tỷ lệ phần trăm phân bổ).  
> 3. Tối ưu hóa hiệu năng truy vấn và giúp Semantic Model được tổ chức chuyên nghiệp, dễ bảo trì theo chuẩn doanh nghiệp.

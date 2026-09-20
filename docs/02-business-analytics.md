# FastOrder Business & Analytics Scope

## 1. Mục đích

Tài liệu này mô tả:

- FastOrder là doanh nghiệp giả lập như thế nào;
- stakeholder nào cần dữ liệu;
- những câu hỏi phân tích nào platform phải hỗ trợ;
- các KPI chính;
- giới hạn phân tích hiện tại;
- phạm vi dashboard v1.

Tài liệu không mô tả implementation chi tiết của Spark, Airflow hay MinIO.

---

# 2. Business Context

FastOrder được mô hình hóa như một **B2C e-commerce marketplace**.

Business context được thiết kế để mô phỏng các vấn đề thường xuất hiện trong một hệ thống thương mại điện tử thực tế:

```text
Customer
   ↓
Browse
   ↓
Add to Cart
   ↓
Checkout
   ↓
Payment
   ↓
Order Created
   ↓
Inventory Reserved
   ↓
Packing
   ↓
Shipping
   ↓
Delivered
   ↓
Review
```

FastOrder có:

- nhiều customers;
- nhiều sellers;
- nhiều products;
- nhiều warehouses;
- inventory;
- order lifecycle;
- payments;
- deliveries;
- customer reviews;
- behavioral clickstream;
- external weather context.

Quy mô business ban đầu được thiết kế theo hướng enterprise-style:

```text
~50,000 orders/day
~200 sellers
5 warehouses
```

Các con số trên là **business design context**, không phải row count hiện tại của Olist seed dataset.

---

# 3. Tại sao không dùng Olist như một CSV analytics project?

FastOrder không được thiết kế như:

```text
Olist CSV
   ↓
Pandas
   ↓
Dashboard
```

Thay vào đó:

```text
Olist
   ↓
Operational Database Seed
   ↓
FastOrder OLTP
   ↓
Data Platform
```

Điều này giúp project mô phỏng các bài toán Data Engineering thực tế hơn:

- incremental ingestion;
- source updates;
- replay;
- late changes;
- warehouse operations;
- APIs;
- file ingestion;
- multiple data domains;
- analytical modeling.

---

# 4. Stakeholders

## 4.1. Executive / CEO

Quan tâm:

- Revenue;
- Orders;
- Customers;
- growth;
- Average Order Value;
- high-level business performance.

Ví dụ câu hỏi:

```text
Revenue đang tăng hay giảm?
Orders tăng hay giảm so với kỳ trước?
Customer base đang thay đổi thế nào?
State nào đóng góp lớn?
Category nào đang tạo nhiều giá trị?
```

---

## 4.2. Sales

Quan tâm:

- product categories;
- sellers;
- items sold;
- merchandise value;
- order volume.

Ví dụ:

```text
Category nào bán tốt nhất?
Seller nào tạo nhiều merchandise value?
Sản phẩm nào bán nhiều?
Average selling price khác nhau thế nào?
```

---

## 4.3. Operations / Logistics

Quan tâm:

- warehouses;
- delivery speed;
- delivered orders;
- cancellations;
- freight;
- weather context.

Ví dụ:

```text
Warehouse nào xử lý nhiều đơn?
Average delivery time là bao nhiêu?
Delivered rate giữa các warehouse khác nhau thế nào?
Weather có trùng với các giai đoạn vận hành bất thường hay không?
```

---

## 4.4. Inventory

Quan tâm:

- warehouse;
- product;
- available inventory;
- stock movement;
- fulfillment capability.

Inventory hiện chủ yếu đóng vai trò operational context trong platform.

Phân tích inventory sâu hơn được để ngoài BI scope v1.

---

## 4.5. Marketing / Product

Quan tâm:

- sessions;
- events;
- user engagement;
- active sessions;
- session duration;
- browsing behavior.

Ví dụ:

```text
Traffic thay đổi theo ngày như thế nào?
Một session trung bình có bao nhiêu events?
Session kéo dài bao lâu?
Bao nhiêu session trải qua nhiều ngày?
```

---

## 4.6. Finance

Quan tâm:

- payment value;
- revenue;
- freight;
- order value;
- average order value.

---

# 5. Commerce Analytics

## 5.1. Executive KPIs

Các KPI chính:

```text
Total Revenue
Total Orders
Total Customers
Average Order Value
Items Sold
```

### Total Revenue

Nguồn:

```text
Fact Orders
```

Được tính từ tổng payment value của orders.

### Total Orders

Grain:

```text
1 row / order
```

### Total Customers

Distinct customers xuất hiện trong order context.

### Average Order Value

```text
Total Revenue / Total Orders
```

### Items Sold

Nguồn:

```text
Fact Order Items
```

---

# 6. Order Analytics

Các chỉ số:

```text
Total Orders
Delivered Orders
Cancelled Orders
Delivered Rate
Cancellation Rate
Average Delivery Days
```

Các câu hỏi:

```text
Order status distribution như thế nào?
Bao nhiêu % orders được delivered?
Bao nhiêu % orders bị cancelled?
Delivery mất trung bình bao nhiêu ngày?
```

---

# 7. Product Analytics

Product analysis dựa trên:

```text
Fact Order Items
```

không dựa trực tiếp vào payment-level `Fact Orders`.

Các metrics:

```text
Merchandise Value
Order Item Value
Items Sold
Item Orders
Average Selling Price per Item
Freight Share %
```

Các câu hỏi:

```text
Category nào tạo merchandise value cao nhất?
Category nào bán nhiều items nhất?
Product nào có item value cao?
Average selling price khác nhau ra sao?
```

---

# 8. Seller Analytics

Seller chỉ tồn tại ở item grain.

Do đó Seller analytics phải sử dụng các item-level measures như:

```text
Merchandise Value
Items Sold
Item Orders
Freight Value
```

Không dùng trực tiếp:

```text
Total Revenue
Average Order Value
```

nếu relationship hiện tại không cho Seller filter `Fact Orders`.

---

# 9. Customer & Geography Analytics

Customer dimension hỗ trợ:

```text
state
city
customer
```

Các câu hỏi:

```text
State nào tạo revenue cao nhất?
State nào có nhiều orders nhất?
AOV giữa các state khác nhau ra sao?
Customer distribution thay đổi thế nào?
```

Executive dashboard sử dụng dynamic metric analysis cho Customer State.

---

# 10. Warehouse Analytics

FastOrder có 5 warehouses:

```text
WH_HCM
WH_HN
WH_DN
WH_CT
WH_HP
```

Olist gốc không có warehouse assignment phù hợp với FastOrder business model.

Do FastOrder xem Olist như operational seed của một backend giả lập, warehouse được bổ sung như một **synthetic operational attribute**.

Assignment được thiết kế:

- deterministic;
- stable giữa các rerun;
- một order thuộc một warehouse;
- order items của cùng order sử dụng warehouse tương ứng.

Phân bố mục tiêu:

```text
WH_HCM ≈ 30%
WH_HN  ≈ 25%
WH_DN  ≈ 20%
WH_CT  ≈ 15%
WH_HP  ≈ 10%
```

Warehouse assignment là assumption của FastOrder simulation, không phải attribute quan sát trực tiếp từ Olist.

---

# 11. Clickstream Analytics

YOOCHOOSE được sử dụng để mô phỏng behavioral data.

Các KPI chính:

```text
Total Sessions
Total Events
Average Events per Session
Average Session Duration
Cross-Date Sessions
Cross-Date Session Rate
Average Daily Active Sessions
Average Daily Distinct Items
```

---

# 12. Session Semantics

`fact_clickstream_sessions` có grain:

```text
1 row / session
```

Một session có:

```text
session_start_at
session_end_at
event_count
session_duration_seconds
is_cross_date_session
```

`Total Sessions` thường được phân tích theo:

```text
session start date
```

Trong khi:

```text
active_session_count
```

có thể đếm session active trên một ngày dù session bắt đầu ở ngày khác.

Hai metric này không có cùng semantics.

---

# 13. Không tạo Conversion Rate giữa Clickstream và Orders

YOOCHOOSE và Olist/FastOrder không chia sẻ business key có thể chứng minh:

```text
YOOCHOOSE item_id
≠
FastOrder product_id
```

Do đó hiện tại không có cơ sở đáng tin cậy để tính:

```text
session → order conversion
click → purchase conversion
product view → purchase conversion
```

FastOrder ưu tiên analytical correctness hơn một dashboard conversion đẹp nhưng không có lineage hợp lệ.

---

# 14. Weather Analytics

Weather context được thu thập theo:

```text
warehouse
+
time
```

Các metrics:

```text
Average Temperature
Average Humidity
Total Precipitation
Average Wind Speed
```

Weather có hai loại analytical data:

```text
Historical Weather Context
Forecast Weather Context
```

---

# 15. Weather Interpretation Boundary

Có thể đặt weather và operational performance cạnh nhau để tìm pattern.

Ví dụ:

```text
mưa lớn
+
delivery time tăng
```

có thể được xem là analytical correlation candidate.

Tuy nhiên dashboard không được tự động kết luận:

```text
weather CAUSED delivery delay
```

nếu chưa có causal analysis phù hợp.

---

# 16. Forecast Semantics

Forecast data có nhiều snapshots.

Ví dụ:

```text
snapshot A:
forecast cho 10:00 ngày mai

snapshot B:
forecast mới hơn cho cùng 10:00 ngày mai
```

Do đó một forecast chart phải hiểu rõ:

```text
forecast_time
vs
snapshot / ingestion time
```

Nếu tổng hợp tất cả snapshots mà không chọn snapshot phù hợp, metric có thể gây hiểu nhầm.

---

# 17. Analytical Fact Boundaries

## Fact Orders

Phù hợp với:

```text
Revenue
Orders
Customers
Delivery
Order Status
AOV
```

## Fact Order Items

Phù hợp với:

```text
Products
Sellers
Items Sold
Merchandise Value
Freight
```

## Fact Clickstream Sessions

Phù hợp với:

```text
Sessions
Events
Duration
Session Behavior
```

## Weather Facts

Phù hợp với:

```text
Weather by warehouse/time
```

---

# 18. BI Scope v1

Power BI v1 tập trung hoàn thiện:

```text
Executive Overview
```

Executive Overview hiện hướng tới:

```text
Revenue
Orders
Customers
AOV
Items Sold
Metric Trend
Customer State
Order Status
Product Categories
Top Products
Warehouse Performance
```

---

# 19. Future BI Pages

Các dashboard sau được giữ là future enhancement:

```text
Product & Seller Performance
Customer & Geography
Digital Behavior
Weather & Operations Context
```

Việc chưa dựng đầy đủ các page này không đồng nghĩa Data Platform chưa hoàn thành.

Power BI là consumption layer của một project có trọng tâm chính là Data Engineering.

---

# 20. Analytical Design Principles

FastOrder sử dụng các nguyên tắc sau:

```text
Business question
    ↓
Correct Fact grain
    ↓
Correct Dimension
    ↓
Correct Measure
    ↓
Visualization
```

Không làm ngược lại:

```text
Muốn có chart
    ↓
ép relationship
    ↓
tạo metric không có business meaning
```

---

# 21. Analytical Scope Summary

FastOrder hiện có khả năng hỗ trợ bốn nhóm analytics lớn:

```text
Commerce
Operations
Digital Behavior
Weather Context
```

Thông qua một analytical model chung nhưng vẫn giữ domain boundaries khi dữ liệu không có shared business key.

Mục tiêu của platform không phải tạo số lượng dashboard lớn nhất, mà là đảm bảo:

```text
correct data
+
correct grain
+
correct relationships
+
reproducible pipelines
+
explainable analytics
```
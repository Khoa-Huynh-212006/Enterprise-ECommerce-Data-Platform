# FastOrder OLAP / Analytical Schema

## 1. Mục đích

Tài liệu này mô tả analytical dimensional model của FastOrder.

Ở đây "OLAP schema" được hiểu là:

```text
Facts
Dimensions
Business Grain
Relationships
Analytical Measures
```

không phải một SSAS multidimensional cube.

---

# 2. Modeling Principles

FastOrder sử dụng các nguyên tắc:

```text
Fact
= event / transaction / measurement

Dimension
= descriptive analytical context
```

Relationship mặc định:

```text
Dimension
   1
   │
   *
 Fact
```

Cross-filter:

```text
Single direction
Dimension → Fact
```

Không tạo fact-to-fact relationship chỉ để chart hoạt động.

---

# 3. Current Analytical Marts

## Dimensions

```text
dim_date
dim_customers
dim_products
dim_sellers
dim_warehouses
```

## Commerce Facts

```text
fact_orders
fact_order_items
```

## Clickstream

```text
fact_clickstream_sessions
agg_clickstream_daily
```

## Weather

```text
fact_weather_forecast_hourly
fact_weather_historical_forecast_hourly
```

---

# 4. Model Overview

```text
                    ┌──────────────┐
                    │   Dim Date   │
                    └──────┬───────┘
                           │
       ┌───────────────────┼─────────────────────┐
       │                   │                     │
       ▼                   ▼                     ▼
 Fact Orders      Fact Order Items        Clickstream Facts
       │                   │
       │                   │
       ▼                   ▼
Dim Customers        Dim Products
Dim Warehouses       Dim Sellers
                     Dim Warehouses


Dim Date ───────────────► Weather Facts
Dim Warehouses ─────────► Weather Facts
```

---

# 5. `dim_date`

## Grain

```text
1 row / calendar date
```

Vai trò:

- conformed date dimension;
- year;
- month;
- month name;
- day;
- day of week;
- analytical date filtering.

Primary analytical key:

```text
date_key
```

Business date:

```text
date_day
```

---

# 6. `dim_customers`

## Grain

```text
1 row / customer
```

Chứa descriptive attributes như:

```text
customer_id
city
state_code
```

Được sử dụng cho:

```text
customer analytics
geography
state analysis
```

---

# 7. `dim_products`

## Grain

```text
1 row / product
```

Ví dụ attributes:

```text
product_id
category
category_name_english
```

Dùng chủ yếu với:

```text
fact_order_items
```

---

# 8. `dim_sellers`

## Grain

```text
1 row / seller
```

Seller analysis sử dụng item-level Fact.

---

# 9. `dim_warehouses`

## Grain

```text
1 row / warehouse
```

FastOrder hiện có 5 warehouses:

```text
WH_HCM
WH_HN
WH_DN
WH_CT
WH_HP
```

Dimension này được conformed giữa:

```text
Commerce
Weather
```

---

# 10. `fact_orders`

## Grain

```text
1 row / order
```

Natural business key:

```text
order_id
```

Một số analytical attributes:

```text
order_id
customer_id
order_warehouse_id
order_status
order_purchase_timestamp
order_approved_at
order_delivered_carrier_date
order_delivered_customer_date

purchase_date_key
approved_date_key
carrier_date_key
delivered_date_key
estimated_delivery_date_key

delivery_days
distinct_product_count
distinct_seller_count
distinct_warehouse_count
total_payment_value
```

---

# 11. `fact_orders` Measures

Các measures phù hợp:

```text
Total Orders
Total Revenue
Total Customers
Average Order Value
Delivered Orders
Cancelled Orders
Delivered Rate
Cancellation Rate
Average Delivery Days
```

---

# 12. Fact Orders Relationships

```text
Dim Customers[customer_id]
        1
        │
        *
Fact Orders[customer_id]
```

```text
Dim Warehouses[warehouse_id]
        1
        │
        *
Fact Orders[order_warehouse_id]
```

Active date relationship:

```text
Dim Date[date_key]
        1
        │
        *
Fact Orders[purchase_date_key]
```

Các role-playing date keys còn lại hiện không cần active relationships nếu business requirement chưa sử dụng.

---

# 13. `fact_order_items`

## Grain

```text
1 row / order item
```

Business key:

```text
(order_id, order_item_id)
```

Một số columns:

```text
order_id
order_item_id
customer_id
product_id
seller_id
warehouse_id
order_status

order_purchase_timestamp
shipping_limit_date

purchase_date_key
shipping_limit_date_key

quantity
unit_price
freight_value
merchandise_value
item_total_value
item_line_count
```

---

# 14. Fact Order Items Measures

```text
Items Sold
Merchandise Value
Freight Value
Order Item Value
Item Orders
Distinct Products
Active Sellers
Average Selling Price per Item
Freight Share %
```

---

# 15. Fact Order Items Relationships

```text
Dim Customers
     ↓
Fact Order Items
```

```text
Dim Products
     ↓
Fact Order Items
```

```text
Dim Sellers
     ↓
Fact Order Items
```

```text
Dim Warehouses
     ↓
Fact Order Items
```

```text
Dim Date
     ↓
Fact Order Items[purchase_date_key]
```

---

# 16. Không tạo Fact Orders ↔ Fact Order Items relationship

Dù hai facts cùng có:

```text
order_id
```

Power BI model không tạo relationship:

```text
Fact Orders
    ↔
Fact Order Items
```

Lý do:

- tránh fact-to-fact ambiguity;
- tránh double filtering;
- giữ star schema semantics;
- mỗi Fact sử dụng shared Dimensions.

---

# 17. Commerce Measure Boundary

Một số metric thuộc order grain:

```text
Total Revenue
Total Orders
AOV
Delivery Days
```

Một số metric thuộc item grain:

```text
Merchandise Value
Items Sold
Freight
Seller Performance
Product Performance
```

Do đó:

```text
Product → Total Revenue
```

không nên được giả định nếu Product không filter được Fact Orders.

---

# 18. `fact_clickstream_sessions`

## Grain

```text
1 row / session
```

Một số columns:

```text
session_id
session_start_at
session_end_at
start_date_key
end_date_key

event_count
session_duration_seconds
is_cross_date_session
session_count
```

---

# 19. Clickstream Measures

```text
Total Sessions
Total Events
Average Events per Session
Average Session Duration
Cross-Date Sessions
Cross-Date Session Rate
```

---

# 20. Clickstream Date Relationship

Active relationship:

```text
Dim Date[date_key]
        1
        │
        *
Fact Clickstream Sessions[start_date_key]
```

`end_date_key` không cần active relationship trong current BI scope.

---

# 21. `agg_clickstream_daily`

## Grain

```text
1 row / event date
```

Columns gồm:

```text
date_key
event_date
sessions_started
active_session_count
event_count
distinct_item_count
distinct_category_count
avg_session_duration_seconds
```

Relationship:

```text
Dim Date
   ↓
Agg Clickstream Daily
```

---

# 22. Sessions Started vs Active Sessions

Hai khái niệm khác nhau:

```text
sessions_started
= session bắt đầu trong ngày
```

```text
active_session_count
= session có activity trong ngày
```

Cross-date sessions có thể làm hai số này khác nhau.

---

# 23. Không join Clickstream sang Commerce

Không tạo:

```text
Clickstream → Dim Products
Clickstream → Fact Orders
Clickstream → Dim Customers
```

vì YOOCHOOSE identifiers không được chứng minh tương ứng với FastOrder/Olist identifiers.

---

# 24. `fact_weather_forecast_hourly`

## Grain

```text
warehouse_id
+
ingestion_id
+
forecast_time
```

Forecast snapshot dimension được giữ thông qua:

```text
ingestion_id
snapshot timestamp
```

Một forecast time có thể xuất hiện trong nhiều forecast snapshots.

---

# 25. Forecast Relationships

```text
Dim Warehouses[warehouse_id]
        1
        │
        *
Fact Weather Forecast[warehouse_id]
```

```text
Dim Date[date_key]
        1
        │
        *
Fact Weather Forecast[forecast_date_key]
```

Date filter đại diện cho:

```text
forecasted date
```

không phải snapshot date.

---

# 26. `fact_weather_historical_forecast_hourly`

## Grain

```text
warehouse_id
+
weather_time
```

Relationship:

```text
Dim Warehouses
     ↓
Historical Weather
```

```text
Dim Date
     ↓
Historical Weather
```

---

# 27. Weather Measures

Historical:

```text
Historical Avg Temperature
Historical Avg Humidity
Historical Total Precipitation
Historical Avg Wind Speed
```

Forecast:

```text
Forecast Avg Temperature
Forecast Avg Humidity
Forecast Total Precipitation
Forecast Avg Wind Speed
```

---

# 28. Forecast Analytical Warning

Forecast should generally be interpreted with one appropriate snapshot.

Aggregating blindly across:

```text
multiple ingestion snapshots
```

có thể double-count cùng forecast time.

---

# 29. Conformed Dimensions

Hiện tại hai conformed Dimensions quan trọng nhất xuyên domain là:

```text
Dim Date
Dim Warehouses
```

### Dim Date

Shared bởi:

```text
Commerce
Clickstream
Weather
```

### Dim Warehouses

Shared bởi:

```text
Commerce
Weather
```

---

# 30. Current Power BI Relationships

## Commerce

```text
Dim Customers 1 → * Fact Orders
Dim Customers 1 → * Fact Order Items

Dim Products 1 → * Fact Order Items

Dim Sellers 1 → * Fact Order Items

Dim Warehouses 1 → * Fact Orders
Dim Warehouses 1 → * Fact Order Items

Dim Date 1 → * Fact Orders
Dim Date 1 → * Fact Order Items
```

## Clickstream

```text
Dim Date 1 → * Fact Clickstream Sessions
Dim Date 1 → * Agg Clickstream Daily
```

## Weather

```text
Dim Date 1 → * Fact Weather Forecast
Dim Date 1 → * Fact Weather Historical

Dim Warehouses 1 → * Fact Weather Forecast
Dim Warehouses 1 → * Fact Weather Historical
```

Relationships sử dụng:

```text
Single-direction filtering
Dimension → Fact
```

---

# 31. Current Analytical Model Summary

```text
                     Dim Date
                        │
       ┌────────────────┼────────────────┐
       │                │                │
       ▼                ▼                ▼
 Fact Orders       Clickstream        Weather
       │
       │
       ├── Dim Customers
       └── Dim Warehouses


                     Dim Date
                        │
                        ▼
                Fact Order Items
                 │      │      │
                 ▼      ▼      ▼
             Products Sellers Warehouses
```

Model ưu tiên:

```text
correct grain
correct relationships
correct measure semantics
```

thay vì cố đưa mọi domain vào một connected graph.
# Ma trận Stakeholders & Yêu cầu KPIs

Hệ thống Data Platform phục vụ 7 nhóm Stakeholders chính. Dưới đây là chi tiết mục tiêu và yêu cầu dữ liệu của từng nhóm để thiết kế các Data Mart tương ứng.

| Stakeholder | Mục tiêu Kinh doanh | Quyết định Cần đưa ra | Dữ liệu Theo dõi (Metrics) | Output | Tần suất |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1. CEO** | Tăng trưởng doanh thu, tăng lợi nhuận, mở rộng thị phần. | Có đạt mục tiêu không? Khu vực/Danh mục nào mang lại lợi nhuận cao nhất? | Revenue, Profit, Orders, Growth Rate, Customer Growth | Executive Data Product | Hàng ngày |
| **2. Seller Performance** | Tăng doanh số bán hàng. | Sản phẩm nào bán chạy? Khu vực nào doanh số thấp? Kênh nào hoạt động hiệu quả? | Orders, Revenue, AOV, Sales by Category / Region | Sales Data Product | Hàng giờ / Hàng ngày |
| **3. Inventory Manager** | Tối ưu tồn kho. | Khi nào cần nhập thêm? Hàng nào sắp hết hoặc tồn kho quá lâu? | Inventory Level, Stock In/Out, Inventory Turnover, Stock-out Rate | Inventory Data Product | Hàng giờ |
| **4. Logistics Manager** | Nâng cao hiệu quả giao hàng. | Đơn vị VC nào tốt nhất? Bao nhiêu đơn trễ? Khu vực nào giao chậm nhất? | Shipping Status, Delivery Time, On-time Delivery Rate, Delayed Orders | Logistics Data Product | Hàng giờ |
| **5. Marketing Manager** | Tăng hiệu quả chiến dịch. | Campaign nào hiệu quả nhất? Kênh nào mang lại doanh thu cao nhất? | Conversion Rate, ROAS, Clicks, Impressions, Campaign Revenue | Marketing Data Product | Hàng ngày |
| **6. Finance Manager** | Kiểm soát chi phí, tối đa lợi nhuận. | Biên lợi nhuận hiện tại? Chi phí nhập tăng không? Tỷ giá ảnh hưởng thế nào? | Revenue, Cost, Profit, Profit Margin, Exchange Rate | Finance Data Product | Hàng ngày |
| **7. Customer Service** | Nâng cao trải nghiệm KH. | Sản phẩm nào bị khiếu nại nhiều? Nguyên nhân hoàn trả? Đánh giá có giảm không? | Customer Reviews, Return Orders, Complaint Rate, Customer Satisfaction | CX Data Product | Hàng ngày |
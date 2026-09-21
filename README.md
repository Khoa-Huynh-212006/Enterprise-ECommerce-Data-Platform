# FastOrder — Enterprise E-Commerce Data Platform

FastOrder is a local-first, end-to-end Data Engineering project that simulates an e-commerce data platform processing operational, API, and clickstream data.

The platform ingests data from PostgreSQL, Open-Meteo APIs, and YOOCHOOSE files; stores raw and curated data in MinIO; transforms large-scale datasets with Apache Spark and Delta Lake; loads analytical data into PostgreSQL; models business-ready marts with dbt; and serves analytics through Power BI.

<p align="center">
  <img src="assets/fastorder-architecture.jpg" alt="FastOrder Architecture" width="100%">
</p>

## Project at a Glance

| Area | Implementation |
|---|---|
| **Operational Data** | 11 PostgreSQL tables with incremental ingestion |
| **Clickstream Data** | ~33M events, ~9.2M sessions, 183 days |
| **External API** | Open-Meteo forecast and historical weather |
| **Data Lake** | MinIO Bronze + Delta Lake Silver |
| **Processing** | Apache Spark / PySpark |
| **Orchestration** | Apache Airflow 3 |
| **Data Warehouse** | PostgreSQL |
| **Transformation** | dbt Core |
| **Analytics** | Power BI |
| **Runtime** | Docker Compose, local-first and zero-cost |

## Business Context

FastOrder represents a B2C e-commerce marketplace operating across five warehouses.

The platform is designed to support analytical questions around:

- revenue and order performance;
- customer and product behavior;
- seller performance;
- warehouse operations;
- delivery performance;
- digital clickstream behavior;
- weather context around warehouse operations.

The objective is not only to move data between systems, but to build a pipeline that can be **replayed, validated, recovered after failure, and consumed reliably by downstream analytics**.

## End-to-End Data Flow

```text
PostgreSQL OLTP
        │
        │ Incremental ingestion
        ▼
     MinIO Bronze
        │
        │
        ▼
Apache Spark + Delta Lake
        │
        ▼
     MinIO Silver
        │
        │ Spark JDBC loading
        ▼
PostgreSQL Data Warehouse
        │
        ├── staging
        ├── intermediate
        └── marts
             │
             ▼
           dbt
             │
             ▼
         Power BI
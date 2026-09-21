# FastOrder — Enterprise E-Commerce Data Platform

FastOrder is a local-first, end-to-end Data Engineering project that
integrates operational databases, REST APIs, and large-scale
clickstream files into an analytical platform for business reporting.

The platform follows a layered architecture:

**PostgreSQL OLTP → MinIO Bronze → Spark / Delta Lake Silver →
PostgreSQL DWH → dbt Marts → Power BI**

<p align="center">
  <img
    src="assets/fastorder-architecture.jpg"
    alt="FastOrder Data Platform Architecture"
    width="100%">
</p>

## Project at a Glance

| Area | Implementation |
|---|---|
| **Operational Data** | 11 PostgreSQL tables with incremental ingestion |
| **Clickstream** | ~33M events, ~9.2M sessions, 183 days |
| **External API** | Open-Meteo forecast and historical weather |
| **Orchestration** | Apache Airflow 3 |
| **Processing** | Apache Spark, PySpark |
| **Data Lake** | MinIO, Delta Lake |
| **Data Warehouse** | PostgreSQL |
| **Transformation** | dbt Core |
| **Analytics** | Power BI |
| **Runtime** | Docker Compose |

## Business Context

FastOrder represents a B2C e-commerce marketplace operating across
five warehouses.

The platform supports analytics for:

- revenue and order performance;
- customers, products, and sellers;
- warehouse and delivery operations;
- digital clickstream behavior;
- weather context around warehouse operations.

The goal is not only to move data between systems, but to build
pipelines that are **replay-safe, recoverable, testable, and reliable
for downstream analytics**.

## Key Engineering Highlights

### Reliable Incremental Ingestion

Incremental ingestion is implemented for **11 operational tables**
using composite watermarks based on:

`(updated_at, primary_key...)`

The framework includes:

- checkpoint-based progress tracking;
- fixed upper watermarks per run;
- pending-batch recovery;
- deterministic Bronze identities;
- replay-safe processing;
- source-to-Bronze validation.

### Large-Scale Clickstream Processing

The YOOCHOOSE dataset contains approximately:

- **33M clickstream events**
- **9.2M sessions**
- **183 days of activity**

Apache Spark and Delta Lake are used to transform event-level data
into analytical datasets.

Current clickstream marts include:

- `fact_clickstream_sessions`
- `agg_clickstream_daily`

### End-to-End Orchestration

Apache Airflow orchestrates ingestion, transformation, warehouse
loading, dbt modeling, and final data-quality validation across:

- operational data;
- weather forecast data;
- clickstream data.

Historical weather is maintained as a separate backfill workflow
instead of being executed during every normal platform run.

<p align="center">
  <img
    src="assets/airflow-platform-e2e.jpg"
    alt="FastOrder Airflow End-to-End Pipeline"
    width="100%">
</p>

## Engineering Trade-offs

### Local-first infrastructure

FastOrder uses **Docker Compose, MinIO, and PostgreSQL** instead of
managed cloud services.

This keeps the project zero-cost and reproducible locally, at the
trade-off of being limited by the resources of a single development
machine.

### PostgreSQL as the analytical warehouse

PostgreSQL is sufficient for the current project scale and integrates
cleanly with dbt and Power BI.

A dedicated OLAP engine would become more appropriate at substantially
larger analytical scale.

### Session-level clickstream marts

The project does not copy all ~33M clickstream events into another DWH
fact table without a clear business requirement.

Event-level data remains available in Silver, while the current DWH
focuses on session-level and daily analytics.

### No artificial cross-domain joins

YOOCHOOSE `item_id` and FastOrder/Olist `product_id` do not share a
trustworthy business key.

The project therefore does not fabricate a relationship between them
simply to make the analytical schema appear fully connected.

## Analytical Models

### Core

- `dim_customers`
- `dim_products`
- `dim_sellers`
- `dim_warehouses`
- `dim_date`
- `fact_orders`
- `fact_order_items`

### Clickstream

- `fact_clickstream_sessions`
- `agg_clickstream_daily`

### Weather

- `fact_weather_forecast_hourly`
- `fact_weather_historical_forecast_hourly`

## Current Scale

| Model | Approx. Rows |
|---|---:|
| `fact_orders` | 99,492 |
| `fact_order_items` | 112,843 |
| `fact_clickstream_sessions` | 9,249,729 |
| `agg_clickstream_daily` | 183 |
| `fact_weather_forecast_hourly` | 1,920 |
| `fact_weather_historical_forecast_hourly` | 12,240 |

## Power BI

The current dashboard focuses on an **Executive Overview** with KPIs
for revenue, orders, customers, fulfillment, warehouse performance,
and related operational metrics.

<p align="center">
  <img
    src="assets/dashboard-executive-overview.png"
    alt="FastOrder Executive Overview"
    width="100%">
</p>

## Technology Stack

**Languages:** Python, SQL

**Data Engineering:** Apache Airflow, Apache Spark, PySpark,
Delta Lake, dbt

**Databases & Storage:** PostgreSQL, MinIO

**Analytics:** Power BI, DAX

**Infrastructure:** Docker, Docker Compose

## Documentation

Detailed technical documentation:

- [`01-data-flows.md`](docs/01-data-flows.md)
- [`02-business-analytics.md`](docs/02-business-analytics.md)
- [`03-technology-architecture.md`](docs/03-technology-architecture.md)
- [`04-olap-schema.md`](docs/04-olap-schema.md)
- [`05-decision-log.md`](docs/05-decision-log.md)
- [`06-project-timeline.md`](docs/06-project-timeline.md)
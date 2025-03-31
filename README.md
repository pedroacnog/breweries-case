# BEES Data Engineering – Breweries Case

🇧🇷 [Leia a versão em português](README_ptbr.md)

## 🎯 Objective

This project demonstrates the implementation of a data pipeline based on the Medallion architecture (Bronze, Silver, Gold), using PySpark, Apache Airflow, and MinIO for data storage. The data comes from the [Open Brewery DB API](https://www.openbrewerydb.org/).

---

## 🧱 Pipeline Architecture

### Bronze
- Responsible for ingesting raw data from the API.
- Data is persisted in JSON format with a timestamp in the filename.
- Structure: `s3a://bronze/breweries_YYYY-MM-DD_HH-MM-SS.json`

### Silver
- Transformation and cleansing layer:
  - Character normalization (removing accents).
  - Conversion to columnar format (Parquet).
  - Partitioning by `country` and `state` for easier consumption.
  - Creation of the `active_status` column to assist in filters.
  - Dropping of columns like `phone` and `website_url` as they were not relevant to the requested aggregations.
  - Concatenation of `address_1`, `address_2`, and `address_3` into a single `address` column.
  - Explicit schema definition applied to the raw data.

### Gold
- Analytical aggregation layer. The following views are generated:
  - Main view: count of breweries by type and country.
  - Extra view: Top 10 states with the most active breweries.
- Data remains in Parquet format but is no longer partitioned.

> The Gold layer can be extended depending on how the data is consumed: connecting to BI tools, validation in DuckDB, or sending to databases (Redshift, BigQuery, etc).

---

## 🗓️ Orchestration with Airflow

Individual DAGs were created for each step:
- `bronze_breweries_ingestion`
- `silver_breweries_transformation`
- `gold_breweries_aggregations`
- An extra DAG, `orchestration_dag`, orchestrates the full pipeline (Bronze → Silver → Gold).

Each DAG is configured with:
- `retries`, `retry_delay`, and `email_on_failure` (email is set, but not fully configured).
- `schedule_interval` for sequential execution overnight.

---

## 🛡️ Monitoring and Reliability

- Logging has been included in all layers.
- Failure handling is managed by Airflow via `BashOperator`, which captures script error codes. This allows failures to be marked as `failed`, stopping the DAG and enabling retries or alerts.
- **Monitoring and alerting suggestions**:
  - Add data quality validations (nulls, valid domains, unexpected values).
  - Integrate Airflow with notification systems like Slack or email via `on_failure_callback`.

---

## 🧪 Tests

Unit tests were added for the `normalize_ascii` function using `pytest` and a local `SparkSession`, covering various input cases including special characters. Additional tests may be added as understanding of the data improves.

---

## 🚢 Containers and Environment

- Fully containerized project using Docker Compose.
- Services:
  - Apache Airflow
  - Apache Spark
  - MinIO

> 🔹 MinIO can be replaced with AWS S3, since communication uses the `s3a://` connector with authentication. Just create the buckets `bronze`, `silver`, and `gold` in the respective provider.

---

## ✉️ Local Execution

1. Clone the repository

2. Build the containers:
```bash
docker compose build
```

3. Start the services:
```bash
docker compose up
```

4. Access MinIO: [http://localhost:9001](http://localhost:9001)  
   - User: `admin`  
   - Password: `admin123`

5. **Manually create the following buckets** in the MinIO UI:
   - `bronze`
   - `silver`
   - `gold`

6. Access Airflow: [http://localhost:8080](http://localhost:8080)  
   - User: `airflow`  
   - Password: `airflow`

7. Manually enable the desired DAGs in the Airflow UI.

   Run the DAGs in the Medallion order, or use the `orchestration_dag` to execute the full pipeline.

8. Check each task's logs in Airflow to verify execution.

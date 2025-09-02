# Test Exercise - Google Cloud Data Warehouse

## 1. Solution Architecture

This project implements a fully **serverless** data pipeline on Google Cloud Platform. The architecture is designed to be scalable, secure, and cost-efficient.

-   **Data Sources**: Two data sources are used:
    1.  A PostgreSQL database managed by **Cloud SQL**, for transactional data such as user records.
    2.  A public REST API for external data, such as cryptocurrency prices.
-   **Ingestion Pipelines**: Two independent **Cloud Run** services, each with its own Docker container, handle data ingestion. This allows each task to run autonomously and scale independently.
-   **Data Warehouse**: **BigQuery** is the core component, acting as the final destination for the data. It is divided into two datasets: `raw` for unprocessed data and `staging` for transformed tables ready for analysis.
-   **Orchestration**: **Cloud Scheduler** triggers the Cloud Run services on a defined schedule, acting as a fully managed task scheduler.

---

## 2. Design Decisions and Evaluation Criteria

-   **Use of GCP Services**:
    -   **Cloud SQL**: Provides a managed, scalable PostgreSQL database, removing the overhead of maintaining a database server.
    -   **Cloud Run**: Its serverless nature means code only runs when needed, scaling down to zero and minimizing compute costs.
    -   **BigQuery**: Enables analysis of petabytes of data without infrastructure management. The use of `raw` and `staging` schemas follows best practices.
    -   **Cloud Scheduler**: Provides a simple and reliable orchestration mechanism to trigger the pipelines.
-   **Incremental Ingestion Logic**: The `postgres_to_bq.py` script implements incremental logic. On the first run, it loads all the data. On subsequent runs, it queries the destination table in BigQuery for the timestamp of the last record and ingests only new data from the source database.
-   **Transformations**: Transformations are performed in BigQuery. The view `users_with_crypto_data` joins the data from `raw.users` and `raw.crypto_prices` tables:

```sql
CREATE OR REPLACE VIEW `testintell.raw.users_with_crypto_data` AS
SELECT
  t1.id,
  t1.name,
  t1.email,
  t2.usd AS bitcoin_price_usd,
  t1.created_at,
  t2.ingestion_time AS crypto_ingestion_time
FROM
  `testintell.raw.users` AS t1
CROSS JOIN
  `testintell.raw.crypto_prices` AS t2
WHERE
  t2.ingestion_time = (SELECT MAX(ingestion_time) FROM `testintell.raw.crypto_prices`);
```sql
-   **Security**: Authentication is managed through GCP service accounts with specific roles (Cloud SQL Client and BigQuery Data Editor). Communication between Cloud Run and Cloud SQL is secure and encrypted through the Cloud SQL Proxy, which is automatically handled by the Python connector.

-   **Production Deployment**: Deployment is performed using the gcloud CLI, which automates the creation of Docker images and the deployment of services to Cloud Run and Cloud Scheduler. Credential configuration is securely passed as environment variables to the Cloud Run services, ensuring no sensitive information is exposed in the code.

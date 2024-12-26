from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
}

with DAG(
    dag_id="bq_transformations_pipeline",
    default_args=default_args,
    description="Pipeline for BigQuery transformations to generate product insights",
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    best_products_query = f"""
    SELECT
        asin,
        COUNT(*) AS total_reviews,
        CAST(ROUND(AVG(overall), 3) AS FLOAT64) AS avg_rating
    FROM
        `{Variable.get('BQ_PROJECT_ID')}.{Variable.get('BQ_DATASET_NAME')}.kindle_reviews`
    GROUP BY
        asin
    HAVING
        total_reviews >= 5
    ORDER BY
        avg_rating DESC
    """

    best_products_task = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_bigquery",
        task_id="best_products",
        configuration={
            "query": {
                "query": best_products_query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": Variable.get('BQ_PROJECT_ID'),
                    "datasetId": Variable.get('BQ_DATASET_NAME'),
                    "tableId": "best_products",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location="US",
    )

    worst_products_query = f"""
    SELECT
        asin,
        COUNT(*) AS total_reviews,
        AVG(overall) AS avg_rating
    FROM
        `{Variable.get('BQ_PROJECT_ID')}.{Variable.get('BQ_DATASET_NAME')}.kindle_reviews`
    GROUP BY
        asin
    HAVING
        total_reviews >= 5
    ORDER BY
        avg_rating ASC
    """

    worst_products_task = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_bigquery",
        task_id="worst_products",
        configuration={
            "query": {
                "query": worst_products_query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": Variable.get('BQ_PROJECT_ID'),
                    "datasetId": Variable.get('BQ_DATASET_NAME'),
                    "tableId": "worst_products",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location="US",
    )

    trending_products_query = f"""
    SELECT
        asin,
        COUNT(*) AS review_count_last_week,
        CAST(ROUND(AVG(overall), 3) AS FLOAT64) AS avg_rating_last_week
    FROM
        `{Variable.get('BQ_PROJECT_ID')}.{Variable.get('BQ_DATASET_NAME')}.kindle_reviews`
    WHERE
        PARSE_DATE('%m %d, %Y', reviewTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY
        asin
    ORDER BY
        review_count_last_week DESC
    """

    trending_products_task = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_bigquery",
        task_id="trending_products",
        configuration={
            "query": {
                "query": trending_products_query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": Variable.get('BQ_PROJECT_ID'),
                    "datasetId": Variable.get('BQ_DATASET_NAME'),
                    "tableId": "trending_products",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location="US",
    )

    new_products_query = f"""
    SELECT
        asin,
        COUNT(*) AS total_reviews,
        AVG(overall) AS avg_rating
    FROM
        `{Variable.get('BQ_PROJECT_ID')}.{Variable.get('BQ_DATASET_NAME')}.kindle_reviews`
    WHERE
        PARSE_DATE('%m %d, %Y', reviewTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    GROUP BY
        asin
    ORDER BY
        total_reviews DESC
    """

    new_products_task = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_bigquery",
        task_id="new_products",
        configuration={
            "query": {
                "query": new_products_query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": Variable.get('BQ_PROJECT_ID'),
                    "datasetId": Variable.get('BQ_DATASET_NAME'),
                    "tableId": "new_products",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location="US",
    )

    best_products_task >> worst_products_task >> trending_products_task >> new_products_task

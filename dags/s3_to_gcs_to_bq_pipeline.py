from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os
import logging
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.exceptions import AirflowSkipException


AWS_BUCKET_NAME = Variable.get("AWS_BUCKET_NAME")
RAW_FOLDER = Variable.get("RAW_FOLDER", default_var="raw/")
ARCHIVE_FOLDER = Variable.get("ARCHIVE_FOLDER", default_var="archive/")
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET_NAME")
RAW_FILE_NAME = Variable.get("RAW_FILE_NAME")
LOCAL_RAW_PATH = Variable.get("LOCAL_RAW_PATH")
LOCAL_PARQUET_PATH = Variable.get("LOCAL_PARQUET_PATH")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BQ_DATASET_NAME = Variable.get("BQ_DATASET_NAME")
BQ_TABLE_NAME = Variable.get("BQ_TABLE_NAME")
BQ_PROJECT_ID = Variable.get("BQ_PROJECT_ID")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
}

def pull_from_s3(aws_conn_id="aws_default"):
    try:
        if not RAW_FILE_NAME:
            raise ValueError("The 'RAW_FILE_NAME' Airflow variable is not set. Please configure it in the Airflow UI.")

        file_key = f"{RAW_FOLDER}{RAW_FILE_NAME}"
        logging.info(f"Checking for file {file_key} in S3...")
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        if not s3_hook.check_for_key(key=file_key, bucket_name=AWS_BUCKET_NAME):
            logging.warning(f"File {file_key} not found in S3. Skipping task.")
            raise AirflowSkipException(f"File {file_key} not found in S3.")

        logging.info(f"Downloading file {file_key} from S3...")
        s3_hook.get_key(key=file_key, bucket_name=AWS_BUCKET_NAME).download_file(LOCAL_RAW_PATH)
        logging.info(f"File {file_key} downloaded successfully.")
    except Exception as e:
        logging.error(f"Error pulling file: {str(e)}")
        raise


def validate_csv():
    try:
        logging.info("Starting CSV validation...")
        df = pd.read_csv(LOCAL_RAW_PATH)

        expected_columns = [
            "Unnamed: 0", "asin", "helpful", "overall", "reviewText",
            "reviewTime", "reviewerID", "reviewerName", "summary", "unixReviewTime"
        ]
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in CSV: {missing_columns}")

        if not pd.api.types.is_integer_dtype(df["overall"]):
            raise TypeError("Column 'overall' must be of type INTEGER.")

        critical_fields = ["asin", "overall", "reviewTime"]
        for field in critical_fields:
            if df[field].isnull().any():
                raise ValueError(f"Critical column '{field}' contains null values.")

        if not df["overall"].between(1, 5).all():
            raise ValueError("Column 'overall' must contain values between 1 and 5.")

        logging.info("CSV validation passed successfully.")
    except Exception as e:
        logging.error(f"CSV validation failed: {str(e)}")
        raise

def batch_csv_to_parquet():
    try:
        logging.info("Converting CSV to Parquet...")
        df = pd.read_csv(LOCAL_RAW_PATH)
        os.makedirs(LOCAL_PARQUET_PATH, exist_ok=True)
        batch_size = 10000
        for i, batch in enumerate(range(0, len(df), batch_size)):
            batch_df = df.iloc[batch:batch + batch_size]
            batch_file = os.path.join(LOCAL_PARQUET_PATH, f"batch_{i}.parquet")
            batch_df.to_parquet(batch_file, index=False)
            logging.info(f"Batch {i} written to {batch_file}.")
    except Exception as e:
        logging.error(f"Error converting CSV to Parquet: {str(e)}")
        raise

def upload_to_gcs():
    try:
        logging.info("Uploading Parquet files to GCS...")
        files = os.listdir(LOCAL_PARQUET_PATH)
        gcs_hook = GCSHook()

        for file in files:
            if file.endswith('.parquet'):
                local_file_path = os.path.join(LOCAL_PARQUET_PATH, file)
                gcs_file_path = f"processed/{file}"
                for attempt in range(3):
                    try:
                        gcs_hook.upload(bucket_name=GCS_BUCKET_NAME, object_name=gcs_file_path, filename=local_file_path)
                        logging.info(f"File {file} uploaded to GCS successfully.")
                        break
                    except Exception as e:
                        logging.error(f"Attempt {attempt + 1} failed to upload {file}: {str(e)}")
                        if attempt == 2:
                            raise
        logging.info("All Parquet files uploaded to GCS successfully.")
    except Exception as e:
        logging.error(f"Error uploading files to GCS: {str(e)}")
        raise

def load_parquet_to_bigquery():
    return GCSToBigQueryOperator(
        task_id="load_parquet_to_bigquery",
        bucket=GCS_BUCKET_NAME,
        source_objects=["processed/*.parquet"],
        destination_project_dataset_table=f"{BQ_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id="google_cloud_bigquery",
    )

def delete_from_s3():
    try:
        logging.info("Deleting file from S3...")
        s3_hook = S3Hook(aws_conn_id="aws_default")
        file_key = f"{RAW_FOLDER}{RAW_FILE_NAME}"
        s3_hook.delete_objects(bucket=AWS_BUCKET_NAME, keys=[file_key])
        logging.info(f"File {file_key} deleted successfully from S3.")
    except Exception as e:
        logging.error(f"Error deleting file from S3: {str(e)}")
        raise

def clean_local_files():
    try:
        logging.info("Cleaning up local files...")
        if os.path.exists(LOCAL_RAW_PATH):
            os.remove(LOCAL_RAW_PATH)
            logging.info(f"Deleted local file: {LOCAL_RAW_PATH}")

        if os.path.exists(LOCAL_PARQUET_PATH):
            for file in os.listdir(LOCAL_PARQUET_PATH):
                os.remove(os.path.join(LOCAL_PARQUET_PATH, file))
            os.rmdir(LOCAL_PARQUET_PATH)
            logging.info(f"Deleted local directory: {LOCAL_PARQUET_PATH}")
    except Exception as e:
        logging.error(f"Error during local file cleanup: {str(e)}")
        raise

with DAG(
    dag_id="s3_to_gcs_to_bq_pipeline",
    default_args=default_args,
    description="ETL pipeline to process the S3 file and move it to GCS and finally to BigQuery",
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    start = PythonOperator(task_id="start", python_callable=lambda: logging.info("Starting DAG"))
    pull_file_task = PythonOperator(task_id="pull_file_from_s3", python_callable=pull_from_s3, op_kwargs={"aws_conn_id": "aws_default"})
    validate_task = PythonOperator(task_id="validate_csv", python_callable=validate_csv)
    batch_task = PythonOperator(task_id="batch_csv_to_parquet", python_callable=batch_csv_to_parquet)
    upload_task = PythonOperator(task_id="upload_to_gcs", python_callable=upload_to_gcs)
    load_bq_task = load_parquet_to_bigquery()
    delete_s3_task = PythonOperator(task_id="delete_from_s3", python_callable=delete_from_s3)
    clean_task = PythonOperator(task_id="clean_local_files", python_callable=clean_local_files)

    start >> pull_file_task >> validate_task >> batch_task >> upload_task >> load_bq_task >> delete_s3_task >> clean_task


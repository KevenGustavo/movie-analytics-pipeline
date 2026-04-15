from airflow.sdk import dag, task
from pendulum import datetime
import os
from dotenv import load_dotenv

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from scripts.ingest_movie_data import download_data, upload_to_gcs

load_dotenv()

url_dataset = os.getenv("URL_DATASET","")
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH","")
bucket_name = os.getenv("GCS_BUCKET_NAME","")
dest_folder = os.getenv("GCS_DESTINATION_FOLDER","")

with open("/usr/local/airflow/include/sql/create_raw_tables.sql", "r") as f:
        sql_raw_tables = f.read()

with open("/usr/local/airflow/include/sql/create_analytics_tables.sql", "r") as f:
        sql_analytics_tables = f.read()

@dag(
    dag_id="movie_analytics_ingestion",
    schedule="@once",
    start_date=datetime(2026,4,13),
    catchup=False,
    tags=['ingestion', 'movies', 'gcs', 'taskflow'],
)
def movie_ingestion_pipeline():

    @task
    def extract_data_task():
        path_saved = download_data(url_dataset, local_extract_path)
        return path_saved
    
    @task
    def upload_to_gcs_task(source_path: str):
        upload_to_gcs(
            bucket_name=bucket_name,
            source_folder=source_path,
            destination_folder=dest_folder
        )

    create_raw_tables_task = BigQueryInsertJobOperator(
        task_id='create_raw_tables_in_bigquery',
        configuration={
            "query": {
                "query": sql_raw_tables,
                "useLegacySql": False,
            }
        }
    )

    create_analytics_tables_task = BigQueryInsertJobOperator(
        task_id='create_analytics_tables_in_bigquery',
        location='US',
        configuration={
            "query": {
                "query": sql_analytics_tables,
                "useLegacySql": False,
            }
        }
    )

    extracted_path = extract_data_task()
    upload_task = upload_to_gcs_task(source_path=extracted_path)
    
    upload_task >> create_raw_tables_task >> create_analytics_tables_task

movie_ingestion_pipeline()
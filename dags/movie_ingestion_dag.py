from airflow.sdk import dag, task
from pendulum import datetime
import os
from dotenv import load_dotenv

from scripts.ingest_movie_data import download_data, upload_to_gcs

load_dotenv()

url_dataset = os.getenv("URL_DATASET","")
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH","")
bucket_name = os.getenv("GCS_BUCKET_NAME","")
dest_folder = os.getenv("GCS_DESTINATION_FOLDER","")

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
        upload_to_gcs(bucket_name=bucket_name,source_folder=source_path,destination_folder=dest_folder)

    extracted_path = extract_data_task()
    upload_to_gcs_task(extracted_path)

movie_ingestion_pipeline()
import os, requests, zipfile
from google.cloud import storage
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_file(zip_path: str, extract_path: str):
    logging.info(f"Extraindo arquivos...")
    try:
        with zipfile.ZipFile(zip_path, "r") as zip:
            zip.extractall(extract_path)
        logging.info(f"Arquivos extraídos em {extract_path}")
    except Exception as err:
        logging.error(f"Erro durante a extração: {err}")
        raise

def download_data(url: str, extract_path: str):
    zip_path = os.path.join(extract_path, "movielens.zip")

    os.makedirs(extract_path, exist_ok=True)

    logging.info(f"Iniciando o download do link: {url}")

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"Download conluído com sucesso!")

        extract_file(zip_path, extract_path)
        os.remove(zip_path)

        return extract_path

    except Exception as err:
        logging.error(f"Erro durante o download: {err}")
        raise
        
def upload_to_gcs(bucket_name: str, source_folder: str, destination_folder: str):
    logging.info(f"Iniciando o upload para o bucket {bucket_name} no GCS...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        for root, dirs, files in os.walk(source_folder):
            for filename in files:
                if filename.endswith(".csv"):
                    file_path = os.path.join(root, filename)
                    
                    blob_name = f"{destination_folder}/{filename}"
                    blob = bucket.blob(blob_name)

                    if blob.exists():
                        logging.info(f"O arquivo {filename} já existe no GCS. Pulando upload.")
                        continue 

                    logging.info(f"Fazendo upload de {filename} para gs://{bucket_name}/{blob_name}")
                    blob.upload_from_filename(file_path)

        logging.info("Upload para o GCS concluído com sucesso!")
    except Exception as err:
        logging.error(f"Erro durante o upload para o GCS: {err}")
        raise
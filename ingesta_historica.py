import json
import requests
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
import zipfile
import io
import logging

# Configuración básica de logging
logging.basicConfig(level=logging.INFO)

class DownloadAndProcessZip(beam.DoFn):
    def __init__(self, bucket_name, folder_name, extract):
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.extract = extract

    def start_bundle(self):
        self.storage_client = storage.Client()
        logging.info("Storage client initialized.")

    def process(self, element):
        url = element
        logging.info(f"Processing URL: {url}")
        
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad status codes
            zip_name = url.split("/")[-1].replace(".zip", "")
            logging.info(f"Downloaded ZIP: {zip_name}")

            if self.extract:
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    for file_info in z.infolist():
                        with z.open(file_info) as file:
                            # Crear la ruta del blob incluyendo la carpeta con el nombre del ZIP
                            blob_path = f'{self.folder_name}/{zip_name}/{file_info.filename}'
                            blob = self.storage_client.bucket(self.bucket_name).blob(blob_path)
                            blob.upload_from_file(file)
                            logging.info(f"Uploaded file: {blob_path}")
                yield f"Extracted {zip_name}"
            else:
                file_name = url.split("/")[-1]
                blob_path = f'{self.folder_name}/{file_name}'
                blob = self.storage_client.bucket(self.bucket_name).blob(blob_path)
                blob.upload_from_string(response.content)
                logging.info(f"Downloaded and uploaded {file_name} to {blob_path}")
                yield f"Downloaded and uploaded {file_name} to {blob_path}"
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to download or process {url}: {e}")

def run_pipeline(json_url, bucket_name, folder_name, extract):
    try:
        # Descargar el JSON desde la URL
        response = requests.get(json_url)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()
        logging.info("JSON data downloaded successfully.")
        
        # Extraer todas las URLs de los recursos
        urls = [resource['url'] for resource in data['result']['resources']]
        logging.info(f"Extracted {len(urls)} URLs from JSON.")
        
        options = PipelineOptions()

        with beam.Pipeline(options=options) as p:
            (p
             | 'Create URLs' >> beam.Create(urls)
             | 'Download and Process' >> beam.ParDo(DownloadAndProcessZip(bucket_name, folder_name, extract)))
        
        logging.info("Pipeline executed successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download JSON data: {e}")

if __name__ == '__main__':
    json_url = "https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et"
    bucket_name = 'bcrudo_historicosbeam'
    folder_name = 'historicos'
    extract = True  # Cambiar a False si solo se quiere descargar sin extraer
    
    run_pipeline(json_url, bucket_name, folder_name, extract)

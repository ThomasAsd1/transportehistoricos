import json
import requests
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
import logging

# Configuración básica de logging
logging.basicConfig(level=logging.INFO)

class DownloadZip(beam.DoFn):
    def __init__(self, bucket_name, folder_name):
        self.bucket_name = bucket_name
        self.folder_name = folder_name

    def start_bundle(self):
        self.storage_client = storage.Client()
        logging.info("Storage client initialized.")

    def process(self, element):
        url = element
        logging.info(f"Processing URL: {url}")
        
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad status codes
            
            file_name = url.split("/")[-1]
            blob_path = f'{self.folder_name}/{file_name}'
            blob = self.storage_client.bucket(self.bucket_name).blob(blob_path)
            blob.upload_from_string(response.content)
            
            logging.info(f"Downloaded and uploaded {file_name} to {blob_path}")
            yield f"Downloaded and uploaded {file_name} to {blob_path}"
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to download {url}: {e}")

def run_pipeline(json_url, bucket_name, folder_name):
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
             | 'Download Zips' >> beam.ParDo(DownloadZip(bucket_name, folder_name)))
        
        logging.info("Pipeline executed successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download JSON data: {e}")

if __name__ == '__main__':
    json_url = "https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et"
    bucket_name = 'bcrudo_historicosbeam'
    folder_name = 'historicos_zip'
    
    run_pipeline(json_url, bucket_name, folder_name)

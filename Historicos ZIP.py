import json
import requests
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage

class DownloadZip(beam.DoFn):
    def __init__(self, bucket_name, folder_name):
        self.bucket_name = bucket_name
        self.folder_name = folder_name

    def start_bundle(self):
        self.storage_client = storage.Client()

    def process(self, element):
        url = element
        response = requests.get(url)
        if response.status_code == 200:
            file_name = url.split("/")[-1]
            blob_path = f'{self.folder_name}/{file_name}'
            blob = self.storage_client.bucket(self.bucket_name).blob(blob_path)
            blob.upload_from_string(response.content)
            yield f"Downloaded and uploaded {file_name} to {blob_path}"

def run_pipeline(json_url, bucket_name, folder_name):
    # Descargar el JSON desde la URL
    response = requests.get(json_url)
    data = response.json()
    
    # Extraer todas las URLs de los recursos
    urls = [resource['url'] for resource in data['result']['resources']]
    
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        (p
         | 'Create URLs' >> beam.Create(urls)
         | 'Download Zips' >> beam.ParDo(DownloadZip(bucket_name, folder_name)))

if __name__ == '__main__':
    json_url = "https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et"
    bucket_name = 'bcrudo_historicosbeam'
    folder_name = 'historicos_zip'
    
    run_pipeline(json_url, bucket_name, folder_name)

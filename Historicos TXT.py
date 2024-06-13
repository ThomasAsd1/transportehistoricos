import json
import requests
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
import zipfile
import io

class DownloadAndExtractZip(beam.DoFn):
    def __init__(self, bucket_name, folder_name):
        self.bucket_name = bucket_name
        self.folder_name = folder_name

    def start_bundle(self):
        self.storage_client = storage.Client()

    def process(self, element):
        url = element
        response = requests.get(url)
        zip_name = url.split("/")[-1].replace(".zip", "")
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for file_info in z.infolist():
                with z.open(file_info) as file:
                    # Crear la ruta del blob incluyendo la carpeta con el nombre del ZIP
                    blob_path = f'{self.folder_name}/{zip_name}/{file_info.filename}'
                    blob = self.storage_client.bucket(self.bucket_name).blob(blob_path)
                    blob.upload_from_file(file)
        yield f"Extracted {zip_name}"

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
         | 'Download and Extract' >> beam.ParDo(DownloadAndExtractZip(bucket_name, folder_name)))

if __name__ == '__main__':
    json_url = "https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et"
    bucket_name = 'bcrudo_historicosbeam'
    folder_name = 'historicos_txt'
    
    run_pipeline(json_url, bucket_name, folder_name)

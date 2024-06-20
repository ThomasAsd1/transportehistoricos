import json
import requests
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from datetime import datetime
import logging

# Configuración básica de logging
logging.basicConfig(level=logging.INFO)

class DownloadAndUploadJSON(beam.DoFn):
    def __init__(self, bucket_name, folder_name):
        self.bucket_name = bucket_name
        self.folder_name = folder_name

    def start_bundle(self):
        self.storage_client = storage.Client()
        logging.info("Storage client initialized.")

    def process(self, element):
        recorrido = element
        url_reco = f"https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint={recorrido}"
        logging.info(f"Processing URL: {url_reco}")
        
        try:
            response = requests.get(url_reco)
            response.raise_for_status()
            data = response.json()
            archivo_nombre = f"{self.folder_name}{recorrido}.json"
            blob = self.storage_client.bucket(self.bucket_name).blob(archivo_nombre)
            
            # Verificar si el archivo ya existe
            if not blob.exists():
                blob.upload_from_string(json.dumps(data, indent=4), content_type='application/json')
                logging.info(f"Archivo {archivo_nombre} subido correctamente.")
                yield f"Archivo {archivo_nombre} subido correctamente."
            else:
                logging.info(f"Archivo {archivo_nombre} ya existe en el bucket, omitiendo descarga.")
                yield f"Archivo {archivo_nombre} ya existe en el bucket, omitiendo descarga."
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to download or process {url_reco}: {e}")

def run_pipeline(bucket_name):
    try:
        # URL de las APIs
        url_all = "https://www.red.cl/restservice_v2/rest/getservicios/all"
        
        # Descargar la lista de recorridos
        response = requests.get(url_all)
        response.raise_for_status()
        recorridos = response.json()
        logging.info("Recorridos data downloaded successfully.")
        
        # Nombre de la carpeta basada en la fecha de hoy
        fecha_hoy = datetime.now().strftime('%d_%m_%Y')
        folder_name = f"{fecha_hoy}/"

        options = PipelineOptions()

        with beam.Pipeline(options=options) as p:
            (p
            | 'Create Recorridos' >> beam.Create(recorridos)
            | 'Download and Upload JSON' >> beam.ParDo(DownloadAndUploadJSON(bucket_name, folder_name)))
        
        logging.info("Pipeline executed successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download recorridos data: {e}")

if __name__ == '__main__':
    bucket_name = 'bcrudo_diarios'
    run_pipeline(bucket_name)

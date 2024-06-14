import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
import csv
import logging

# Configuración básica de logging
logging.basicConfig(level=logging.INFO)

class ConvertTxtToCsv(beam.DoFn):
    def __init__(self, source_bucket_name, dest_bucket_name):
        self.source_bucket_name = source_bucket_name
        self.dest_bucket_name = dest_bucket_name

    def start_bundle(self):
        self.storage_client = storage.Client()
        logging.info("Storage client initialized.")

    def process(self, element):
        txt_file_path = element
        logging.info(f"Processing file: {txt_file_path}")
        
        try:
            # Elimina la parte de la carpeta 'historicos_txt/' del path original
            filename_without_extension = txt_file_path.replace('historicos_txt/', '').rsplit('.', 1)[0]
            # Construye el path del archivo CSV sin la carpeta 'historicos_txt'
            csv_file_path = f'{filename_without_extension}.csv'
            bucket = self.storage_client.bucket(self.source_bucket_name)
            blob = bucket.blob(txt_file_path)
            content = blob.download_as_text()
            csv_data = [line.split(',') for line in content.splitlines()]
            dest_bucket = self.storage_client.bucket(self.dest_bucket_name)
            dest_blob = dest_bucket.blob(csv_file_path)
            
            with dest_blob.open('w') as output_file:
                writer = csv.writer(output_file)
                writer.writerows(csv_data)
            
            logging.info(f"Processed and uploaded CSV: {csv_file_path}")
            yield f"Processed {txt_file_path}"
        except Exception as e:
            logging.error(f"Error processing {txt_file_path}: {e}")

def run_pipeline(source_bucket_name, source_folder, dest_bucket_name):
    try:
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(source_bucket_name, prefix=source_folder)
        txt_files = [blob.name for blob in blobs if blob.name.endswith('.txt')]
        logging.info(f"Found {len(txt_files)} TXT files to process.")
        
        options = PipelineOptions()

        with beam.Pipeline(options=options) as p:
            (p
             | 'Create File Paths' >> beam.Create(txt_files)
             | 'Convert and Upload' >> beam.ParDo(ConvertTxtToCsv(source_bucket_name, dest_bucket_name)))
        
        logging.info("Pipeline executed successfully.")
    except Exception as e:
        logging.error(f"Failed to run pipeline: {e}")

if __name__ == '__main__':
    source_bucket_name = 'bcrudo_historicosbeam'
    source_folder = 'historicos_txt/'
    dest_bucket_name = 'bcsv_historicosbeam'
    
    run_pipeline(source_bucket_name, source_folder, dest_bucket_name)

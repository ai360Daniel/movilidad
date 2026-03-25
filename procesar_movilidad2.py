import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from datetime import datetime
import pytz
import pyarrow as pa

# 1. Función de Transformación Core
def transform_row(line):
    try:
        # El CSV no tiene cabeceras, dividimos por coma
        parts = line.split(',')
        
        # Validación mínima de longitud de línea (esperamos 18 columnas)
        if len(parts) < 16:
            return None

        # Conversión de Timestamp: Milisegundos Unix -> CDMX
        ts_ms = float(parts[5])
        dt_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=pytz.utc)
        cdmx_tz = pytz.timezone('America/Mexico_City')
        dt_cdmx = dt_utc.astimezone(cdmx_tz)
        
        # Retornamos un diccionario con las columnas seleccionadas
        return {
            "device_id": str(parts[0]),
            "latitude": float(parts[2]),
            "longitude": float(parts[3]),
            "timestamp": dt_cdmx.strftime('%Y-%m-%d %H:%M:%S'),
            "device_os": str(parts[7]),
            "geohash": str(parts[15])
        }
    except Exception:
        # En Big Data, es vital ignorar filas corruptas para no detener el Job
        return None

# 2. Configuración del Pipeline
def run():
    options = PipelineOptions()
    
    # Configuración de GCP
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'movilidad-491219'
    google_cloud_options.region = 'us-central1' # Cambia a tu región de preferencia
    google_cloud_options.job_name = 'csv-to-parquet-movilidad-nov'
    google_cloud_options.staging_location = 'gs://datalake_movilidad/temp/staging'
    google_cloud_options.temp_location = 'gs://datalake_movilidad/temp'
    
    # Ejecutor: DataflowRunner (para la nube) o DirectRunner (para probar local)
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Definir el esquema de Parquet
    parquet_schema = pa.schema([
        ('device_id', pa.string()),
        ('latitude', pa.float64()),
        ('longitude', pa.float64()),
        ('timestamp', pa.string()),
        ('device_os', pa.string()),
        ('geohash', pa.string()),
    ])

    with beam.Pipeline(options=options) as p:
        (
            p 
            | 'Leer Archivos GZ' >> beam.io.ReadFromText('gs://datalake_movilidad/data2/year=2022/month=11/**/*.gz')
            | 'Transformar y Filtrar' >> beam.Map(transform_row)
            | 'Eliminar Nulos' >> beam.Filter(lambda x: x is not None)
            | 'Escribir a Parquet' >> beam.io.parquetio.WriteToParquet(
                'gs://datalake_movilidad/data2_parquet/year=2022/month=11/datos',
                schema=parquet_schema,
                file_name_suffix='.parquet',
                num_shards=0 # Dataflow decidirá el número óptimo de archivos de salida
            )
        )

if __name__ == '__main__':
    run()
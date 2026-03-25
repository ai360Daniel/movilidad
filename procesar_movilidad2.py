import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from datetime import datetime
import pytz
import pyarrow as pa
import logging

# 1. Función de Transformación: Limpieza y Cambio de Hora
def transform_row(line):
    try:
        # El CSV no tiene cabeceras, dividimos por coma
        parts = line.split(',')
        
        # Validación: El dataset original tiene 18 columnas
        if len(parts) < 16:
            return None

        # --- CONVERSIÓN DE TIMESTAMP ---
        # El valor original está en milisegundos (ej. 1667305599000)
        ts_ms = float(parts[5])
        
        # 1. Convertir a objeto datetime en UTC
        dt_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=pytz.utc)
        
        # 2. Convertir a Zona Horaria de CDMX (maneja Horario de Verano automáticamente)
        cdmx_tz = pytz.timezone('America/Mexico_City')
        dt_cdmx = dt_utc.astimezone(cdmx_tz)
        
        # 3. Formatear como string legible
        timestamp_cdmx = dt_cdmx.strftime('%Y-%m-%d %H:%M:%S')

        # Retornamos el diccionario con las columnas seleccionadas
        return {
            "device_id": str(parts[0]),
            "latitude": float(parts[2]),
            "longitude": float(parts[3]),
            "timestamp": timestamp_cdmx,
            "device_os": str(parts[7]),
            "geohash": str(parts[15])
        }
    except Exception as e:
        # En procesos masivos, ignoramos filas con errores para no detener el Job
        logging.warning(f"Error procesando línea: {e}")
        return None

# 2. Configuración del Pipeline de Dataflow
def run():
    # Definir opciones del pipeline
    options = PipelineOptions()
    
    # Configuración específica de Google Cloud
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'movilidad-491219'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.job_name = 'movilidad-gz-to-parquet-cdmx'
    
    # Rutas temporales para que Dataflow "piense"
    google_cloud_options.staging_location = 'gs://datalake_movilidad/temp/staging'
    google_cloud_options.temp_location = 'gs://datalake_movilidad/temp'
    
    # Runner: DataflowRunner para ejecutar en la nube
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Esquema de salida para el archivo Parquet (PyArrow)
    parquet_schema = pa.schema([
        ('device_id', pa.string()),
        ('latitude', pa.float64()),
        ('longitude', pa.float64()),
        ('timestamp', pa.string()),
        ('device_os', pa.string()),
        ('geohash', pa.string()),
    ])

    # Definición de la ruta de entrada y salida
    input_path = 'gs://datalake_movilidad/data2/year=2022/month=11/**/*.gz'
    output_path = 'gs://datalake_movilidad/data2_parquet/year=2022/month=11/datos'

    with beam.Pipeline(options=options) as p:
        (
            p 
            | 'Leer Archivos GZ' >> beam.io.ReadFromText(input_path)
            | 'Transformar a CDMX' >> beam.Map(transform_row)
            | 'Filtrar Nulos' >> beam.Filter(lambda x: x is not None)
            | 'Escribir a Parquet' >> beam.io.parquetio.WriteToParquet(
                output_path,
                schema=parquet_schema,
                file_name_suffix='.parquet',
                num_shards=0 # Permite que Dataflow decida cuántos archivos generar según el volumen
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

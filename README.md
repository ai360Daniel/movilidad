# movilidad

# Pipeline de Procesamiento de Datos de Movilidad (GZ a Parquet)

Este repositorio contiene un pipeline de datos robusto desarrollado en **Apache Beam** y ejecutado en **Google Cloud Dataflow**. El objetivo es procesar un Data Lake masivo (~3,000 archivos comprimidos con 9 billones de registros) para estandarizar, filtrar y optimizar los datos para análisis avanzado.

## 🚀 Descripción del Proceso

El pipeline realiza las siguientes tareas de ingeniería de datos:

1.  **Ingesta Masiva**: Lee archivos `.csv.gz` directamente desde Google Cloud Storage de forma paralela.
2.  **Filtrado de Atributos**: Reduce el dataset original de 18 columnas a las 6 esenciales (`device_id`, `latitude`, `longitude`, `timestamp`, `device_os`, `geohash`), optimizando el almacenamiento y la velocidad de consulta.
3.  **Transformación de Zona Horaria**: 
    * Convierte el `timestamp` original (Unix Epoch en milisegundos UTC).
    * Ajusta automáticamente a la **Hora de la Ciudad de México (CDMX)** manejando cambios estacionales mediante la librería `pytz`.
4.  **Optimización de Formato**: Convierte los datos crudos a **Apache Parquet**, un formato columnar altamente eficiente para herramientas de Big Data como BigQuery.

## 🛠️ Stack Tecnológico

* **Lenguaje:** Python 3.x
* **Framework de Procesamiento:** Apache Beam
* **Ejecutor (Runner):** Google Cloud Dataflow (Serverless & Auto-scaling)
* **Formato de Salida:** Apache Parquet (Compresión Snappy)
* **Librerías Clave:** `pyarrow`, `pytz`, `pandas`

## 📂 Estructura de Datos

* **Entrada:** `gs://datalake_movilidad/data2/year=2022/month=11/**/*.gz`
* **Salida:** `gs://datalake_movilidad/data2_parquet/year=2022/month=11/`

## 💻 Ejecución

Para lanzar el job en la infraestructura de Google Cloud, asegúrate de tener configurado tu entorno y ejecuta:




```bash
git clone https://github.com/ai360Daniel/movilidad
cd movilidad

# 1. Obtener el número de tu proyecto automáticamente
PROJ_NUM=$(gcloud projects list --filter="project_id:movilidad-491219" --format='value(project_number)')

# 2. Darle permiso total de escritura al agente de servicio en el bucket
gcloud projects add-iam-policy-binding movilidad-491219 \
    --member="serviceAccount:service-$PROJ_NUM@dataflow-service-producer-prod.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Crear un bucket nuevo en la misma región
gsutil mb -l us-central1 gs://temp-movilidad-pipeline

# Lanzar el job usando este nuevo bucket para staging y temp
python3 procesar_movilidad2.py \
    --project movilidad-491219 \
    --region us-central1 \
    --runner DataflowRunner \
    --temp_location gs://temp-movilidad-pipeline/temp \
    --staging_location gs://temp-movilidad-pipeline/staging \
    --requirements_file requirements.txt \
    --save_main_session



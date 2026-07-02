import os
import ast
import re
import warnings
import pandas as pd
import numpy as np
import time
import json
import textwrap
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.oauth2 import service_account
from langchain_google_vertexai import ChatVertexAI, HarmBlockThreshold, HarmCategory
from langchain_core.messages import HumanMessage
import pendulum
warnings.filterwarnings("ignore")

def trigger_alert_on_failure(context):
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    error_message = str(exception) if exception else "Error desconocido"

    # truncar a 200 caracteres
    error_message = textwrap.shorten(error_message, width=200, placeholder="...")

    TriggerDagRunOperator(
        task_id=f"trigger_alert_{task_instance.task_id}",
        trigger_dag_id="bi-send-teams-alerts",
        conf={
            "webhook": "webhook_IA",
            "error_message": error_message,
            "dag_id": task_instance.dag_id,
            "task_id": task_instance.task_id,
            "execution_date": str(task_instance.execution_date),
        },
        trigger_rule="all_failed",  # asegura que se dispare al fallar
    ).execute(context)

default_args = {
    "owner": "BI - Squad Analytics",
    "depends_on_past": False,
    "email": ["omar.vergara@realplaza.com.pe"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "description": "DAG - Comentarios NPS",
    "provide_context": True,
    "on_failure_callback": trigger_alert_on_failure
}

dag = DAG(
    'bi-nps-one-experience',
    default_args=default_args,
    description='Ejecutar todos los lunes a las 07:00 hora Lima',
    schedule_interval='0 7 * * 1',  
    #start_date=days_ago(1),
    start_date=datetime(2025,10,13, tzinfo=pendulum.timezone("America/Lima")),
    catchup=False,
    tags=["experiencia", "ia", "bi"]
)

#Variables airflow
try:
    dag_settings = Variable.get("clasificacion_nps_settings", deserialize_json=True)
    bucket_name = dag_settings["clasificacion_nps_bucket"] 
    var_bucket_cred = dag_settings["service_account_bucket"]
    nombre_json_gcp = dag_settings["service_account_name"]
    project_id = dag_settings["project_id"]
    dataset_id = dag_settings["dataset_project"]
    location = dag_settings["location_project"]
    model_genia = dag_settings["model_genia"]
    temperature_api = dag_settings["temperature"] # temperatura de la inferencia
    top_p_api = dag_settings["top_p"] # top_p de la inferencia
    seed_api = dag_settings["seed_api"] # semilla de la inferencia
    token_api = dag_settings.get("max_output_tokens", 65536) # cantidad maxima de tokens de salida (default: 65536 máximo para Gemini 2.5 Flash)
    name_procedure = dag_settings["name_procedure"]
    table_clasification = dag_settings["name_table_clasification"]
    table_recomendation = dag_settings["name_table_recomendation"]
    table_coment = dag_settings["name_table_coment"]
    table_log_error = dag_settings["name_log_error"]
    table_log_error_recom = dag_settings["name_log_error_recom"]
    start_date_Var = dag_settings["start_date"]
    end_date_Var = dag_settings["end_date"]
    umbral_carga_clasificacion = dag_settings["umbral_carga_clasificacion"]
except KeyError as e:
    raise ValueError(f"Variable de Airflow 'clasificacion_nps_settings {e}")    

# Esquema de la tabla
schema = [
    bigquery.SchemaField("Codmes", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("Mall", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Categoria", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Recomendacion", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("fecha_ejecucion", "STRING", mode="REQUIRED"),
]

schema_clasi = [
    bigquery.SchemaField("Centro_comercial", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Comentario", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Categoria", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Subcategoria", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Subsubcategoria", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Sentimiento", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Puntuacion_sentimiento", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Recomendacion", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Codmes", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("Codmes_fecha", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("Fecha", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("Formulario", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("Recomendaria_Real_Plaza", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("Anio", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("Fuente", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Mes", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("Fecha_ejecucion", "TIMESTAMP", mode="REQUIRED")
]

schema_recom = [
    bigquery.SchemaField("Codmes", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("Centro_comercial", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Categoria", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Recomendacion", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("fecha_ejecucion", "TIMESTAMP", mode="REQUIRED")
]

# Safety settings para LangChain Vertex AI
safety_settings = {
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
}

def get_llm_model(credentials=None, response_schema=None):
    """
    Factory function para crear el modelo LLM usando LangChain.
    Facilita el cambio de proveedor en el futuro (OpenAI, Anthropic, etc.)

    Args:
        credentials: Credenciales de GCP (opcional, usa default si no se proveen)
        response_schema: Esquema JSON para forzar salida estructurada (opcional)

    Returns:
        ChatVertexAI: Instancia del modelo configurado
    """
    llm_kwargs = {
        "model": model_genia,
        "project": project_id,
        "location": location,
        "temperature": temperature_api,
        "max_output_tokens": token_api,
        "top_p": top_p_api,
        "safety_settings": safety_settings,
        "credentials": credentials,
    }

    if response_schema:
        llm_kwargs["response_mime_type"] = "application/json"
        llm_kwargs["response_schema"] = response_schema

    try:
        return ChatVertexAI(**llm_kwargs)
    except TypeError:
        llm_kwargs.pop("response_mime_type", None)
        llm_kwargs.pop("response_schema", None)
        return ChatVertexAI(**llm_kwargs)

def invoke_llm(llm, prompt: str) -> str:
    """
    Invoca el modelo LLM con el prompt dado.
    Abstracción que facilita el cambio de proveedor.

    Args:
        llm: Instancia del modelo LLM
        prompt: Texto del prompt

    Returns:
        str: Respuesta del modelo
    """
    message = HumanMessage(content=prompt)
    response = llm.invoke([message])
    return response.content

schema_log_error = [
    bigquery.SchemaField("mall", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("comentario", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("error", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("fecha_ejecucion", "DATE", mode="REQUIRED")
]

schema_log_error_recom = [
    bigquery.SchemaField("mall", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("categoria", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("error", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("fecha_ejecucion", "DATE", mode="REQUIRED")
]

# Identificación del proyecto, dataset y tabla
job_config = bigquery.LoadJobConfig(schema=schema)

#Variables generales
procedure_name = f"{project_id}.{dataset_id}.{name_procedure}"
# Prompt
pregunta = """Necesito que categorices el texto de content_nps unicamente mediante las categorias y subcategorias de la sección Categorias_NPS y SubCategorias_NPS respectivamente. Considerar que como resultado puede haber más de una categoría y subcategoría para cada content_nps. Si la categoría no tiene subcategorías o no se puede definir, usar Subcategoria = "Otros".
También, mostrar el sentimiento negativo, positivo y/o neutro por categoría. Además, mostrar el nivel de intensidad del sentimiento según escala del 1 al 5 ;en donde, a mayor número, sentimiento más positivo para cada categoría clasificada, los comentarios negativos deben tener intensidad de sentimiento estricto menor que los neutros, y los neutros deber ser estricto menor que los positivos.
considerar que los positivos solo podrán tener intensidad 4 o 5, neutro será intensidad 3, y negativo intensidad 1 y 2.
Adicional, generar una recomendación para brindar solución mediante alguna acción al comentario, de un máximo de 25 palabras.
La salida debe ser JSON puro, sin etiquetas Markdown, sin saltos de línea y sin formato adicional;además,sin texto adicional antes ni después. Sin encabezados, ni la palabra 'json', ni llaves []. Y si hubiera mas de 1 respuesta, separarlo por coma. De la siguiente forma:
[
    {
        "Categoria": "valor",
        "Subcategoria": "valor",
        "Sentimiento": "valor",
        "Intensidad": "valor",
        "Recomendación": "valor"
    }
]

Ejemplo de salida correcta:
[{"Categoria": "Baños", "Subcategoria": "Olor", "Sentimiento": "Negativo", "Intensidad": 2, "Recomendación": "Realizar una inspección inmediata..."},
{"Categoria": "Estacionamiento", "Subcategoria": "Accesibilidad al estacionamiento", "Sentimiento": "Negativo", "Intensidad": 1, "Recomendación": "Realizar una inspección inmediata..."}]
"""
agrup_recom = """Necesito generar una única recomendación concisa (máximo 30 palabras) a partir de la siguiente lista de comentarios.  Esta recomendación debe ser útil para mejorar la experiencia del cliente. No menciones el nombre del centro comercial ni la ciudad.
La salida debe ser **EXACTAMENTE** en este formato JSON, sin saltos de línea ni texto adicional:
{"Recomendación": "Aquí va la recomendación concisa"}
"""

# Prompt para batch processing de múltiples comentarios
pregunta_batch = """Clasifica CADA comentario (identificado por su índice numérico) usando las categorías proporcionadas.

INSTRUCCIONES:
- Para CADA comentario, devolver su clasificación con el índice correspondiente
- Cada comentario puede tener múltiples categorías (devolver múltiples objetos en "clasificaciones")
- Si la categoría no tiene subcategorías o no se puede definir, usar Subcategoria = "Otros"
- Sentimiento: "Negativo" (intensidad 1-2), "Neutro" (intensidad 3), "Positivo" (intensidad 4-5)
- Recomendación: máximo 25 palabras por clasificación

FORMATO JSON (sin markdown, sin texto adicional antes o después):
[{"comentario_index": 0, "clasificaciones": [{"Categoria": "valor", "Subcategoria": "valor", "Sentimiento": "valor", "Intensidad": N, "Recomendacion": "valor"}]}, {"comentario_index": 1, "clasificaciones": [...]}]
"""

CLASIFICACION_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "Categoria": {"type": "string"},
            "Subcategoria": {"type": "string"},
            "Sentimiento": {"type": "string"},
            "Intensidad": {"type": "integer"},
            "Recomendación": {"type": "string"}
        },
        "required": [
            "Categoria",
            "Subcategoria",
            "Sentimiento",
            "Intensidad",
            "Recomendación"
        ]
    }
}

RECOMENDACION_SCHEMA = {
    "type": "object",
    "properties": {
        "Recomendación": {"type": "string"}
    },
    "required": ["Recomendación"]
}

# Schema para batch processing de múltiples comentarios
CLASIFICACION_BATCH_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "comentario_index": {"type": "integer"},
            "clasificaciones": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "Categoria": {"type": "string"},
                        "Subcategoria": {"type": "string"},
                        "Sentimiento": {"type": "string"},
                        "Intensidad": {"type": "integer"},
                        "Recomendacion": {"type": "string"}
                    },
                    "required": ["Categoria", "Subcategoria",
                                "Sentimiento", "Intensidad", "Recomendacion"]
                }
            }
        },
        "required": ["comentario_index", "clasificaciones"]
    }
}

#Definición de categorías a clasificar
Categorias_NPS = """
{
  "Marketing": "Comentarios positivos y/o negativos relacionados con las ofertas, lanzamientos, canjes, premios y otras acciones promocionales dirigidas a los clientes. Incluye percepciones sobre acciones del centro comercial como campañas, eventos, shows, concursos o activaciones. También abarca la gestión, organización, difusión, puntualidad, experiencia del público y aforo.",
  "Baños": "Comentarios positivos y/o negativos relacionados con los baños del centro comercial. Este comentario puede ser etiquetado dentro de la categoría Baños.",
  "Áreas comunes": "Comentarios positivos y/o negativos relacionados con las áreas comunes del centro comercial. Este comentario puede ser etiquetado dentro de la categoría Áreas comunes.",
  "Servicio al cliente": "Comentarios positivos y/o negativos relacionados con la atención del personal y los servicios brindados por Real Plaza. Este comentario puede ser etiquetado dentro de la categoría Servicio al cliente.",
  "Locatario/comercial": "Comentarios positivos y/o negativos relacionados con locales/tiendas, la oferta comercial y la incorporación de nuevas tiendas o servicios en el centro comercial.",
  "Estacionamiento": "Comentarios positivos y/o negativos que involucran los servicios, señalización y accesibilidad del estacionamiento o parking del centro comercial.",
  "Patio de comidas": "Comentarios positivos y/o negativos relacionados con el patio de comidas del centro comercial. Este comentario puede ser etiquetado dentro de la categoría Patio de comidas.",
  "Otros": "Comentarios que no se pueden definir."
}

"""
SubCategorias_NPS = """ Según la categoria inferida, en donde se consideró Categorias_NPS, se generan las subcategorias:
{
  "Marketing": {
                 "Actividades y Eventos": "Comentarios positivos y/o negativos relacionados con las actividades y eventos organizados o promovidos por Real Plaza o el centro comercial. Incluye comentarios sobre la experiencia general, entretenimiento, artistas invitados, participación y organización. Ejemplo: 'El evento de Navidad estuvo lindo, pero empezó tarde y había demasiada gente.'",
                 "Canjes y premios": "Comentarios positivos y/o negativos vinculados a campañas de canje, sorteos, premios y otros incentivos promocionales ofrecidos por Real Plaza o el centro comercial. Ejemplo: 'Participé en el sorteo pero nunca publicaron a los ganadores.'",
                 "Entretenimiento": "Comentarios positivos y/o negativos relacionados con las opciones de entretenimiento del centro comercial, como shows, actividades para niños, juegos o experiencias interactivas. Ejemplo: 'Me gustó la zona de juegos para niños, pero deberían ampliarla.'",
                 "Otros": "Comentarios que no se pueden definir"
                 },
  "Baños": {
              "Limpieza": "Comentarios positivos y/o negativos relacionados con el estado de limpieza de los baños. Incluye comentarios sobre higiene general, pisos, lavamanos, inodoros, tachos de basura y mantenimiento del orden. Ejemplo: 'Los baños estaban sucios y no había papel.'",
              "Atención al cliente": "Comentarios positivos y/o negativos vinculados a la atención brindada por el personal de limpieza o encargado de los baños. Incluye amabilidad, disposición y respuesta ante solicitudes. Ejemplo: 'La señora de limpieza fue amable y repuso el papel rápido.'",
              "Infraestructura": "Comentarios positivos y/o negativos relacionados con la infraestructura y el estado físico de los baños. Incluye puertas, cerraduras, grifos, iluminación, ventilación y estado de los servicios higiénicos. Ejemplo: 'El caño estaba malogrado y no salía agua.'",
              "Olor": "Comentarios positivos y/o negativos que hacen referencia a los olores percibidos en los baños. Incluye malos olores, falta de aromatización o ventilación. Ejemplo: 'Había un olor terrible, parecía que no limpiaban.'",
              "Recursos y/o suministros": "Comentarios positivos y/o negativos relacionados con la disponibilidad y reposición de insumos como papel higiénico, jabón, toallas, etc. Ejemplo: 'No había jabón ni papel para secarse.'",
              "Otros": "Comentarios que no se pueden definir"
              },
  "Áreas comunes": {
                    "Limpieza": "Comentarios positivos y/o negativos que involucran la limpieza en las áreas comunes",
                    "Zona de descanso": "Comentarios positivos y/o negativos sobre la cantidad, ubicación y comodidad de las zonas de descanso; el funcionamiento de las zonas de carga; el tipo y volumen de la música",
                    "Ascensores/Escaleras/Aire Acondicionado": "Comentarios positivos y/o negativos sobre la cantidad y ubicación de los ascensores, escaleras y aire acondicionado en el centro comercial",
                    "Infraestructura": "Comentarios positivos y/o negativos que involucran características del ambiente, ambientación, decoración y mobiliario de las áreas comunes en el centro comercial",
                    "Seguridad": "Comentarios positivos y/o negativos respecto a la seguridad o inseguridad, en donde se vulnera o puede vulnerar al cliente o familiares, incluyendo mendicidad y/o comercio ambulatorio en las áreas comunes de Real Plaza o el centro comercial",
                    "Otros": "Comentarios que no se pueden definir"
                    },
  "Servicio al cliente": {
                          "Atención al cliente": "Comentarios positivos y/o negativos respecto a la atención del personal. Involucra al personal de experiencia y la atención del personal de un local de Real Plaza",
                          "Servicios": "Comentarios positivos y/o negativos sobre los servicios de Real Plaza. Involucra el préstamo de sillas y/o coches"
                          },
  "Locatario/comercial": {
                          "Oferta, Promociones y Precios": "Comentarios positivos y/o negativos que involucran las ofertas, promociones y precios del centro comercial",
                          "Variedad de tiendas": "Comentarios positivos y/o negativos que involucran la variedad de tiendas del centro comercial",
                          "Cine": "Comentarios positivos y/o negativos que involucran el cine del centro comercial"
                          },
  "Estacionamiento": {
                      "JapiBici": "Comentarios positivos y/o negativos que involucran la zona de bicicletas y scooters en el centro comercial",
                      "Señalización galerias": "Comentarios positivos y/o negativos que involucran la señalización y facilidad para ubicar el ingreso a galería y/o ascensores",
                      "Señalización vehicular": "Comentarios positivos y/o negativos que involucran la señalización y facilidad para ubicar su vehículo y la salida del estacionamiento",
                      "Atención al cliente": "Comentarios positivos y/o negativos respecto a la atención del personal. Involucra al personal del estacionamiento de Real Plaza o el centro comercial",
                      "Accesibilidad al estacionamiento": "Comentarios positivos y/o negativos que involucran la accesibilidad, tiempo para ingresar y salir del estacionamiento",
                      "Pago de estacionamiento": "Comentarios positivos y/o negativos que involucran el pago del estacionamiento. Incluye métodos de pago, accesibilidad y señalización a zona de pago",
                      "Seguridad": "Comentarios positivos y/o negativos respecto a la seguridad o inseguridad, en donde se vulnera o puede vulnerar al cliente o familiares, incluyendo mendicidad y/o comercio ambulatorio en Real Plaza o el centro comercial",
                      "Otros": "Comentarios que no se pueden definir"
                      },
  "Patio de comidas": {
                       "Limpieza": "Comentarios positivos y/o negativos relacionados con la limpieza del patio de comidas. Incluye mesas, sillas, pisos, tachos de basura y el orden general. Ejemplo: 'Las mesas estaban sucias y los tachos llenos, no había nadie limpiando.'",
                       "Atención al cliente": "Comentarios positivos y/o negativos vinculados a la atención brindada dentro del patio de comidas. Incluye trato del personal, apoyo en mesas, orden y rapidez en atención general. Ejemplo: 'El personal fue muy amable y nos ayudó a encontrar una mesa libre.'",
                       "Infraestructura": "Comentarios positivos y/o negativos relacionados con la infraestructura del patio de comidas. Incluye espacio, cantidad y estado de mesas/sillas, ventilación, iluminación y comodidad general. Ejemplo: 'Muchas sillas están rotas y el área se siente muy congestionada.'",
                       "Seguridad": "Comentarios positivos y/o negativos asociados a la percepción o situaciones de seguridad/inseguridad en el patio de comidas. Ejemplo: 'Había demasiada gente y no se veía personal de seguridad controlando.'",
                       "Otros": "Comentarios que no se pueden definir"
                       },
  "Otros": {
            "Otros": "No tiene subcategorías."
            }
}

"""
# Listas de categorías
category_list = ["Marketing","Baños","Áreas comunes","Servicio al cliente","Locatario/comercial","Estacionamiento","Patio de comidas","Otros"]
subcategory_list = ["Actividades y Eventos","Canjes y premios","Entretenimiento","Otros","Limpieza","Atención al cliente","Infraestructura",
                    "Olor","Recursos y/o suministros","Zona de descanso","Ascensores/Escaleras/Aire Acondicionado","Seguridad","Servicios",
                    "Oferta, Promociones y Precios","Variedad de tiendas","Cine","JapiBici","Señalización galerias","Señalización vehicular",
                    "Accesibilidad al estacionamiento","Pago de estacionamiento"]

#paametros de variables
intervalo_espera = 15  # Reducido de 45 para mejorar rendimiento
fecha_actual = datetime.now()
fecha = fecha_actual.date()
timestamp_ejecucion = pd.Timestamp.now(tz='UTC')  # Timestamp completo con timezone para BigQuery
dia_eje = str(fecha.day).zfill(2)
anio_eje = str(fecha.year)
mes_eje = str(fecha.month).zfill(2)
fecha_ejecucion=anio_eje+mes_eje+dia_eje

if start_date_Var == "1990-01-01" or end_date_Var == "1990-01-01":
    # Calcular fechas automáticamente
    dia_hoy = fecha_actual.date()
    lunes_anterior = dia_hoy - timedelta(days=7)
    
    anio = str(lunes_anterior.year)
    mes = str(lunes_anterior.month).zfill(2)
    codmes = anio + mes
    
    anio_fin = str(dia_hoy.year)
    mes_fin = str(dia_hoy.month).zfill(2)
    codmes_fin = anio_fin + mes_fin
    
    start_date = str(lunes_anterior)
    end_date = str(dia_hoy)
else:
    # Usar fechas proporcionadas por variables
    start_date = start_date_Var
    end_date = end_date_Var
    
    # Obtener codmes desde la fecha de inicio
    fecha_inicio_dt = datetime.strptime(start_date, "%Y-%m-%d")
    fecha_fin_dt = datetime.strptime(end_date, "%Y-%m-%d")
    anio = str(fecha_inicio_dt.year)
    mes = str(fecha_inicio_dt.month).zfill(2)
    codmes = anio + mes 
    
    anio_fin = str(fecha_fin_dt.year)
    mes_fin = str(fecha_fin_dt.month).zfill(2)
    codmes_fin = anio_fin + mes_fin    

def inicio_proceso():
    print("Iniciando el proceso de Clasificación y Recomendación de comentarios NPS...")
       
def getCredentialGCP():
        try:
            AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
            file_download_cs = AIRFLOW_HOME + '/'
            print(f"Directorio AIRFLOW_HOME: {AIRFLOW_HOME}")
            print(f"Ruta de descarga para las credenciales: {file_download_cs}")
            
            print("Iniciando conexion con Google Cloud Storage...")
            storage_client = storage.Client()
            print(f"Obteniendo el bucket de credenciales: {var_bucket_cred}")
            bucket_cred = storage_client.get_bucket(var_bucket_cred)

            print(f"Buscando el archivo de credenciales: {nombre_json_gcp}")
            blob_json = bucket_cred.blob(nombre_json_gcp)

            print(f"Descargando el archivo de credenciales desde el bucket: {var_bucket_cred}...")
            blob_json.download_to_filename(file_download_cs + nombre_json_gcp)
            print(f"Archivo de credenciales descargado exitosamente en: {file_download_cs + nombre_json_gcp}")
            
            #SET - Cuenta de servicio particular
            credential_path = file_download_cs + nombre_json_gcp
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
            print(f"Credenciales de GCP configuradas correctamente con la ruta: {credential_path}")

        except Exception as e:
                print(f"Error al obtener las credenciales GCP: {str(e)}") 
                
def actualizar_sp_bq(**kwargs):
    
    getCredentialGCP()  # Asegurar autenticación en GCP        
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path:
        credentials = service_account.Credentials.from_service_account_file(cred_path)
        print("Cuenta de servicio:", credentials.service_account_email)
    else:
        print("No se encontró GOOGLE_APPLICATION_CREDENTIALS en el entorno")

    client = bigquery.Client(
        credentials=credentials,
        project=project_id
        )    
    
    args = [
        bigquery.ScalarQueryParameter('start_date', 'DATE', start_date),
        bigquery.ScalarQueryParameter('end_date', 'DATE', end_date),
    ]
    # Llama al procedimiento almacenado
    print(start_date)
    print(end_date)
    try:
        print(f"CALL `{procedure_name}`({start_date}, {end_date})")    
        result = client.query(
            f"CALL `{procedure_name}`(@start_date, @end_date)", 
            job_config=bigquery.QueryJobConfig(query_parameters=args)
        )
        print("Procedimiento almacenado ejecutado con éxito.")
        
    except Exception as e:
        print(f"Error al ejecutar el procedimiento almacenado: {e}")
        return False

    return True

def carga_datos_encuestas(**kwargs):
    
    getCredentialGCP()  # Asegurar autenticación en GCP        
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path:
        credentials = service_account.Credentials.from_service_account_file(cred_path)
        print("Cuenta de servicio:", credentials.service_account_email)
    else:
        print("No se encontró GOOGLE_APPLICATION_CREDENTIALS en el entorno")

    client = bigquery.Client(
        credentials=credentials,
        project=project_id
        )
    
    print("Fecha de ejecucion: ",fecha)
    print("Fecha inicio: ",start_date)
    print("Fecha fin: ",end_date)
    # Validar que las variables necesarias estén definidas
    if not all([start_date, end_date, procedure_name]):
        raise ValueError("start_date, end_date, procedure_name no están definidos correctamente.")
    # Eliminado time.sleep inicial - el stored procedure ya se ejecutó en la tarea anterior
    sql_query = f"""
    SELECT
        Mall,
        Fecha,
        Formulario,
        UPPER(Pregunta) AS Pregunta,
        Respuesta
    FROM `pe-realplaza-gcp.realplaza_sw_surveys.VistaEncuestasConsolidado`
    WHERE
        PuntoContacto = "ENCUESTAS NPS QR"
        AND DATE(Fecha) >= DATE('{start_date}')
        AND DATE(Fecha) < DATE('{end_date}')
        AND Respuesta IS NOT NULL
    """
    try:
        base_encuestas_df = pd.read_gbq(
            sql_query,
            project_id=project_id,
            credentials=credentials,
            dialect='standard'
        )
    except Exception as e:
        print(f"Error al ejecutar la consulta SQL: {e}")
        return False

    if base_encuestas_df.empty:
        print("No se encontraron datos para el rango de fechas proporcionado.")
        return False

    umbral_carga = base_encuestas_df.shape[0] / 12
    print("Umbral de carga: ",umbral_carga)
   
    sql_query = f"""
    SELECT *
    FROM `{project_id}.{dataset_id}.{table_coment}`
    WHERE DATE(Fecha)>= '{start_date}' AND DATE(Fecha)< '{end_date}'
    """    
    intentos = 0
    
    while intentos < 11:
        time.sleep(30)  # Reducido de 60 para mejorar rendimiento
        result = pd.read_gbq(
            sql_query,
            project_id=project_id,
            credentials=credentials,
            dialect='standard'
        )

        if not result.empty:
            # Extrae el valor de la primera fila y primera columna
            registros_actuales = result.shape[0]
            print("Registros actuales en BQ: ", registros_actuales)

            # Compara el número de registros con el valor esperado
            if registros_actuales >= umbral_carga:
                print(result["Fecha"])
                #result["Fecha"] = result["Fecha"].dt.date
                kwargs['ti'].xcom_push(key='result', value=result.to_json(orient='split'))
                print("La tabla tiene suficientes registros.")
                return True
            else:
                print("No se alcanzó el umbral de registros esperados.")
        else:
            print("No se obtuvieron registros de la consulta.") 

        intentos += 1
        print(f"La tabla de registros tiene {registros_actuales} registros. Esperando {intervalo_espera} segundos antes de volver a verificar.")
        time.sleep(intervalo_espera)    
        
    # Mostrar las primeras filas del DataFrame
    print("Inicio de datos: ", start_date)
    print("Fin de los datos: ", end_date)
    print(result.shape) 
    result["Fecha"] = result["Fecha"].dt.date
    
    # Si se agotaron todos los intentos, empujar el resultado final a XCom (si existe)
    if not result.empty:
        kwargs['ti'].xcom_push(key='result', value=result.to_json(orient='split'))
    else:
        print("No se pudo importar los archivos desde GCP")
    
    return result   

def clean_data(df):
    # Crear copia del DataFrame y limpiar la columna 'Comentario'
    df_bkp = df.copy()
    df_bkp['Comentario'] = df_bkp['Comentario'].str.upper().str.strip()

    # Filtrar filas con comentarios vacíos o valores NaN en 'Comentario' y 'Recomendaria_Real_Plaza'
    df_bkp = df_bkp.loc[df_bkp['Comentario'].ne('') & df_bkp['Comentario'].notna() & df_bkp['Recomendaria_Real_Plaza'].notna()]

    # Seleccionar la tercera columna dinámica
    third_column_name = df_bkp.columns[5]

    # Obtener la fila con el valor máximo en la tercera columna para cada 'Comentario'
    df_bkp = df_bkp.loc[df_bkp.groupby('Comentario')[third_column_name].idxmax()].reset_index(drop=True)

    # Extraer comentarios únicos con más de 3 palabras
    comentarios = df.iloc[:, 4].dropna().str.strip().str.upper().drop_duplicates()
    comentarios = comentarios[comentarios.str.count(r'\S+') > 2]

    return df_bkp, comentarios

def post_procesamiento(df, df_2, centro_comercial):
    # Ordenar el DataFrame por comentario e intensidad
    df = df.sort_values(by=['Comentario', 'Intensidad'], ascending=[True, False]).reset_index(drop=True)
    print("1: ",df.head())
    # Crear la columna 'orden' y filtrar en una sola operación
    df = df.assign(orden=df.groupby('Comentario').cumcount() + 1)
    df = df[(df['orden'] < 3) & (df['Sentimiento'].isin(['Negativo', 'Positivo', 'Neutro']))].drop(columns=['orden'])

    # Renombrar clasificaciones fuera de la definición a "Otros"
    df['Categoria'] = df['Categoria'].where(df['Categoria'].isin(category_list), "Otros")
    df['Subcategoria'] = df['Subcategoria'].where(df['Subcategoria'].isin(subcategory_list), "Otros")

    # Aplicar reglas condicionales con `mask`
    df['Subcategoria'] = df['Subcategoria'].mask(df['Categoria'] == "Otros", "Otros")
    
    # Merge con `df_2`
    df = df.merge(df_2, on="Comentario", how="inner")

    # Convertir fecha y extraer año/mes en una sola asignación
    df['Fecha'] = pd.to_datetime(df['Fecha'])
    print("2: ",df.head())
    df = df.assign(
        Anio=df['Fecha'].dt.year.astype(str),
        Mes=df['Fecha'].dt.month.astype(str).str.zfill(2)
    )

    # Crear Codmes y renombrar columnas
    df['Codmes'] = df['Anio'] + df['Mes']
    df.rename(columns={"Intensidad": "Intensidad del Sentimiento"}, inplace=True)
    print("3: ",df.head())
    df["Mall"] = centro_comercial
    col_order = ['Mall'] + [col for col in df.columns if col != 'Mall']
    df = df[col_order]
    print("4: ",df.head())

    return df

def verificar_datos(data_json, key_name):
    if data_json is None:
        print(f"ERROR: El JSON para '{key_name}' está vacío o es None. No se puede cargar.")
        return pd.DataFrame()  # Devuelve un DataFrame vacío en caso de error
    else:
        return pd.read_json(data_json, orient='split')
    
def limpieza_json(output_text):
    # **LIMPIEZA DE LA CADENA JSON (MÁS ROBUSTA)**
    output_text = re.sub(r'^\s*json', '', output_text, flags=re.IGNORECASE).strip()  # Elimina "json" al inicio
    output_text = re.sub(r'```(?:json)?', '', output_text).strip()  # Elimina ``` y ```json        
    output_text = re.sub(r'```json|```', '', output_text).strip()
    
    # Limpiar posibles espacios adicionales alrededor de las llaves y comillas
    output_text = re.sub(r'^\s*{\s*', '{', output_text)  # Eliminar espacios al inicio de la cadena
    output_text = re.sub(r'\s*}\s*$', '}', output_text)  # Eliminar espacios al final de la cadena
    output_text = output_text.replace('\n', '').replace('\r', '')  # Eliminar Saltos de línea y Retornos de Carro    
    return output_text

def is_json_truncated(text):
    """
    Detecta si un JSON está truncado (incompleto).
    Retorna True si parece estar truncado.
    """
    if not text:
        return True
    text = text.strip()
    # Contar corchetes y llaves
    open_brackets = text.count('[') - text.count(']')
    open_braces = text.count('{') - text.count('}')
    # Si hay más abiertos que cerrados, está truncado
    if open_brackets > 0 or open_braces > 0:
        return True
    # Verificar que termine con ] o } (ignorando espacios)
    if text and text[-1] not in [']', '}']:
        return True
    return False

def parse_json_like(response_text):
    if isinstance(response_text, (dict, list)):
        return response_text, None

    if response_text is None:
        raise ValueError("Respuesta vacía del modelo.")

    markdown_text = str(response_text).strip()
    if not markdown_text:
        raise ValueError("Respuesta vacía del modelo.")

    if not markdown_text.startswith("[") and not markdown_text.startswith("{"):
        markdown_text = "[" + markdown_text.replace("}\n{", "},{") + "]"

    markdown_text = limpieza_json(markdown_text)

    # Detectar si el JSON está truncado antes de intentar parsear
    if is_json_truncated(markdown_text):
        raise ValueError(f"Respuesta truncada del modelo (JSON incompleto)")

    try:
        return json.loads(markdown_text), markdown_text
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(markdown_text), markdown_text
        except (ValueError, SyntaxError) as exc:
            raise ValueError(f"Salida no es JSON válido: {exc}") from exc

def normalize_classification_output(parsed_output):
    if isinstance(parsed_output, dict):
        return [parsed_output]

    if isinstance(parsed_output, list) and all(isinstance(item, list) for item in parsed_output):
        raise ValueError("Formato inesperado: lista de listas detectada.")

    if not isinstance(parsed_output, list):
        raise ValueError("Formato inesperado: se esperaba lista de objetos.")

    return parsed_output

def invoke_llm_with_retry(llm, prompt: str, max_retries: int = 3, retry_delay: int = 2) -> str:
    """
    Invoca el modelo LLM con reintentos automáticos cuando la respuesta está truncada.

    Args:
        llm: Instancia del modelo LLM
        prompt: Texto del prompt
        max_retries: Número máximo de reintentos
        retry_delay: Segundos de espera entre reintentos

    Returns:
        str: Respuesta del modelo

    Raises:
        ValueError: Si después de todos los reintentos la respuesta sigue truncada
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            response_text = invoke_llm(llm, prompt)

            # Verificar si la respuesta está truncada
            cleaned = limpieza_json(str(response_text).strip())
            if is_json_truncated(cleaned):
                raise ValueError(f"Respuesta truncada del modelo (intento {attempt + 1}/{max_retries})")

            return response_text

        except ValueError as e:
            last_error = e
            if "truncada" in str(e).lower() and attempt < max_retries - 1:
                print(f"Reintentando... ({attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                continue
            raise

    raise ValueError(f"Respuesta truncada después de {max_retries} intentos: {last_error}")

def process_comments_batch(llm, comentarios_batch: list, categorias_context: str) -> dict:
    """
    Procesa un batch de comentarios en una sola llamada al LLM.

    Args:
        llm: Instancia del modelo LLM configurado con CLASIFICACION_BATCH_SCHEMA
        comentarios_batch: Lista de comentarios a procesar
        categorias_context: Contexto de categorías pre-construido

    Returns:
        dict: Mapeo de índice -> lista de clasificaciones
    """
    comentarios_formateados = "\n".join([
        f"[{i}]: {comentario}"
        for i, comentario in enumerate(comentarios_batch)
    ])

    prompt_completo = f"""{pregunta_batch}

{categorias_context}

COMENTARIOS A CLASIFICAR:
{comentarios_formateados}
"""

    response_text = invoke_llm_with_retry(llm, prompt_completo, max_retries=3, retry_delay=2)
    parsed_output, _ = parse_json_like(response_text)

    # Convertir a diccionario indexado
    result = {}
    if isinstance(parsed_output, list):
        for item in parsed_output:
            idx = item.get("comentario_index")
            clasificaciones = item.get("clasificaciones", [])
            if idx is not None:
                result[idx] = clasificaciones

    return result

def asignacion_clasificacion_ia(**kwargs):
    getCredentialGCP()  # Asegurar autenticación en GCP

    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path:
        credentials = service_account.Credentials.from_service_account_file(cred_path)
        print("Cuenta de servicio:", credentials.service_account_email)
    else:
        print("No se encontró GOOGLE_APPLICATION_CREDENTIALS en el entorno")

    storage_client = storage.Client(
        credentials=credentials
    )
    bucket = storage_client.bucket(bucket_name)
    client = bigquery.Client(
        credentials=credentials,
        project=project_id
        )

    result_json = kwargs['ti'].xcom_pull(key='result')
    encuestas_df = pd.DataFrame()
    result_df = verificar_datos(result_json, 'df_experiencia')

    # Inicializar modelo LLM usando LangChain con schema para batch
    llm_batch = get_llm_model(credentials=credentials, response_schema=CLASIFICACION_BATCH_SCHEMA)

    result_df["Fecha"] = pd.to_datetime(result_df["Fecha"], unit="ms").dt.strftime("%Y-%m-%d %H:%M:%S")
    result_df["fecha_ejecucion"] = timestamp_ejecucion

    # Pre-construir contexto de categorías UNA sola vez (optimización)
    categorias_context = (
        "Categorias_NPS: " + Categorias_NPS + "\n" +
        "SubCategorias_NPS: " + SubCategorias_NPS
    )

    # Configuración de batch processing
    BATCH_SIZE = 15  # Comentarios por llamada al LLM

    # Usar listas para acumulación eficiente (en lugar de concat iterativo)
    all_errors = []
    all_encuestas = []

    for i in result_df['Mall'].drop_duplicates():
        j = i.replace(" ", "_")
        print(j)
        base = result_df[result_df["Mall"] == i]
        base['Recomendaria_Real_Plaza'] = pd.to_numeric(base['Recomendaria_Real_Plaza'], errors='coerce')
        print("Inicio pre procesamiento")
        print("Registros originales :", base.shape[0])
        base_bkp, comentarios = clean_data(base)
        print("Cantidad de comentarios que pasan filtro de duplicidad y # letras: ", len(comentarios))
        print("Inicio de LangChain + Gemini AI (Batch Processing)")

        # Convertir a lista para indexación
        comentarios_list = comentarios.tolist()
        print(f"Cantidad de comentarios: {len(comentarios_list)}, Batches: {(len(comentarios_list) + BATCH_SIZE - 1) // BATCH_SIZE}")

        # Acumular clasificaciones para este mall
        mall_clasificaciones = []

        # Procesar en batches
        for batch_start in range(0, len(comentarios_list), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(comentarios_list))
            batch = comentarios_list[batch_start:batch_end]
            batch_num = (batch_start // BATCH_SIZE) + 1
            total_batches = (len(comentarios_list) + BATCH_SIZE - 1) // BATCH_SIZE
            print(f"Procesando batch {batch_num}/{total_batches} ({len(batch)} comentarios)")

            try:
                # Una sola llamada para todo el batch
                batch_results = process_comments_batch(llm_batch, batch, categorias_context)

                # Procesar resultados del batch
                for local_idx, clasificaciones in batch_results.items():
                    if local_idx < len(batch):
                        comentario = batch[local_idx]
                        for clasif in clasificaciones:
                            mall_clasificaciones.append({
                                'Comentario': comentario,
                                'Categoria': clasif.get('Categoria'),
                                'Subcategoria': clasif.get('Subcategoria'),
                                'Sentimiento': clasif.get('Sentimiento'),
                                'Intensidad': clasif.get('Intensidad'),
                                'Recomendación': clasif.get('Recomendacion', clasif.get('Recomendación', ''))
                            })

            except Exception as e:
                print(f"Error en batch {batch_num}: {e}")
                # Registrar error para cada comentario del batch fallido
                for comentario in batch:
                    all_errors.append({
                        'mall': i,
                        'comentario': comentario,
                        'error': str(e),
                        'fecha_ejecucion': fecha
                    })

        # Crear DataFrame para este mall si hay clasificaciones
        if mall_clasificaciones:
            result_clasificacion_df = pd.DataFrame(mall_clasificaciones)
            print("Request successful.")
            print("Inicio post procesamiento")
            result_clasificacion_df = post_procesamiento(result_clasificacion_df, base_bkp, i)
            result_clasificacion_df['Fecha'] = result_clasificacion_df['Fecha'].dt.tz_localize(None)
            print("Cantidad de registros obtenidos: ", result_clasificacion_df.shape[0])
            all_encuestas.append(result_clasificacion_df)
            print("Clasificación exitosa")
        else:
            print("Error en la ejecución - sin clasificaciones")

    # Concat final eficiente (una sola vez)
    if all_encuestas:
        encuestas_df = pd.concat(all_encuestas, ignore_index=True)

    print("campos: ",encuestas_df.info())
    print("Dataframe antes de BQ: ",encuestas_df.head())

    # Cargar errores a BigQuery si hay
    if all_errors:
        errores_df = pd.DataFrame(all_errors)
        errores_df['fecha_ejecucion'] = pd.to_datetime(errores_df['fecha_ejecucion']).dt.date
        table_ref = f"{project_id}.{dataset_id}.{table_log_error}"
        job_config = bigquery.LoadJobConfig(schema=schema_log_error)
        client.load_table_from_dataframe(
            dataframe=errores_df,
            destination=table_ref,
            job_config=job_config,
            project=project_id,
        ).result()
    
    if not encuestas_df.empty:
            kwargs['ti'].xcom_push(key='result_clasi', value=encuestas_df.to_json(orient='split'))
    else:
        print("No se guardó las clasificación con éxito")   
        raise AirflowFailException("El DataFrame `encuestas_df` está vacío, deteniendo la ejecución.")
    return "Ok"

def ingesta_clasificacion_bq(**kwargs):
    getCredentialGCP()  # Asegurar autenticación en GCP        
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path:
        credentials = service_account.Credentials.from_service_account_file(cred_path)
        print("Cuenta de servicio:", credentials.service_account_email)
    else:
        print("No se encontró GOOGLE_APPLICATION_CREDENTIALS en el entorno")
       
    # Obtener datos
    result_clasi_json = kwargs['ti'].xcom_pull(key='result_clasi') 
    encuestas_df = verificar_datos(result_clasi_json, 'df_experiencia') 
    
    # Renombrar columnas
    encuestas_df.rename(columns={
        "Intensidad del Sentimiento": "Puntuacion_sentimiento",
        "Recomendación": "Recomendacion",
        "Mall": "Centro_comercial",
        "fecha_ejecucion": "Fecha_ejecucion"
    }, inplace=True)

    if "Subsubcategoria" not in encuestas_df.columns:
        encuestas_df["Subsubcategoria"] = pd.NA

    # Optimización de conversiones
    encuestas_df["Puntuacion_sentimiento"] = encuestas_df["Puntuacion_sentimiento"].astype(str)
    encuestas_df.loc[
        (encuestas_df["Puntuacion_sentimiento"] == "3") & (encuestas_df["Sentimiento"] == "Positivo"),
        "Puntuacion_sentimiento"
    ] = "4"
    
    encuestas_df = encuestas_df.astype({
        "Codmes": "int",
        "Anio": "int",
        "Mes": "int",
        "Recomendaria_Real_Plaza": "int"
    })

    # Generar Codmes_fecha
    encuestas_df["Codmes_fecha"] = (encuestas_df["Anio"].astype(str) + 
                                    encuestas_df["Mes"].astype(str).str.zfill(2)).astype(int)
    
    encuestas_df["Fuente"] = "RP NPS"

    # Conversión de fechas
    encuestas_df["Fecha_ejecucion"] = pd.to_datetime(encuestas_df["Fecha_ejecucion"], unit="ms", utc=True)
    encuestas_df["Fecha"] = pd.to_datetime(encuestas_df["Fecha"], unit="ms", utc=True)

    # Reordenar columnas
    encuestas_df.insert(9, "Codmes_fecha", encuestas_df.pop("Codmes_fecha"))
    encuestas_df.insert(16, "Fecha_ejecucion", encuestas_df.pop("Fecha_ejecucion"))
    
    # Filtrar datos con recomendación no nula
    categorias_nps_df = encuestas_df[encuestas_df["Recomendacion"].notna()].copy()
    
    # Conversión eficiente de tipos
    categorias_nps_df = categorias_nps_df.astype({
        "Puntuacion_sentimiento": "string",
        "Codmes_fecha": "int64"
    })
    categorias_nps_df["Fecha_ejecucion"] = pd.to_datetime(categorias_nps_df["Fecha_ejecucion"])
    # Normalizar precision de timestamps para BigQuery (microseconds)
    for col in ["Fecha", "Fecha_ejecucion"]:
        categorias_nps_df[col] = pd.to_datetime(categorias_nps_df[col], utc=True).dt.floor("us")

    # Eliminar columna 'Otro'/'Otros' si existe
    categorias_nps_df.drop(columns=["Otro", "Otros"], errors="ignore", inplace=True)
    
    # Campos que deben exportarse (según el esquema de BigQuery)
    columnas_finales = [
        "Centro_comercial",
        "Comentario",
        "Categoria",
        "Subcategoria",
        "Subsubcategoria",
        "Sentimiento",
        "Puntuacion_sentimiento",
        "Recomendacion",
        "Codmes",
        "Codmes_fecha",
        "Fecha",
        "Formulario",
        "Recomendaria_Real_Plaza",
        "Anio",
        "Mes",
        "Fuente",
        "Fecha_ejecucion"
    ]

    # Filtrar solo las columnas necesarias y en orden exacto
    categorias_nps_df = categorias_nps_df[columnas_finales].copy()

    # Asegurar tipos correctos
    categorias_nps_df = categorias_nps_df.astype({
        "Centro_comercial": "string",
        "Comentario": "string",
        "Categoria": "string",
        "Subcategoria": "string",
        "Subsubcategoria": "string",
        "Sentimiento": "string",
        "Puntuacion_sentimiento": "string",
        "Recomendacion": "string",
        "Fuente": "string",
        "Codmes": "int64",
        "Codmes_fecha": "int64",
        "Formulario": "int64",
        "Recomendaria_Real_Plaza": "int64",
        "Anio": "int64",
        "Mes": "int64",
    })

    # Convertir fechas
    #categorias_nps_df["Fecha"] = pd.to_datetime(categorias_nps_df["Fecha"])
    #categorias_nps_df["Fecha_ejecucion"] = pd.to_datetime(categorias_nps_df["Fecha_ejecucion"])
        

    # Configuración de BigQuery
    table_ref = f"{project_id}.{dataset_id}.{table_clasification}"
    job_config = bigquery.LoadJobConfig(schema=schema_clasi)
    client = bigquery.Client(
        credentials=credentials,
        project=project_id
        )     

    # Eliminar registros con Codmes duplicado antes de la inserción
    print("Fecha inicio: ",start_date)
    print("Fecha fin: ",end_date)
    
    query = f"""
    DELETE FROM `{project_id}.{dataset_id}.{table_clasification}`
    WHERE date(Fecha) >= '{start_date}' AND date(Fecha) < '{end_date}'
    """
    client.query(query).result()
    print(f"Se eliminaron los datos con date(Fecha) >= {start_date} AND date(Fecha) <= {end_date} de la tabla {table_clasification}")    
    print("Tipos de columnas:")
    print(categorias_nps_df.dtypes)

    # Cargar datos en BigQuery
    client.load_table_from_dataframe(
        dataframe=categorias_nps_df,
        destination=table_ref,
        job_config=job_config,
        project=project_id,
    ).result()

    print("Los datos se cargaron correctamente en BigQuery.")    
    return True

def carga_clasificacion_IA(**kwargs):

    getCredentialGCP()  # Asegurar autenticación en GCP        
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path:
        credentials = service_account.Credentials.from_service_account_file(cred_path)
        print("Cuenta de servicio:", credentials.service_account_email)
    else:
        print("No se encontró GOOGLE_APPLICATION_CREDENTIALS en el entorno")
        
    client = bigquery.Client(
        credentials=credentials,
        project=project_id
        ) 

    if not all([start_date, end_date, procedure_name]):
        raise ValueError("start_date, end_date, procedure_name no están definidos correctamente.")

    sql_query = f"""
    SELECT *
    FROM `{project_id}.{dataset_id}.{table_clasification}`
    WHERE Codmes >= {codmes} AND Codmes <= {codmes_fin}
    """    
    umbral_carga = umbral_carga_clasificacion
    print("Umbral de carga: ",umbral_carga)
      
    intentos = 0
    while intentos < 11:
        time.sleep(30)  # Reducido de 60 para mejorar rendimiento
        result = pd.read_gbq(
            sql_query,
            project_id=project_id,
            credentials=credentials,
            dialect='standard'
        )

        if not result.empty:
            # Extrae el valor de la primera fila y primera columna
            registros_actuales = result.shape[0]
            print("Registros actuales en BQ: ", registros_actuales)

            # Compara el número de registros con el valor esperado
            if registros_actuales >= umbral_carga:
                #result["Fecha"] = result["Fecha"].dt.date
                kwargs['ti'].xcom_push(key='result_clasirecom', value=result.to_json(orient='split'))
                print("La tabla tiene suficientes registros.")
                return True
            else:
                print("No se alcanzó el umbral de registros esperados.")
        else:
            print("No se obtuvieron registros de la consulta.") 

        intentos += 1
        print(f"La tabla de registros tiene {registros_actuales} registros. Esperando {intervalo_espera} segundos antes de volver a verificar.")
        time.sleep(intervalo_espera)    
    
    # Si se agotaron todos los intentos, empujar el resultado final a XCom (si existe)
    if not result.empty:
        kwargs['ti'].xcom_push(key='result_clasirecom', value=result.to_json(orient='split'))
    else:
        print("No se pudo importar los archivos desde GCP")
    
    return result   

def asignacion_recomendacion_ia(**kwargs):

    getCredentialGCP()
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path:
        credentials = service_account.Credentials.from_service_account_file(cred_path)
        print("Cuenta de servicio:", credentials.service_account_email)
    else:
        print("No se encontró GOOGLE_APPLICATION_CREDENTIALS en el entorno")

    storage_client = storage.Client(
        credentials=credentials
    )
    bucket = storage_client.bucket(bucket_name)
    client = bigquery.Client(
        credentials=credentials,
        project=project_id
        )

    # Inicializar modelo LLM usando LangChain
    llm = get_llm_model(credentials=credentials, response_schema=RECOMENDACION_SCHEMA)

    result_recom_json = kwargs['ti'].xcom_pull(key='result_clasirecom')
    result_recom_df = verificar_datos(result_recom_json, 'df_experiencia')
    result_recom_df["Fecha"] = pd.to_datetime(result_recom_df["Fecha"], unit="ms")
    recomendaciones_agrupadas = result_recom_df.groupby(['Codmes','Centro_comercial','Categoria'])['Comentario'].apply(list).reset_index()
    centros_categorias = recomendaciones_agrupadas[['Codmes','Centro_comercial', 'Categoria']].drop_duplicates()

    encuestas_recom_df = pd.DataFrame(columns=['Codmes', 'Mall', 'Categoria', 'Recomendación', 'fecha_ejecucion'])
    encuestas_recom_df['Codmes'] = encuestas_recom_df['Codmes'].astype('int64')

    errores_df = pd.DataFrame({
        'mall': pd.Series(dtype='str'),
        'categoria': pd.Series(dtype='str'),
        'error': pd.Series(dtype='str'),
        'fecha_ejecucion': pd.Series(dtype='datetime64[ns]')
    })

    for _, row in centros_categorias.iterrows():
        centro_comercial = row['Centro_comercial']
        categoria = row['Categoria']
        codmes = row['Codmes']
        print(codmes, " - Categoría ", categoria, " - del mall: ", centro_comercial)

        df_filtrado = recomendaciones_agrupadas[
            (recomendaciones_agrupadas['Centro_comercial'] == centro_comercial) &
            (recomendaciones_agrupadas['Categoria'] == categoria) &
            (recomendaciones_agrupadas['Codmes'] == codmes)
        ]
        recom_list = df_filtrado['Comentario'].tolist()

        try:
            prompt_completo = agrup_recom + "\n" + "recom_list: " + str(recom_list)

            # Usar LangChain para invocar el modelo con reintentos automáticos
            response_text = invoke_llm_with_retry(llm, prompt_completo, max_retries=3, retry_delay=2)

            recomendacion = ""
            cleaned_text = None
            try:
                parsed_output, cleaned_text = parse_json_like(response_text)
                if isinstance(parsed_output, list):
                    parsed_output = parsed_output[0] if parsed_output else {}
                if not isinstance(parsed_output, dict):
                    raise ValueError("Formato inesperado: se esperaba objeto.")
                recomendacion = parsed_output.get("Recomendación") or parsed_output.get("Recomendacion", "")

                data = {
                    "Codmes": [codmes],
                    "Mall": [centro_comercial],
                    "Categoria": [categoria],
                    "Recomendación": [recomendacion],
                    "fecha_ejecucion": [timestamp_ejecucion]
                }

                data_df = pd.DataFrame(data)
                encuestas_recom_df = pd.concat([encuestas_recom_df, data_df], ignore_index=True)

            except ValueError as e:
                print(f"Error al convertir a DataFrame: {e}")
                if cleaned_text:
                    print(f"Texto JSON problemático: {cleaned_text}")
                else:
                    print(f"Texto JSON problemático: {response_text}")
                error_df = {
                    'mall': [centro_comercial],
                    'categoria': [categoria],
                    'error': [str(e)],
                    'fecha_ejecucion':[fecha]
                }
                error_df = pd.DataFrame(error_df)
                errores_df = pd.concat([errores_df,error_df], axis=0, ignore_index=True)
                mensaje = "Request failed."

        except Exception as e:
            print(f"Error en la generación con LangChain para {centro_comercial}, {categoria}, {codmes}: {e}")
            error_df = {
                'mall': [centro_comercial],
                'categoria': [categoria],
                'error': [str(e)],
                'fecha_ejecucion':[fecha]
            }
            error_df = pd.DataFrame(error_df)
            mensaje = "Request failed."

    if not encuestas_recom_df.empty:
        mensaje = "Request successful."

    print(mensaje)

    if not errores_df.empty:
        errores_df['fecha_ejecucion'] = pd.to_datetime(errores_df['fecha_ejecucion']).dt.date
        table_ref = f"{project_id}.{dataset_id}.{table_log_error_recom}"
        job_config = bigquery.LoadJobConfig(schema=schema_log_error_recom)
        client.load_table_from_dataframe(
            dataframe=errores_df,
            destination=table_ref,
            job_config=job_config,
            project=project_id,
        ).result()

    if not encuestas_recom_df.empty:
        kwargs['ti'].xcom_push(key='result_recom_ia', value=encuestas_recom_df.to_json(orient='split'))
    else:
        print("No se guardó la recomendación con éxito")
        raise AirflowFailException("El DataFrame `encuestas_recom_df` está vacío, deteniendo la ejecución.")
    return encuestas_recom_df       

def ingesta_recomendacion_bq(**kwargs):
    
    getCredentialGCP()
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path:
        credentials = service_account.Credentials.from_service_account_file(cred_path)
        print("Cuenta de servicio:", credentials.service_account_email)
    else:
        print("No se encontró GOOGLE_APPLICATION_CREDENTIALS en el entorno")   
        
    result_recom_json = kwargs['ti'].xcom_pull(key='result_recom_ia') 
    encuestas_df = verificar_datos(result_recom_json, 'df_experiencia') 
    encuestas_df.rename(columns={"Recomendación":"Recomendacion"}, inplace=True) 
    if pd.api.types.is_numeric_dtype(encuestas_df['fecha_ejecucion']):
        encuestas_df['fecha_ejecucion'] = pd.to_datetime(encuestas_df['fecha_ejecucion'], unit='ms', utc=True)
    else:
        encuestas_df['fecha_ejecucion'] = pd.to_datetime(encuestas_df['fecha_ejecucion'], utc=True)
    # Normalizar precision de timestamp para BigQuery (microseconds)
    encuestas_df['fecha_ejecucion'] = encuestas_df['fecha_ejecucion'].dt.floor("us")
    encuestas_df.rename(columns={"Mall":"Centro_comercial"}, inplace=True)
    
    table_ref = f"{project_id}.{dataset_id}.{table_recomendation}"
    job_config = bigquery.LoadJobConfig(schema=schema_recom)
    client = bigquery.Client(
        credentials=credentials,
        project=project_id
        )
    
    query = f"""
    DELETE FROM `{project_id}.{dataset_id}.{table_recomendation}`
    WHERE Codmes >= {codmes} AND Codmes <= {codmes_fin}
    """

    # Ejecutar la consulta
    query_job = client.query(query)
    query_job.result()  # Esperar a que termine
    print(f"Se eliminaron los datos con Codmes = {codmes} y {codmes_fin} en {table_recomendation}")    

    # Carga el DataFrame en BigQuery
    client.load_table_from_dataframe(
        dataframe=encuestas_df,
        destination=table_ref,
        job_config=job_config,
        project=project_id,
    ).result() 
    print("Los datos se cargaron correctamente en BigQuery.")    
    return True            
                    
def fin_proceso():
    print("Proceso de Clasificación y Recomendación de comentarios NPS finalizado.")

task_ini = PythonOperator(
    task_id="inicio_proceso",
    python_callable=inicio_proceso,
    dag=dag,
)  

task_actualizar_sp_bq = PythonOperator(
    task_id="actualizar_sp_bq",
    python_callable=actualizar_sp_bq,
    provide_context=True,
)  

task_cargar_encuestas = PythonOperator(
    task_id="carga_datos_encuestas",
    python_callable=carga_datos_encuestas,
    provide_context=True,
)  

task_clasificacion = PythonOperator(
    task_id="asignacion_clasificacion_ia",
    python_callable=asignacion_clasificacion_ia,
    provide_context=True,
)

task_ingesta_clasificacion = PythonOperator(
    task_id="ingesta_clasificacion_bq",
    python_callable=ingesta_clasificacion_bq,
    provide_context=True,
)

task_carga_clasificacion_IA = PythonOperator(
    task_id="carga_clasificacion_IA",
    python_callable=carga_clasificacion_IA,
    provide_context=True,
)

task_recomendacion = PythonOperator(
    task_id="asignacion_recomendacion_ia",
    python_callable=asignacion_recomendacion_ia,
    provide_context=True,
)

task_ingesta_recomendacion = PythonOperator(
    task_id="ingesta_recomendacion_bq",
    python_callable=ingesta_recomendacion_bq,
    provide_context=True,
)

task_fin = PythonOperator(
    task_id="fin_proceso",
    python_callable=fin_proceso,
    dag=dag,
)

task_ini>>task_actualizar_sp_bq>>task_cargar_encuestas>>task_clasificacion>>task_ingesta_clasificacion>>task_carga_clasificacion_IA>>task_recomendacion>>task_ingesta_recomendacion>>task_fin

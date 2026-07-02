import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

import pandas_gbq
from google.auth.exceptions import GoogleAuthError

KEY_PATH = r"C:\Users\eparedes\Documents\SPSA\d-sfh-un-pvea-28891d32fdc7.json"
PROJECT_ID = "d-sfh-un-pvea"

def read_query(query):
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials )

    try:
        query_job = client.query(query)
        df = query_job.to_dataframe()
        return df

    except Exception as e:
        print(f"Error en la conexión o ejecución de la consulta: {e}")
        raise

def read_query_sdk(query):
    
    # El cliente busca automáticamente las credenciales locales de gcloud SDK (ADC)
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query_job = client.query(query)
        df = query_job.to_dataframe()
        return df

    except Exception as e:
        print(f"Error en la conexión o ejecución de la consulta: {e}")
        raise

def write_table(df, tabla_destino, modo="append"):
    if modo.lower() not in ["append", "replace"]:
        raise ValueError("modo debe ser 'append' o 'replace'")
    
    if_exists = "replace" if modo.lower() == "replace" else "append"
    
    try:
        pandas_gbq.to_gbq(
            dataframe=df,
            destination_table=tabla_destino,
            project_id=PROJECT_ID,
            if_exists=if_exists,
            credentials=service_account.Credentials.from_service_account_file(KEY_PATH),
            progress_bar=False
        )
        print(f"Se cargaron {len(df):,} registros en {tabla_destino}")
        
    except Exception as e:
        print(f"Error al cargar la tabla en BigQuery: {e}")
        raise
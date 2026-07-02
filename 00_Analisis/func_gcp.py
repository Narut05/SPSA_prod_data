import pandas as pd
from google.cloud import bigquery


def read_query(query):
    project_id = "spsa-operaciones-seg-sd"
    client = bigquery.Client(project=project_id)
       
    try:
        query_job = client.query(query)
       
        # Descarga vectorizada optimizada con pyarrow hacia el DataFrame
        df = query_job.to_dataframe()
        return df
       
    except Exception as e:
        print(f"Error en la conexión o ejecución de la consulta: {e}")
        raise
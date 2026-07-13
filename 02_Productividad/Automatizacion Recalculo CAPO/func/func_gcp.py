import pandas as pd
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

def read_query(sql_file):
    project_id = "spsa-operaciones-seg-sd"
    client = bigquery.Client(project=project_id)

    with open(sql_file, "r", encoding="utf-8") as f:
        query = f.read()

    query_job = client.query(query)
    return query_job.to_dataframe()


def write_dataframe(
    df,
    project_id,
    dataset_id,
    table_id,
    mode="append",
    credential_env="GCP_DEV_SPSA"
):
    credential_path = os.getenv(credential_env)

    if credential_path is None:
        raise ValueError(
            f"No existe la variable de entorno '{credential_env}'."
        )

    credentials = service_account.Credentials.from_service_account_file(
        credential_path
    )

    client = bigquery.Client(
        project=project_id,
        credentials=credentials
    )

    destination = f"{project_id}.{dataset_id}.{table_id}"

    mode = mode.lower()

    if mode == "append":
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif mode == "replace":
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        raise ValueError("mode debe ser 'append' o 'replace'")

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
    )

    job = client.load_table_from_dataframe(
        df,
        destination,
        job_config=job_config
    )

    job.result()

    print(f"Se cargaron {len(df):,} filas en {destination} ({mode})")
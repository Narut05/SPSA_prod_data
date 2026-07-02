import pandas as pd
from google.cloud import bigquery
import win32com.client
import re
import subprocess

def get_powerbi_port():
    task_output = subprocess.check_output(
        'tasklist /FI "IMAGENAME eq msmdsrv.exe" /FO CSV',
        shell=True
    ).decode('utf-8', errors='ignore')

    pids = re.findall(r'"msmdsrv\.exe","(\d+)"', task_output)

    if not pids:
        raise Exception("No hay instancia de Power BI abierta")

    pid = pids[-1]

    netstat_output = subprocess.check_output(
        'netstat -ano -p tcp',
        shell=True
    ).decode('utf-8', errors='ignore')

    match = re.search(
        rf'127\.0\.0\.1:(\d+)\s+.*LISTENING\s+{pid}',
        netstat_output
    )

    if not match:
        raise Exception("No se encontró puerto")

    return match.group(1)



def get_powerbi_port():
    task_output = subprocess.check_output(
        'tasklist /FI "IMAGENAME eq msmdsrv.exe" /FO CSV',
        shell=True
    ).decode('utf-8', errors='ignore')

    pids = re.findall(r'"msmdsrv\.exe","(\d+)"', task_output)

    if not pids:
        raise Exception("No hay instancia de Power BI abierta")

    pid = pids[-1]

    netstat_output = subprocess.check_output(
        'netstat -ano -p tcp',
        shell=True
    ).decode('utf-8', errors='ignore')

    match = re.search(
        rf'127\.0\.0\.1:(\d+)\s+.*LISTENING\s+{pid}',
        netstat_output
    )

    if not match:
        raise Exception("No se encontró puerto")



def ejecutar_dax(dax):

    port = get_powerbi_port()

    conn_str = (
        f"Provider=MSOLAP;"
        f"Data Source=localhost:{port};"
    )

    conn = win32com.client.Dispatch("ADODB.Connection")
    conn.Open(conn_str)

    cmd = win32com.client.Dispatch("ADODB.Command")
    cmd.ActiveConnection = conn
    cmd.CommandTimeout = 0
    cmd.CommandText = dax

    rs = cmd.Execute()[0]

    columns = [rs.Fields.Item(i).Name for i in range(rs.Fields.Count)]

    rows = []

    while not rs.EOF:

        row = []

        for i in range(rs.Fields.Count):

            valor = rs.Fields.Item(i).Value

            if hasattr(valor, "strftime"):
                valor = valor.strftime("%Y-%m-%d %H:%M:%S")

            row.append(valor)

        rows.append(row)

        rs.MoveNext()

    rs.Close()
    conn.Close()

    return pd.DataFrame(rows, columns=columns)
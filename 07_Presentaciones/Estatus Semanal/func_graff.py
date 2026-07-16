import pandas as pd
import plotly.graph_objects as go

def Historico_Comparativo(
    df,
    anio1,
    anio2,
    cumplimiento,
    titulo,
    archivo_html,
    col_mes="Mes",
    ymin=None,
    ymax=None,
    y2min=-0.60,
    y2max=0.30
):

    meses = {
        "01": "ENE", "02": "FEB", "03": "MAR", "04": "ABR",
        "05": "MAY", "06": "JUN", "07": "JUL", "08": "AGO",
        "09": "SEP", "10": "OCT", "11": "NOV", "12": "DIC"
    }

    dff = df.copy()
    dff[col_mes] = dff[col_mes].astype(str).str.zfill(2).map(meses)
    orden = list(meses.values())
    dff = dff.set_index(col_mes).reindex(orden).reset_index()

    def formatear_valor(x):
        if pd.isna(x):
            return ""
        if abs(x) < 100:
            return f"{x:,.1f}"
        return f"{x:,.0f}"

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=dff[col_mes],
            y=dff[anio1],
            mode="lines+markers",
            name=str(anio1),
            line=dict(color="#EFA686", width=4),
            marker=dict(
                size=10,
                color="white",
                line=dict(color="#EFA686", width=3)
            ),
            text=[formatear_valor(x) for x in dff[anio1]],
            textposition="top center",
            textfont=dict(size=12, color="#EFA686")
        )
    )

    fig.add_trace(
        go.Scatter(
            x=dff[col_mes],
            y=dff[anio2],
            mode="lines+markers",
            name=str(anio2),
            line=dict(color="#006077", width=4),
            marker=dict(
                size=10,
                color="white",
                line=dict(color="#006077", width=3)
            )
        )
    )

    for x, y in zip(dff[col_mes], dff[anio2]):
        if pd.isna(y):
            continue

        fig.add_annotation(
            x=x,
            y=y,
            text=f"<b>{formatear_valor(y)}</b>",
            showarrow=False,
            yshift=22,
            bgcolor="#006077",
            bordercolor="#006077",
            borderpad=4,
            font=dict(color="white", size=12)
        )

    fig.add_trace(
        go.Scatter(
            x=dff[col_mes],
            y=dff[cumplimiento] * 100,
            mode="lines+markers",
            name="% Cump. Meta",
            yaxis="y2",
            line=dict(color="#8A8A8A", width=3, dash="dot"),
            marker=dict(
                size=9,
                color="white",
                line=dict(color="#8A8A8A", width=3)
            )
        )
    )

    for x, y in zip(dff[col_mes], dff[cumplimiento]):
        if pd.isna(y):
            continue

        var = y * 100
        color = "#13B66A" if var >= 0 else "#F04E5E"

        fig.add_annotation(
            x=x,
            y=var,
            yref="y2",
            text=f"<b>{var:+.1f}%</b>",
            showarrow=False,
            yshift=18,
            bgcolor=color,
            bordercolor=color,
            borderpad=4,
            font=dict(color="white", size=11)
        )

    if ymin is None:
        ymin = dff[[anio1, anio2]].min().min() * 0.95

    if ymax is None:
        ymax = dff[[anio1, anio2]].max().max() * 1.10

    fig.update_layout(
        title=dict(
            text=f"<b>{titulo}</b>",
            x=0.5,
            xanchor="center",
            y=0.98,
            yanchor="top",
            font=dict(
                size=20,
                color="#1F2D3D",
                family="Segoe UI"
            )
        ),
        template="simple_white",
        autosize=True,
        height=400,
        hovermode="x unified",
        margin=dict(l=40, r=40, t=40, b=30),
        legend=dict(
            orientation="v",
            x=1.02,
            y=1,
            xanchor="left",
            yanchor="top",
            font=dict(
                size=14,
                color="#1F2D3D"
            ),
            bgcolor="rgba(255,255,255,0)",
            borderwidth=0
        ),
        xaxis=dict(
            title="",
            categoryorder="array",
            categoryarray=orden,
            tickfont=dict(size=14)
        ),
        yaxis=dict(
            title="",
            range=[ymin, ymax],
            gridcolor="#E8EDF5",
            zeroline=False,
            tickfont=dict(size=13)
        ),
        yaxis2=dict(
            overlaying="y",
            side="right",
            range=[y2min * 100, y2max * 100],
            showgrid=False,
            zeroline=False,
            visible=False
        ),
        paper_bgcolor="white",
        plot_bgcolor="white"
    )

    fig.write_html(
        archivo_html,
        include_plotlyjs="cdn",
        full_html=True,
        config={
            "displayModeBar": False,
            "responsive": True
        }
    )


def KPI_Productividad_HTML(
    prod,
    titulo,
    prod_meta,
    prod_aa,
    var_meta,
    var_aa,
    nombre_html="kpi_productividad.html",
):
    cumplimiento = (var_meta - 1) * 100
    variacion = (var_aa - 1) * 100

    def formatear_numero(x):
        if abs(x) < 100:
            return f"{x:,.1f}"
        return f"{x:,.0f}"
    
    color_cump, fondo_cump = (
        ("#16a34a", "#ecfdf5") if cumplimiento >= 0 else ("#dc2626", "#fef2f2")
    )
    color_var, fondo_var = (
        ("#16a34a", "#ecfdf5") if variacion >= 0 else ("#dc2626", "#fef2f2")
    )
    flecha = "▲" if variacion >= 0 else "▼"
    color_dot = (
        "#16a34a"
        if var_meta >= 0
        else ("#f59e0b" if var_meta >= -0.5 else "#dc2626")
    )

    html = f"""<!DOCTYPE html>
    <html>
    <head>
    <meta charset="utf-8">
    <title>{titulo.upper()}</title>
    <style>
        body {{ 
            margin: 0; 
            background: transparent; 
            font-family: Segoe UI, Arial, sans-serif; 
            height: 100vh; 
            display: flex; 
            flex-direction: column;
            box-sizing: border-box;
        }}
        .card {{ 
            width: 100%; 
            height: 100%;
            box-sizing: border-box; 
            background: white; 
            padding: 24px; 
            border-top: 4px solid #143c8c; 
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }}
        .titulo {{ 
            color: #6b7280; 
            font-size: 14px; 
            font-weight: 700; 
            letter-spacing: 2px; 
            text-transform: uppercase; 
        }}
        .principal {{ 
            display: flex; 
            justify-content: space-between; 
            align-items: center; 
            margin-top: 10px; 
        }}
        .valor {{ 
            font-size: 64px; 
            font-weight: 800; 
            color: #13213d; 
            line-height: 1; 
        }}
        .dot {{ 
            width: 16px; 
            height: 16px; 
            border-radius: 50%; 
            background: {color_dot}; 
            box-shadow: 0 0 8px {color_dot}; 
        }}
        hr {{ 
            border: none; 
            border-top: 1px solid #e5e7eb; 
            margin: 16px 0; 
        }}
        .grid {{ 
            display: grid; 
            grid-template-columns: 1fr 1fr; 
            gap: 14px; 
            flex-grow: 1;
        }}
        .box {{ 
            border: 1px solid #e5e7eb; 
            border-radius: 12px; 
            padding: 14px; 
            background: #fafafa; 
            display: flex;
            flex-direction: column;
            justify-content: center;
        }}
        .label {{ 
            font-size: 16px; 
            color: #6b7280; 
            font-weight: 600; 
        }}
        .dato {{ 
            margin-top: 6px; 
            font-size: 24px; 
            font-weight: 700; 
            color: #111827; 
        }}
        .badge {{ 
            display: inline-block; 
            margin-top: 6px; 
            padding: 6px 12px; 
            border-radius: 999px; 
            font-size: 18px; 
            font-weight: 700; 
            text-align: center;
            width: fit-content;
        }}
    </style>
    </head>
    <body>
    <div class="card">
        <div>
            <div class="titulo">{titulo.upper()}</div>
            <div class="principal">
                <div class="valor">{formatear_numero(prod)}</div>
                <div class="dot"></div>
            </div>
        </div>
        <hr>
        <div class="grid">
            <div class="box"><div class="label">Meta</div><div class="dato">{formatear_numero(prod_meta)}</div></div>
            <div class="box"><div class="label">Cumplimiento</div><div class="badge" style="background:{fondo_cump};color:{color_cump};">{cumplimiento:.1f}%</div></div>
            <div class="box"><div class="label">Año anterior</div><div class="dato">{formatear_numero(prod_aa)}</div></div>
            <div class="box"><div class="label">Variación</div><div class="badge" style="background:{fondo_var};color:{color_var};">{flecha} {variacion:+.1f}%</div></div>
        </div>
    </div>
    </body>
    </html>"""

    with open(nombre_html, "w", encoding="utf-8") as f:
        f.write(html)


def Ranking_HTML(df, col_nombre, col_cumplimiento, col_valor1, col_valor2, col_valor3, titulo, archivo_html):
    dff = df.copy().sort_values(col_cumplimiento, ascending=False).reset_index(drop=True)
    maximo = dff[col_cumplimiento].max()

    html = f"""<!DOCTYPE html>
    <html>
    <head>
    <meta charset="utf-8">
    <style>
        body {{ font-family:Segoe UI,Arial,sans-serif; margin:10px; background:white; overflow-x: hidden; }}
        h2 {{ text-align:center; margin-bottom:20px; color:#2c3e50; font-weight:700; font-size:20px; }}
        .fila {{ display:flex; align-items:center; margin-bottom:8px; width: 100%; }}
        .nombre {{ width:110px; min-width:110px; text-align:right; padding-right:8px; font-size:15px; font-weight:600; color:#555; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        .contenedor {{ flex:1; min-width: 0; }}
        .barra {{ height:38px; border-radius:4px; display:flex; align-items:center; padding-left:10px; padding-right:10px; color:white; font-size:14px; font-weight:700; justify-content: space-between; box-sizing: border-box; }}
        .metrics {{ display: flex; gap: 4px; white-space: nowrap; }}
    </style>
    </head>
    <body>
    <h2>{titulo}</h2>"""

    for _, row in dff.iterrows():
        valor = row[col_cumplimiento]
        ancho = max(35, (valor / maximo * 100))
        color = "#3F9142" if valor >= 1 else "#D9534F"

        texto1 = f"{(row[col_valor1])*100:+.1f}%"
        texto2 = f"{(row[col_valor2])*100:+.1f}%"
        valor3 = row[col_valor3]
        texto3 = f"{valor3/1_000_000:.1f}M" if abs(valor3) >= 1_000_000 else (f"{valor3/1_000:.1f}K" if abs(valor3) >= 1_000 else f"{valor3:.1f}")

        html += f"""
        <div class="fila">
            <div class="nombre" title="{row[col_nombre]}">{row[col_nombre]}</div>
            <div class="contenedor">
                <div class="barra" style="width:{ancho:.1f}%; background:{color};">
                    <span class="metrics"><span>{texto1}</span><span>|</span><span>{texto2}</span><span>|</span><span>{texto3}</span></span>
                </div>
            </div>
        </div>"""

    html += "</body></html>"
    with open(archivo_html, "w", encoding="utf-8") as f: f.write(html)


def generar_reporte_consolidado(
    kpi_path="kpi_productividad.html",
    historico_path="Historico_Productividad.html",
    gos_path="Ranking_GOS.html",
    gor_path="Ranking_GOR.html",
    top_path="Ranking_TiendasTOP.html",
    bot_path="Ranking_TiendasBOT.html",
    output_path="dashboard_consolidado.html"
):
    html_template = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard de Productividad Consolidado</title>
    <style>
        html, body {{
            margin: 0;
            padding: 0;
            width: 100%;
            height: 100vh;
            background-color: #f4f6fa;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            overflow: hidden; /* Elimina por completo cualquier barra de scroll */
            display: flex;
            justify-content: center;
            align-items: center;
        }}
        .dashboard-container {{
            box-sizing: border-box;
            padding: 15px;
            display: flex;
            flex-direction: column;
            gap: 15px;
            
            /* Ajuste dinámico de contención manteniendo proporción 12.15 : 5.8 */
            width: min(100vw, calc(100vh * (12.15 / 5.8)));
            height: min(100vh, calc(100vw * (5.8 / 12.15)));
            aspect-ratio: 12.15 / 5.8;
        }}
        .top-row {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 15px;
            flex: 4.2; 
            min-height: 0;
        }}
        .top-row .card-chart {{
            grid-column: span 3;
        }}
        .bottom-row {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 15px;
            flex: 5.4; 
            min-height: 0;
        }}
        .card {{
            background: #ffffff;
            border-radius: 14px;
            box-shadow: 0 4px 14px rgba(0,0,0,.03);
            overflow: hidden;
            display: flex;
        }}
        iframe {{
            width: 100%;
            height: 100%;
            border: none;
            background: transparent;
        }}
    </style>
</head>
<body>
    <div class="dashboard-container">
        <div class="top-row">
            <div class="card"><iframe src="{kpi_path}"></iframe></div>
            <div class="card card-chart"><iframe src="{historico_path}"></iframe></div>
        </div>
        <div class="bottom-row">
            <div class="card"><iframe src="{gos_path}"></iframe></div>
            <div class="card"><iframe src="{gor_path}"></iframe></div>
            <div class="card"><iframe src="{top_path}"></iframe></div>
            <div class="card"><iframe src="{bot_path}"></iframe></div>
        </div>
    </div>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_template)
    
    return output_path


import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def HTML_a_PNG(html_path, png_path=None, width=1920, height=1080):

    if png_path is None:
        png_path = os.path.splitext(html_path)[0] + ".png"

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument(f"--window-size={width},{height}")

    # --- SOLUCIÓN NATIVA DEL MOTOR CHROMIUM ---
    # Desactiva las barras de desplazamiento a nivel navegador (incluye iframes y componentes internos)
    options.add_argument("--hide-scrollbars")
    # ------------------------------------------

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )

    driver.get("file:///" + os.path.abspath(html_path))

    # Espera para asegurar que los gráficos carguen por completo
    time.sleep(2)

    driver.save_screenshot(png_path)
    driver.quit()

    return png_path

def Heatmap_GOR(
    df,
    gor="GOR",
    mes="Mes",
    mes_prior = "Jul",
    valor="Cump_Meta",
    archivo_html="Heatmap_GOR.html",
    titulo="Cumplimiento vs Meta"
):

    meses = {
        "01": "Ene", "02": "Feb", "03": "Mar", "04": "Abr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Ago",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dic"
    }

    dff = df.copy()

    dff[mes] = (
        dff[mes]
        .astype(str)
        .str.zfill(2)
        .map(meses)
    )

    orden = [
        "Ene", "Feb", "Mar", "Abr", "May", "Jun",
        "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"
    ]

    tabla = (
        dff
        .pivot(index=gor, columns=mes, values=valor)
        .reindex(columns=[m for m in orden if m in dff[mes].unique()])
    )

    tabla = tabla.sort_values(by = mes_prior, ascending = False)

    def obtener_color(v):

        if v < -0.10:
            return "#db0000"

        elif v < -0.05:
            return "#f8d1d2"

        elif v < 0:
            return "#f8d1d2"

        elif v < 0.05:
            return "#cfecce"

        elif v < 0.10:
            return "#41b35c"

        else:
            return "#157f3d"

    html = f"""
<!DOCTYPE html>
<html>

<head>

<meta charset="utf-8">

<style>

body{{
    font-family:'Segoe UI',sans-serif;
    background:#ffffff;
    margin:16px;
}}

h2{{
    margin:0 0 16px 0;
    color:#143c8c;
    font-size:28px;
    font-weight:700;
}}

table{{
    width:100%;
    border-collapse:collapse;
    table-layout:fixed;
    border-radius:14px;
    overflow:hidden;
    box-shadow:0 4px 14px rgba(0,0,0,.18);
}}

th{{
    background:#143c8c;
    color:white;
    padding:18px 12px;
    font-size:26px;
    font-weight:700;
    border:1px solid rgba(255,255,255,.10);
}}

td{{
    padding:18px 10px;
    text-align:center;
    font-size:24px;
    font-weight:600;
    border:1px solid #ececec;
    font-variant-numeric:tabular-nums;
}}

th.gor{{
    width:340px;
    text-align:left;
}}

td.gor{{
    width:340px;
    min-width:340px;
    text-align:left;
    background:white;
    color:#222;
    padding-left:22px;
    padding-right:18px;
    white-space:nowrap;
    font-size:26px;
    font-weight:600;
}}

tr:hover td{{
    filter:brightness(0.98);
}}

</style>

</head>

<body>

<h2>{titulo}</h2>

<table>

<tr>

<th class="gor">GOR</th>
"""
    for c in tabla.columns:
        html += f"<th>{c}</th>"

    html += "</tr>"
    for pos, indice in enumerate(tabla.index):

        if pos == 0:
            icono = "🥇"
        elif pos == 1:
            icono = "🥈"
        elif pos == 2:
            icono = "🥉"
        else:
            icono = '<span style="visibility:hidden;">🥉</span>'


        html += f"<tr><td class='gor'>{icono} {indice}</td>"

        for c in tabla.columns:

            v = tabla.loc[indice, c]

            if pd.isna(v):
                html += "<td></td>"
                continue

            color = obtener_color(v)

            if color in ["#8b0000", "#db0000", "#157f3d"]:
                texto = "white"
            else:
                texto = "#111111"

            if v > 0:
                flecha = "▲"
            elif v < 0:
                flecha = "▼"
            else:
                flecha = ""

            html += f"""
<td style="
background:{color};
color:{texto};
">
{v:.1%} {flecha}
</td>
"""

        html += "</tr>"

    html += """
</table>

</body>

</html>
"""

    with open(archivo_html, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Archivo generado: {archivo_html}")

def tiendas_top_bot_gor(df: pd.DataFrame, lista_gor: list, nombre_archivo: str, ascending: bool = True) -> str:
    """
    Genera un archivo HTML en formato matriz 3x3 emulando el diseño de image_70ff48.jpg.
    Muestra exactamente las 5 tiendas con su respectivo cumplimiento por cada GOR,
    sin subtítulos y guarda el archivo directamente en el disco.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Dataframe con las columnas ["GOR", "Tienda", "Cump_Meta"].
    lista_gor : list
        Lista única con los 9 GORs a graficar.
    nombre_archivo : str
        Nombre o ruta del archivo HTML que se va a generar (ej. "reporte_gors.html").
    ascending : bool, default True
        True para ordenar de menor a mayor cumplimiento, False para el caso contrario.
    """
    html_content = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        @page {
            size: A4 landscape;
            margin: 12mm;
            background-color: #f4f6f9;
        }
        * { box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Arial, sans-serif;
            margin: 0; padding: 0;
            background-color: #f4f6f9;
        }
        .dashboard-table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 15px;
        }
        .gor-card-cell {
            width: 33.33%;
            vertical-align: top;
            background: #ffffff;
            border-radius: 6px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            padding-bottom: 12px;
            border: 1px solid #e2e8f0;
        }
        .gor-header {
            background-color: #003366;
            color: #ffffff;
            text-align: center;
            font-weight: bold;
            font-size: 16pt;
            padding: 12px 5px;
            border-top-left-radius: 5px;
            border-top-right-radius: 5px;
            letter-spacing: 0.5px;
        }
        .tiendas-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 8px;
        }
        .tiendas-table td {
            padding: 6px 12px;
            font-size: 14pt;
            color: #2d3748;
            vertical-align: middle;
        }
        .tienda-name {
            text-align: left;
            width: 70%;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .badge-container {
            text-align: right;
            width: 30%;
        }
        .metric-badge {
            display: block;
            text-align: center;
            font-weight: bold;
            font-size: 12pt;
            color: #ffffff;
            padding: 5px 6px;
            border-radius: 5px;
            width: 75px;
            margin-left: auto;
        }
        .badge-red { background-color: #cc0000; }
        .badge-green { background-color: #008800; }
    </style>
</head>
<body>
    <table class="dashboard-table">
    """
    
    # Construcción de la matriz 3x3
    for i in range(0, len(lista_gor), 3):
        html_content += "        <tr>\n"
        for j in range(3):
            if i + j < len(lista_gor):
                gor = lista_gor[i + j]
                
                # Filtrado eficiente y ordenamiento dinámico para extraer el top/bottom 5 exacto
                df_gor_bot1 = (df[df["GOR"] == gor][["Tienda", "Cump_Meta"]]
                               .sort_values(by="Cump_Meta", ascending=ascending)
                               .head(5))
                
                html_content += "            <td class='gor-card-cell'>\n"
                html_content += f"                <div class='gor-header'>{gor}</div>\n"
                html_content += "                <table class='tiendas-table'>\n"
                
                for _, row in df_gor_bot1.iterrows():
                    val = row["Cump_Meta"]
                    
                    # Condición solicitada: menor a 0 -> rojo, mayor -> verde
                    badge_class = "badge-red" if val < 0 else "badge-green"
                    
                    # Formateo visual para porcentajes
                    val_str = f"{val * 100:.1f}%" if abs(val) <= 1.0 else f"{val:.1f}%"
                    
                    html_content += "                    <tr>\n"
                    html_content += f"                        <td class='tienda-name'>{row['Tienda']}</td>\n"
                    html_content += f"                        <td class='badge-container'><span class='metric-badge {badge_class}'>{val_str}</span></td>\n"
                    html_content += "                    </tr>\n"
                    
                html_content += "                </table>\n"
                html_content += "            </td>\n"
            else:
                html_content += "            <td style='width:33.33%; visibility:hidden;'></td>\n"
        html_content += "        </tr>\n"
        
    html_content += """    </table>
</body>
</html>
"""
    
    # Creación y escritura del archivo directamente en disco con codificación segura UTF-8
    with open(nombre_archivo, "w", encoding="utf-8") as f:
        f.write(html_content)
        
    return f"Archivo generado con éxito en: {os.path.abspath(nombre_archivo)}"




def Barras_GOR(
    df,
    gor="GOR",
    ancho="Cump_Meta2",
    cump_meta="Cump_Meta",
    cump_aa="Cump_AA",
    prod="Prod",
    titulo="Productividad",
    archivo_html="Barras_GOR.html"
):

    dff = df.copy()

    dff = dff.sort_values(ancho, ascending=False)

    minimo = dff[ancho].min()
    maximo = dff[ancho].max()

    def porcentaje_barra(v):

        if maximo == minimo:
            return 100

        return 40 + (v - minimo) / (maximo - minimo) * 60

    def color(v):

        if v >= 1:
            return "#2e7d32"

        elif v >= 0.95:
            return "#f4b400"

        else:
            return "#db4437"

    html = f"""
<!DOCTYPE html>
<html>

<head>

<meta charset="utf-8">

<style>

body{{
    font-family:'Segoe UI',sans-serif;
    margin:20px;
    background:white;
}}

h2{{
    color:#143c8c;
}}

table{{
    width:100%;
    border-collapse:separate;
    border-spacing:0 8px;
}}

td.nombre{{
    width:260px;
    font-size:22px;
    font-weight:600;
    color:#555;
    padding-right:12px;
    white-space:nowrap;
}}

td.barra{{
    width:100%;
}}

.contenedor{{
    width:100%;
    background:white;
}}

.valor{{
    height:54px;
    border-radius:0px;
    color:white;
    display:flex;
    align-items:center;
    padding-left:18px;
    font-size:22px;
    font-weight:700;
    box-sizing:border-box;
}}

</style>

</head>

<body>

<h2>{titulo}</h2>

<table>
"""

    for _, r in dff.iterrows():

        ancho_barra = porcentaje_barra(r[ancho])

        texto = (
            f"{r[cump_meta]:.1%}"
            f" | {r[cump_aa]:.1%}"
            f" | {f'{r[prod]/1000:.1f} K' if r[prod] >= 1000 else f'{r[prod]:.1f}'}"
        )

        html += f"""
<tr>

<td class="nombre">
{r[gor]}
</td>

<td class="barra">

<div class="contenedor">

<div class="valor"
style="
width:{ancho_barra:.1f}%;
background:{color(r[ancho])};
">
{texto}
</div>

</div>

</td>

</tr>
"""

    html += """
</table>

</body>

</html>
"""

    with open(archivo_html, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Archivo generado: {archivo_html}")
import pandas as pd

config_reglas_prod = {
    "ELECTRO":      {"tipo": "incluir", "grupo": ["ELECTRO"]},
    "FRESCOS":      {"tipo": "incluir", "grupo": ["FRESCOS"]},
    'ALMACEN FRESCOS':  {"tipo": "incluir", "grupo": ["FRESCOS"]},
    'ABARROTES':    {"tipo": "incluir", "grupo": ["ABARROTES", "NONFOOD"]},
    'CAJAS':        {"tipo": "excluir", "condicion_col": "tipo_venta", "condicion_val": "Venta Ecommerce", "exclusion_col": "area", "exclusion_grupo": ["ELECTRO", "NONFOOD"]},
    'RECEPCION':    {"tipo": "total_general"},
    'ALMACEN':      {"tipo": "incluir", "grupo": ["ABARROTES", "NONFOOD", "ELECTRO"]},
    'INVENTARIOS':  {"tipo": "incluir", "grupo": ["ABARROTES", "NONFOOD", "ELECTRO"]},
    'MULTIFUNCIONAL': {"tipo": "total_general"},
    }


def logica_area_puesto (df_origen, configuracion_areas):
    tipos_venta_validos = ["Venta Ecommerce", "Venta POS", "Last Mile"]
    df_base = df_origen[df_origen["tipo_venta"].isin(tipos_venta_validos)].copy()
    df_base["Vta."] = df_base["VtaNeta"].fillna(0)
    df_base["VtaUn."] = df_base["VtaUnidad"].fillna(0)
    df_base["VtaPpto."] = df_base["VtaNetaPpto"].fillna(0)

    lista_resultados = []

    for nombre_area, config in configuracion_areas.items():
        tipo_filtro = config["tipo"]

        if tipo_filtro == "incluir":
            mask = df_base["area"].isin(config["grupo"])
            df_filtrado = df_base[mask].copy()

        elif tipo_filtro == "excluir":
            col_condicion = config["condicion_col"]
            val_condicion = config["condicion_val"]
            col_excluir = config["exclusion_col"]
            grupo_excluir = config["exclusion_grupo"]
            mask_exclusion = (df_base[col_condicion] == val_condicion) & (df_base[col_excluir].isin(grupo_excluir) )
            df_filtrado = df_base[~mask_exclusion].copy()

        elif tipo_filtro == "total_general":
            df_filtrado = df_base.copy()

        else:
            continue

        df_filtrado["area"] = nombre_area

        df_agrupado = (df_filtrado.groupby(["_Tienda", "Anio", "NroMes", "area"]).agg({"Vta.": "sum", "VtaUn.": "sum", "VtaPpto.": "sum"}).reset_index() )
        lista_resultados.append(df_agrupado)

    df_consolidado = pd.concat(lista_resultados, ignore_index=True)
    return df_consolidado


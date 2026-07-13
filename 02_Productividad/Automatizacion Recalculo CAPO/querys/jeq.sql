WITH

raw_horas_reales AS (
    SELECT
        FORMAT_DATE('%Y%m', FECHA) AS periodo,
        LOCAL as local,
        TRIM(REGEXP_REPLACE(NORMALIZE(UPPER(PUESTO), NFKD), r'\pM', '')) AS puesto,
        TRIM(REGEXP_REPLACE(NORMALIZE(UPPER(SECCION), NFKD), r'\pM', '')) AS seccion,
        SUM(HRS_REAL) AS hrs_real
    FROM `d-sfh-un-pvea.raw_operaciones.raw_horas_reales`
    WHERE FECHA >= DATE '2026-01-01' and FECHA <= DATE '2026-01-31'
    GROUP BY 1,2,3,4
),

dim_dict_local AS (
    SELECT distinct
        nombre_Kr,
        codigo_local
    FROM `d-sfh-un-pvea.raw_operaciones.dim_dict_local`
),

dim_puestos AS (
    SELECT
        departamento,
        nombre_posicion,
        grupo_area
    FROM (
        SELECT
            TRIM(REGEXP_REPLACE(NORMALIZE(UPPER(departamento), NFKD), r'\pM', '')) AS departamento,
            TRIM(REGEXP_REPLACE(NORMALIZE(UPPER(nombre_de_posicion), NFKD), r'\pM', '')) AS nombre_posicion,
            TRIM(REGEXP_REPLACE(NORMALIZE(UPPER(grupo_area), NFKD), r'\pM', '')) AS grupo_area,
            ROW_NUMBER() OVER (
                PARTITION BY
                    TRIM(REGEXP_REPLACE(NORMALIZE(UPPER(departamento), NFKD), r'\pM', '')),
                    TRIM(REGEXP_REPLACE(NORMALIZE(UPPER(nombre_de_posicion), NFKD), r'\pM', ''))
            ) AS rn
        FROM `d-sfh-un-pvea.raw_operaciones.dim_puestos`
    )
    WHERE rn = 1
),

dim_secciones AS (
    SELECT
        tipo,
        _Es_Total,
        _Area,
        _ConsideraSeccion
    FROM (
        SELECT
            TRIM(REGEXP_REPLACE(NORMALIZE(UPPER(TIPO), NFKD), r'\pM', '')) AS tipo,
            _Es_Total,
            _Area,
            _ConsideraSeccion,
            ROW_NUMBER() OVER (
                PARTITION BY
                    TRIM(REGEXP_REPLACE(NORMALIZE(UPPER(TIPO), NFKD), r'\pM', ''))
            ) AS rn
        FROM `d-sfh-un-pvea.raw_operaciones.dim_secciones`
    )
    WHERE rn = 1
),


horas_reales AS (
    SELECT
        hr.periodo,
        dl.codigo_local,
        hr.puesto,
        hr.seccion,
        hr.hrs_real,
        p.grupo_area,

        CASE
            WHEN p.grupo_area = 'ADM' THEN 'Adm'
            WHEN s.tipo IS NULL THEN 'adm'
            ELSE s.tipo
        END AS tipo,

        s._Es_Total,
        s._Area,
        s._ConsideraSeccion

    FROM raw_horas_reales hr

    INNER JOIN dim_dict_local dl
        ON hr.local = dl.nombre_Kr

    LEFT JOIN dim_puestos p
        ON hr.seccion = p.departamento
       AND hr.puesto = p.nombre_posicion

    LEFT JOIN dim_secciones s
        ON p.grupo_area = s.tipo
),
jeq AS (
    SELECT
        periodo,
        codigo_local,
        tipo,
        grupo_area,
        SUM(hrs_real) AS hrs_real
    FROM horas_reales
    GROUP BY 1,2,3,4
)
SELECT * FROM horas_reales;



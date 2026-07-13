SELECT 
    CONCAT(CAST(year AS STRING), LPAD(CAST(mes AS STRING), 2, '0')) AS periodo,
    codigo_local,
    tipo_venta,
    nombre_division,
    nombre_area,
    CASE
        WHEN nombre_area = 'ELECTRO' THEN 'ELECTRO'
        WHEN nombre_division = 'FRESCOS' THEN 'FRESCOS'
        WHEN nombre_division = 'NON FOOD' THEN 'NONFOOD'
        ELSE 'ABARROTES'
    END AS area,
    SUM(venta) AS venta,
    SUM(presupuesto) AS ppto
FROM `spsa-operaciones-seg-sd.bi_imp.cv_venta`
WHERE year >= 2025
GROUP BY 1,2,3,4,5,6;
-- El nombre de las compañias que han tenido mayor venta de acciones en el año 2018
SELECT 
    dn.name,
    SUM(volume) AS volume
FROM 
    fact_stockshare fs2
INNER JOIN 
    dim_name dn 
    ON fs2.name_id = dn.name_id
WHERE 
    DATE_TRUNC('year', "date") = '2018-01-01'
GROUP BY 
    dn."name"
ORDER BY 
    SUM(volume) DESC;
   
-- Los meses en los que mas compañias tuvieron su mayor venta
WITH ctevolumen AS (
    SELECT 
        dn.name,
        DATE,
        volume,
        ROW_NUMBER() OVER (
            PARTITION BY dn.name 
            ORDER BY volume DESC
        ) AS auxMax
    FROM 
        fact_stockshare fs2
    INNER JOIN 
        dim_name dn 
        ON fs2.name_id = dn.name_id
)


SELECT 
    DATE_TRUNC('month', "date") AS month,
    COUNT(1) AS Count
FROM 
    ctevolumen a
WHERE 
    auxMax = 1
GROUP BY 
    month
HAVING 
    COUNT(1) > 1
ORDER BY 
    COUNT(1) DESC;
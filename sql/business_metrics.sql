-- ============================================================
-- Business Metrics Views for Oil & Gas Star Schema (2015-2026)
-- ============================================================
-- Ventana de analisis aplicada en todas las vistas:
-- fecha >= DATE '2015-01-01' AND fecha < DATE '2027-01-01'
-- Supuestos de esquema:
-- fact_produccion(fecha, id_empresa, id_geo, id_pozo, prod_pet, prod_gas, prod_agua, iny_agua, tef, ...)
-- dim_tiempo(fecha, anio, mes, trimestre, semestre)
-- dim_empresa(id_empresa, nombre_empresa)
-- dim_geografia(id_geo, cuenca, provincia, yacimiento)
-- dim_pozo(id_pozo, nombre_pozo, tipo_reservorio, profundidad)


-- 1) Rentabilidad por Cuenca
-- Gráfico sugerido: Combo Chart (columnas para produccion_petroleo, linea para ratio_rentabilidad)
CREATE OR REPLACE VIEW vw_rentabilidad_cuenca_anual AS
WITH base AS (
    SELECT
        t.anio,
        g.cuenca,
        SUM(f.prod_pet) AS prod_pet_acum,
        COUNT(DISTINCT CASE
            WHEN COALESCE(f.prod_pet, 0) > 0
              OR COALESCE(f.prod_gas, 0) > 0
              OR COALESCE(f.tef, 0) > 0
            THEN f.id_pozo
        END) AS pozos_activos
    FROM fact_produccion f
    JOIN dim_tiempo t ON t.fecha = f.fecha
    JOIN dim_geografia g ON g.id_geo = f.id_geo
    WHERE f.fecha >= DATE '2015-01-01'
      AND f.fecha < DATE '2027-01-01'
    GROUP BY t.anio, g.cuenca
)
SELECT
    anio,
    cuenca,
    prod_pet_acum,
    pozos_activos,
    prod_pet_acum / NULLIF(pozos_activos, 0) AS ratio_rentabilidad
FROM base;


-- 2) Uptime (Eficiencia Operativa) mensual por empresa y yacimiento
-- Gráfico sugerido: Heatmap (empresa vs yacimiento, color = uptime_pct) o Line Chart por empresa
CREATE OR REPLACE VIEW vw_uptime_mensual_empresa_yacimiento AS
WITH base AS (
    SELECT
        t.anio,
        t.mes,
        e.nombre_empresa,
        g.yacimiento,
        SUM(f.tef) AS tef_total,
        COUNT(DISTINCT f.id_pozo) AS pozos_reportando,
        EXTRACT(DAY FROM (date_trunc('month', MIN(f.fecha)) + INTERVAL '1 month - 1 day'))::INT AS dias_del_mes
    FROM fact_produccion f
    JOIN dim_tiempo t ON t.fecha = f.fecha
    JOIN dim_empresa e ON e.id_empresa = f.id_empresa
    JOIN dim_geografia g ON g.id_geo = f.id_geo
    WHERE f.fecha >= DATE '2015-01-01'
      AND f.fecha < DATE '2027-01-01'
    GROUP BY t.anio, t.mes, e.nombre_empresa, g.yacimiento
)
SELECT
    anio,
    mes,
    nombre_empresa,
    yacimiento,
    tef_total,
    pozos_reportando,
    dias_del_mes,
    LEAST((tef_total / NULLIF(dias_del_mes * pozos_reportando, 0)) * 100, 100) AS uptime_pct
FROM base;


-- 3) Recuperación Secundaria (iny_agua vs prod_pet) mensual por yacimiento
-- Gráfico sugerido: Scatter Plot (iny_agua vs prod_pet) + linea temporal secundaria
CREATE OR REPLACE VIEW vw_recuperacion_secundaria_mensual AS
WITH base AS (
    SELECT
        t.anio,
        t.mes,
        g.cuenca,
        g.yacimiento,
        SUM(f.iny_agua) AS iny_agua_total,
        SUM(f.prod_pet) AS prod_pet_total
    FROM fact_produccion f
    JOIN dim_tiempo t ON t.fecha = f.fecha
    JOIN dim_geografia g ON g.id_geo = f.id_geo
    WHERE f.fecha >= DATE '2015-01-01'
      AND f.fecha < DATE '2027-01-01'
    GROUP BY t.anio, t.mes, g.cuenca, g.yacimiento
),
filtrado AS (
    SELECT *
    FROM base
    WHERE iny_agua_total > 0
)
SELECT
    anio,
    mes,
    cuenca,
    yacimiento,
    iny_agua_total,
    prod_pet_total,
    iny_agua_total / NULLIF(prod_pet_total, 0) AS ratio_inyeccion_vs_petroleo
FROM filtrado;


-- 4) Water Cut temporal por pozo
-- Gráfico sugerido: Area Chart o Line Chart por pozo (small multiples)
CREATE OR REPLACE VIEW vw_water_cut_mensual_pozo AS
WITH base AS (
    SELECT
        t.anio,
        t.mes,
        p.id_pozo,
        p.nombre_pozo,
        g.cuenca,
        g.yacimiento,
        SUM(f.prod_agua) AS prod_agua_total,
        SUM(f.prod_pet) AS prod_pet_total
    FROM fact_produccion f
    JOIN dim_tiempo t ON t.fecha = f.fecha
    JOIN dim_pozo p ON p.id_pozo = f.id_pozo
    JOIN dim_geografia g ON g.id_geo = f.id_geo
    WHERE f.fecha >= DATE '2015-01-01'
      AND f.fecha < DATE '2027-01-01'
    GROUP BY t.anio, t.mes, p.id_pozo, p.nombre_pozo, g.cuenca, g.yacimiento
)
SELECT
    anio,
    mes,
    id_pozo,
    nombre_pozo,
    cuenca,
    yacimiento,
    prod_agua_total,
    prod_pet_total,
    (prod_agua_total / NULLIF(prod_agua_total + prod_pet_total, 0)) * 100 AS water_cut_pct
FROM base;


-- 5) GOR (Gas-Oil Ratio) y clasificación por empresa
-- Gráfico sugerido: Bar Chart de GOR por empresa y año + segmentación por clase
CREATE OR REPLACE VIEW vw_gor_empresa_anual AS
WITH base AS (
    SELECT
        t.anio,
        e.id_empresa,
        e.nombre_empresa,
        SUM(f.prod_gas) AS prod_gas_total,
        SUM(f.prod_pet) AS prod_pet_total
    FROM fact_produccion f
    JOIN dim_tiempo t ON t.fecha = f.fecha
    JOIN dim_empresa e ON e.id_empresa = f.id_empresa
    WHERE f.fecha >= DATE '2015-01-01'
      AND f.fecha < DATE '2027-01-01'
    GROUP BY t.anio, e.id_empresa, e.nombre_empresa
),
ratio AS (
    SELECT
        anio,
        id_empresa,
        nombre_empresa,
        prod_gas_total,
        prod_pet_total,
        prod_gas_total / NULLIF(prod_pet_total, 0) AS gor
    FROM base
)
SELECT
    anio,
    id_empresa,
    nombre_empresa,
    prod_gas_total,
    prod_pet_total,
    gor,
    CASE
        WHEN gor IS NULL AND prod_gas_total > 0 THEN 'Gas-oriented'
        WHEN gor IS NULL THEN 'No production'
        WHEN gor >= 1 THEN 'Gas-oriented'
        ELSE 'Oil-oriented'
    END AS orientacion
FROM ratio;


-- 6) Pareto de pozos (Top 20% que aportan 80% por cuenca) - detalle por pozo
-- Gráfico sugerido: Pareto Chart (barras + curva acumulada) por cuenca
DROP VIEW IF EXISTS vw_pareto_pozos_cuenca_resumen;
DROP VIEW IF EXISTS vw_pareto_pozos_cuenca_detalle;

CREATE OR REPLACE VIEW vw_pareto_pozos_cuenca_detalle AS
WITH prod_pozo AS (
    SELECT
        g.cuenca,
        p.id_pozo,
        p.nombre_pozo,
        SUM(f.prod_pet) AS prod_pet_total_pozo
    FROM fact_produccion f
    JOIN dim_geografia g ON g.id_geo = f.id_geo
    JOIN dim_pozo p ON p.id_pozo = f.id_pozo
    WHERE f.fecha >= DATE '2015-01-01'
      AND f.fecha < DATE '2027-01-01'
    GROUP BY g.cuenca, p.id_pozo, p.nombre_pozo
),
ranked AS (
    SELECT
        cuenca,
        id_pozo,
        nombre_pozo,
        prod_pet_total_pozo,
        ROW_NUMBER() OVER (
            PARTITION BY cuenca
            ORDER BY prod_pet_total_pozo DESC, id_pozo
        ) AS orden_pozo,
        COUNT(*) OVER (PARTITION BY cuenca) AS total_pozos_cuenca,
        SUM(prod_pet_total_pozo) OVER (PARTITION BY cuenca) AS prod_total_cuenca,
        SUM(prod_pet_total_pozo) OVER (
            PARTITION BY cuenca
            ORDER BY prod_pet_total_pozo DESC, id_pozo
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS prod_acumulada
    FROM prod_pozo
),
metricas AS (
    SELECT
        cuenca,
        id_pozo,
        nombre_pozo,
        prod_pet_total_pozo,
        orden_pozo,
        total_pozos_cuenca,
        prod_total_cuenca,
        prod_acumulada,
        (orden_pozo::NUMERIC / NULLIF(total_pozos_cuenca, 0)) * 100 AS pct_pozos,
        (prod_acumulada / NULLIF(prod_total_cuenca, 0)) * 100 AS pct_prod_acumulada
    FROM ranked
)
SELECT
    cuenca,
    id_pozo,
    nombre_pozo,
    prod_pet_total_pozo,
    orden_pozo,
    total_pozos_cuenca,
    total_pozos_cuenca AS total_pozos_muestra_cuenca,
    prod_total_cuenca,
    pct_pozos,
    pct_prod_acumulada,
    (pct_pozos <= 20) AS es_top_20_pct_pozos,
    (pct_prod_acumulada <= 80) AS dentro_80_pct_produccion
FROM metricas;


-- 7) Pareto resumen por cuenca
-- Gráfico sugerido: Tabla KPI + Donut/Bar para contribución top performers
CREATE OR REPLACE VIEW vw_pareto_pozos_cuenca_resumen AS
WITH base AS (
    SELECT
        cuenca,
        COUNT(*) FILTER (WHERE es_top_20_pct_pozos) AS pozos_top_20_pct,
        COUNT(*) AS pozos_totales,
        SUM(prod_pet_total_pozo) FILTER (WHERE es_top_20_pct_pozos) AS prod_pet_top_20_pct,
        SUM(prod_pet_total_pozo) AS prod_pet_total
    FROM vw_pareto_pozos_cuenca_detalle
    GROUP BY cuenca
)
SELECT
    cuenca,
    pozos_top_20_pct,
    pozos_totales,
    pozos_totales AS total_pozos_muestra_cuenca,
    prod_pet_top_20_pct,
    prod_pet_total,
    (prod_pet_top_20_pct / NULLIF(prod_pet_total, 0)) * 100 AS pct_prod_top_20_pozos
FROM base;

# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: Criterio de Salida Deterioro Segmento Grupal
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_26_CarDet_Gru_Crit_Sal.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 30/09/2022
# MAGIC * Descripcion: notebook que determina cirterio segun familia del criterio 34 a 39.
# MAGIC * Documentacion: https://docs.google.com/spreadsheets/d/1n8IUxUtDIGsMXC7yktDjv9GV7aqt7Jcn/edit#gid=1974089785
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 
# MAGIC * Autor: <Nombre Autor> (<Empresa del Autor (Bci/Otra)>) - Ing. SW BCI: <Nombre Ing. SW BCI>
# MAGIC * Fecha: <dd/mm/yyyy> 
# MAGIC * Descripción: <Descripción de la mantención>      
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin
# MAGIC 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo
# MAGIC 
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios
# MAGIC * Cambiar de tipo de dato el campo origen deterioro (de int a string)
# MAGIC ********************************************************************************************************
# MAGIC ##### Pendientes:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Dependencias

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga funciones comunes

# COMMAND ----------

# MAGIC %run "../segmentacion/Funciones_Comunes" 

# COMMAND ----------

# MAGIC %run "../cartera_deteriorada/Notebook_Apoyo/BCI_AUX_Carga_Ingesta_Temporal_CD"

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/Jerarquia.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parametría

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setea Parámetros

# COMMAND ----------

#dbutils.widgets.removeAll()
dbutils.widgets.text("FechaW","","01-Fecha:")
dbutils.widgets.text("PeriodoW","","02-Periodo:")
dbutils.widgets.text("bd_silverW","","03-Nombre BD Silver:")
dbutils.widgets.text("bd_goldW","","04-Nombre BD Gold:")
dbutils.widgets.text("tipo_procesoW","","05-Tipo de Proceso (C o PC):")

FechaX = dbutils.widgets.get("FechaW") 
PeriodoX = dbutils.widgets.get("PeriodoW")
base_silverX = dbutils.widgets.get("bd_silverW")
base_goldX = dbutils.widgets.get("bd_goldW")
tipo_procesoX = dbutils.widgets.get("tipo_procesoW")

spark.conf.set("bci.Fecha", FechaX)
spark.conf.set("bci.Periodo", PeriodoX)
spark.conf.set("bci.dbnamesilver", base_silverX)
spark.conf.set("bci.dbnameGold", base_goldX)
spark.conf.set("bci.tipo_proceso", tipo_procesoX)

print(f"Fecha de Proceso: {FechaX}")
print(f"Periodo: {PeriodoX}")      
print(f"Nombre BD Silver: {base_silverX}")
print(f"Nombre BD Gold: {base_goldX}")
print(f"Tipo de Proceso: {tipo_procesoX}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Valida parámetros

# COMMAND ----------

# DBTITLE 1,Valida parámetro "FechaX"
validaParametro(FechaX)

# COMMAND ----------

# DBTITLE 1,Valida parámetro "PeriodoX"
validaParametro(PeriodoX)

# COMMAND ----------

# DBTITLE 1,Valida parámetro "base_silverX"
validaParametro(base_silverX)

# COMMAND ----------

# DBTITLE 1,Valida parámetro "base_goldX"
validaParametro(base_goldX)

# COMMAND ----------

# DBTITLE 1,Valida parámetro "tipo_procesoX"
validaParametro(tipo_procesoX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## INICIO Proceso extraccion y transformacion
# MAGIC --------------------------------------
# MAGIC - Por cada fuente que se utilice se debe:
# MAGIC      - Titulo: generar un titulo generico, con nombre fuente, y descripcion del proposito de la extraccion
# MAGIC      - Extraer: para el periodo, o rango de fecha que se necesita la iformacion. Debe tener el prefijo tmp_EXT_{nombrefuente}
# MAGIC      - Transformar: generar la informacion necesaria para la salida final. Se pueden generar mas de una tabla temporal para llegar al resultado final. Debe tener el prefijo tmp_RES_{nombre}_correlativo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Criterios de Deterioro Salida Grupal

# COMMAND ----------

p_GruCritDetEnt = (7,8,9,10,11,12,13)
p_GruCritDetSal = (60,61,62,63,64,65,66,67)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones Deterioradas Grupales (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin)
# MAGIC --------------------------------------
# MAGIC - Se extrae Operaciones Deterioradas Grupales 

# COMMAND ----------

paso_query00 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_ope_prin as
SELECT
  A.periodo_cierre         AS periodo_cierre,
  A.fecha_cierre           AS fecha_cierre,
  A.tipo_proceso           AS tipo_proceso,
  A.rut_cliente            AS rut_cliente,
  A.dv_rut_cliente         AS dv_rut_cliente,
  A.tipo_operacion         AS tipo_operacion,
  A.operacion              AS operacion,
  A.sistema                AS sistema,
  A.segmento               AS segmento,
  A.criterio_entrada       AS criterio_entrada,
  A.origen_deterioro       AS origen_deterioro,
  A.fecha_entrada          AS fecha_entrada
FROM
  {base_silverX}.tbl_cartdet_crit_ent_ope_prin A
WHERE
    A.fecha_cierre = {FechaX}  
AND A.periodo_cierre = {PeriodoX}  
AND A.tipo_proceso = '{tipo_procesoX}'
"""


# COMMAND ----------

sqlSafe(paso_query00)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones Deterioradas Grupales (riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit)
# MAGIC --------------------------------------
# MAGIC - Se extrae Operaciones Deterioradas Grupales periodo actual con condicion de salida (familia)

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_sal_ope_campo as
SELECT 
   periodo_cierre     AS periodo_cierre
  ,fecha_cierre       AS fecha_cierre
  ,tipo_proceso       AS tipo_proceso 
  ,rut_cliente        AS rut_cliente
  ,dv_rut_cliente     AS dv_rut_cliente
  ,tipo_operacion     AS tipo_operacion
  ,operacion          AS operacion
  ,sistema            AS sistema
  ,segmento           AS segmento
  ,criterio_salida    AS criterio_salida
  ,origen_deterioro   AS origen_deterioro
  ,fecha_salida       AS fecha_salida
  ,nombre_campo       AS nombre_campo
  ,valor_campo        AS valor_campo
  ,condicion_regla    AS condicion_regla
  ,valor_regla        AS valor_regla
  ,flag_resultado_regla  AS flag_resultado_regla
FROM
    {base_silverX}.tbl_cartdet_crit_sal_ope_campo
WHERE 
    fecha_cierre = {FechaX}  
AND periodo_cierre = {PeriodoX}  
AND tipo_proceso = '{tipo_procesoX}'
AND criterio_salida  IN {p_GruCritDetSal}
  
"""


# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones (riesgobdu_golden_db.tbl_segmentacion_d00_segmentado)

# COMMAND ----------

#Extrae las operaciones de la tabla riesgobdu_golden_db.tbl_segmentacion_d00_segmentado para el periodo actual
paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado AS
SELECT 
  b.periodo_cierre                      AS periodo_cierre,
  b.fecha_cierre                        AS fecha_cierre,
  b.tipo_proceso                        AS tipo_proceso,
  b.cod_segmento                        AS cod_segmento,
  b.operacion                           AS operacion,
  b.cod_sistema                         AS cod_sistema,
  b.rut_cliente                         AS rut_cliente,
  b.dv_cliente                          AS dv_cliente,
  b.tipo_credito                        AS tipo_credito,
  b.tipo_operacion                      AS tipo_operacion
FROM
  {base_goldX}.tbl_segmentacion_d00_segmentado b
WHERE 
    b.fecha_cierre =  {FechaX}
AND b.periodo_cierre = {PeriodoX}
AND b.tipo_proceso = '{tipo_procesoX}'  
"""

# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Clientes con operaciones con pagos parciales
# MAGIC --------------------------------------
# MAGIC - Se extrae operaciones para evaluar condiciones de salida

# COMMAND ----------

#Extrae las operaciones de la tabla c4_tmp_ope_ctas_par para el periodo actual
paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_ope_ctas_par AS
SELECT 
  b.fec_proc                        AS fecha_cierre,
  b.rut_cli                         AS rut_cliente,
  b.dv                              AS dv,
  b.sistema                         AS cod_sistema,
  b.num_interno_ident               AS operacion,
  b.fec_otorg                       AS fec_otorg,
  b.fec_ext                         AS fec_ext,
  b.nro_ctas                        AS nro_ctas,
  b.periodo                         AS periodo
FROM
  tbl_c4_tmp_ope_ctas_par b
WHERE 
  b.fec_proc = {FechaX}
"""

# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Clientes Lir
# MAGIC --------------------------------------
# MAGIC - Se extrae clientes Lir desde la tabla tbl_LIR que se debe generar

# COMMAND ----------

#Extrae los clientes LIR
paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_LIR AS
SELECT 
  b.rut                             AS rut_cliente,
  b.dv                              AS dv,
  b.dat_cli                         AS dat_cli
FROM
  tbl_LIR b
"""

# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Tabla grupal con la familia por operación 

# COMMAND ----------

paso_query40 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_sal_ope_familia AS
SELECT 
   a.periodo_cierre          AS periodo_cierre
  ,a.fecha_cierre            AS fecha_cierre
  ,a.tipo_proceso            AS tipo_proceso
  ,a.rut_cliente             AS rut_cliente
  ,a.dv_rut_cliente          AS dv_rut_cliente
  ,a.tipo_operacion          AS tipo_operacion  
  ,a.operacion               AS operacion
  ,a.sistema                 AS sistema
  ,a.segmento                AS segmento
  ,b.tipo_credito            AS tipo_credito
  ,CASE 
      WHEN b.tipo_credito IN ('02','09') THEN 'Contingente' 
      WHEN a.tipo_operacion LIKE 'CAE%' THEN 'CAE'
      WHEN a.operacion LIKE 'V%' THEN 'LC_Act'
      WHEN a.operacion LIKE 'A%' THEN 'LC_Act'
      WHEN a.operacion LIKE 'C%' THEN 'LC_Act'
      WHEN a.tipo_operacion = 'CCN008' AND a.sistema = '20' THEN 'LC_Act'
      WHEN a.operacion LIKE 'E0%' THEN 'TCR_Act'
      WHEN a.operacion LIKE 'E1%' THEN 'TCR_Act'
      WHEN a.operacion LIKE 'W%' AND a.sistema = '05' THEN 'TCR_Act'
      WHEN c.operacion IS NOT NULL  THEN 'Pago_Parcial'
      ELSE 'Pago_Estructurado'
   END AS familia_ope
FROM
  tmp_EXT_tbl_cartdet_crit_ent_ope_prin a
LEFT JOIN
  tmp_EXT_tbl_segmentacion_d00_segmentado b
ON
  a.operacion = b.operacion
 AND a.sistema = b.cod_sistema
LEFT JOIN
  tmp_EXT_tbl_ope_ctas_par c
ON
  a.operacion = c.operacion
 AND a.sistema = c.cod_sistema 
 
"""

# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Borrado tabla riesgobdu_silver_db.tbl_cartdet_crit_sal_familia

# COMMAND ----------

paso_query50 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_salida_familia WHERE fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} AND tipo_proceso = '{tipo_procesoX}'  """


# COMMAND ----------

sqlSafe(paso_query50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criterios de Salida por familia
# MAGIC --------------------------------------
# MAGIC - Se extrae operaciones para evaluar condiciones de salida

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Mora Cli (60)

# COMMAND ----------


paso_query60 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_salida_familia
SELECT 
   a.periodo_cierre          AS periodo_cierre
  ,a.fecha_cierre            AS fecha_cierre
  ,a.tipo_proceso            AS tipo_proceso
  ,a.rut_cliente             AS rut_cliente
  ,a.dv_rut_cliente          AS dv_rut_cliente
  ,a.tipo_operacion          AS tipo_operacion
  ,a.operacion               AS operacion
  ,a.sistema                 AS sistema
  ,a.segmento                AS segmento
  ,a.tipo_credito            AS tipo_credito
  ,a.familia_ope             AS familia_ope
  ,'mora_cli'                AS evaluacion
  ,CASE WHEN trim(b.criterio_salida) IS NULL then 0 ELSE b.flag_resultado_regla END AS resultado
FROM 
  tmp_RES_sal_ope_familia a 
LEFT JOIN
  tmp_EXT_tbl_cartdet_crit_sal_ope_campo b
ON
  a.operacion = b.operacion
  AND a.sistema = b.sistema
  AND a.fecha_cierre = b.fecha_cierre
WHERE
  b.criterio_salida = 60
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
"""

# COMMAND ----------

sqlSafe(paso_query60)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Pagos Consecutivos - pago_con (61)

# COMMAND ----------

paso_query70 = f"""
 INSERT INTO {base_silverX}.tbl_cartdet_crit_salida_familia
SELECT 
   a.periodo_cierre          AS periodo_cierre
  ,a.fecha_cierre            AS fecha_cierre
  ,a.tipo_proceso            AS tipo_proceso
  ,a.rut_cliente             AS rut_cliente
  ,a.dv_rut_cliente          AS dv_rut_cliente
  ,a.tipo_operacion          AS tipo_operacion  
  ,a.operacion               AS operacion
  ,a.sistema                 AS sistema
  ,a.segmento                AS segmento
  ,a.tipo_credito            AS tipo_credito
  ,a.familia_ope             AS familia_ope
  ,'pago_con' as evaluacion
  ,CASE WHEN trim(b.criterio_salida) IS NULL then 0 else b.flag_resultado_regla end as resultado
FROM 
  tmp_RES_sal_ope_familia a 
LEFT JOIN
  tmp_EXT_tbl_cartdet_crit_sal_ope_campo b
ON
  a.operacion = b.operacion
  AND a.sistema = b.sistema
  AND a.fecha_cierre = b.fecha_cierre
WHERE
  b.criterio_salida = 61
group by 1,2,3,4,5,6,7,8,9,10,11,12,13  
"""

# COMMAND ----------

sqlSafe(paso_query70)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Mora Sbif - mora_sbif (62)

# COMMAND ----------

paso_query80 = f"""
 INSERT INTO {base_silverX}.tbl_cartdet_crit_salida_familia
SELECT 
   a.periodo_cierre          AS periodo_cierre
  ,a.fecha_cierre            AS fecha_cierre
  ,a.tipo_proceso            AS tipo_proceso
  ,a.rut_cliente             AS rut_cliente
  ,a.dv_rut_cliente          AS dv_rut_cliente
  ,a.tipo_operacion          AS tipo_operacion  
  ,a.operacion               AS operacion
  ,a.sistema                 AS sistema
  ,a.segmento                AS segmento
  ,a.tipo_credito            AS tipo_credito
  ,a.familia_ope             AS familia_ope
  ,'mora_sbif' as evaluacion
  ,CASE WHEN trim(b.criterio_salida) IS NULL then 0 else b.flag_resultado_regla end as resultado
FROM 
  tmp_RES_sal_ope_familia a 
LEFT JOIN
  tmp_EXT_tbl_cartdet_crit_sal_ope_campo b
ON
  a.operacion = b.operacion
  AND a.sistema = b.sistema
  AND a.fecha_cierre = b.fecha_cierre
WHERE
  b.criterio_salida = 62
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
"""

# COMMAND ----------

sqlSafe(paso_query80)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Pagos Parciales - pago_par (63)

# COMMAND ----------

paso_query90 = f"""
 INSERT INTO {base_silverX}.tbl_cartdet_crit_salida_familia
SELECT 
   a.periodo_cierre          AS periodo_cierre
  ,a.fecha_cierre            AS fecha_cierre
  ,a.tipo_proceso            AS tipo_proceso
  ,a.rut_cliente             AS rut_cliente
  ,a.dv_rut_cliente          AS dv_rut_cliente
  ,a.tipo_operacion          AS tipo_operacion  
  ,a.operacion               AS operacion
  ,a.sistema                 AS sistema
  ,a.segmento                AS segmento
  ,a.tipo_credito            AS tipo_credito
  ,a.familia_ope             AS familia_ope
  ,'pago_par'                AS evaluacion
  ,CASE WHEN trim(b.criterio_salida) IS NULL then 0 else b.flag_resultado_regla end as resultado
FROM 
  tmp_RES_sal_ope_familia a 
LEFT JOIN
  tmp_EXT_tbl_cartdet_crit_sal_ope_campo b
ON
  a.operacion = b.operacion
  AND a.sistema = b.sistema
  AND a.fecha_cierre = b.fecha_cierre
WHERE
  b.criterio_salida = 63
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
"""

# COMMAND ----------

sqlSafe(paso_query90)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Sin Refinanciamiento - sin_ref (64)

# COMMAND ----------

paso_query100 = f"""
 INSERT INTO {base_silverX}.tbl_cartdet_crit_salida_familia
SELECT 
   a.periodo_cierre          AS periodo_cierre
  ,a.fecha_cierre            AS fecha_cierre
  ,a.tipo_proceso            AS tipo_proceso
  ,a.rut_cliente             AS rut_cliente
  ,a.dv_rut_cliente          AS dv_rut_cliente
  ,a.tipo_operacion          AS tipo_operacion  
  ,a.operacion               AS operacion
  ,a.sistema                 AS sistema
  ,a.segmento                AS segmento
  ,a.tipo_credito            AS tipo_credito
  ,a.familia_ope             AS familia_ope
  ,'sin_ref' as evaluacion
  ,CASE WHEN trim(b.criterio_salida) IS NULL then 0 else b.flag_resultado_regla end as resultado
FROM 
  tmp_RES_sal_ope_familia a 
LEFT JOIN
  tmp_EXT_tbl_cartdet_crit_sal_ope_campo b
ON
  a.operacion = b.operacion
  AND a.sistema = b.sistema
  AND a.fecha_cierre = b.fecha_cierre
WHERE
  b.criterio_salida = 64
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
"""

# COMMAND ----------

sqlSafe(paso_query100)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### capitalizacion de Pago - pag_cap (65)

# COMMAND ----------

paso_query110 = f"""
 INSERT INTO {base_silverX}.tbl_cartdet_crit_salida_familia
SELECT 
   a.periodo_cierre          AS periodo_cierre
  ,a.fecha_cierre            AS fecha_cierre
  ,a.tipo_proceso            AS tipo_proceso
  ,a.rut_cliente             AS rut_cliente
  ,a.dv_rut_cliente          AS dv_rut_cliente
  ,a.tipo_operacion          AS tipo_operacion  
  ,a.operacion               AS operacion
  ,a.sistema                 AS sistema
  ,a.segmento                AS segmento
  ,a.tipo_credito            AS tipo_credito
  ,a.familia_ope             AS familia_ope
  ,'pago_cap' as evaluacion
  ,CASE WHEN trim(b.criterio_salida) IS NULL then 0 else b.flag_resultado_regla end as resultado
FROM 
  tmp_RES_sal_ope_familia a 
LEFT JOIN
  tmp_EXT_tbl_cartdet_crit_sal_ope_campo b
ON
  a.operacion = b.operacion
  AND a.sistema = b.sistema
  AND a.fecha_cierre = b.fecha_cierre
WHERE
  b.criterio_salida = 65
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
"""

# COMMAND ----------

sqlSafe(paso_query110)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Minimo de meses - min_meses (66)

# COMMAND ----------

paso_query120 = f"""
 INSERT INTO {base_silverX}.tbl_cartdet_crit_salida_familia
SELECT 
   a.periodo_cierre          AS periodo_cierre
  ,a.fecha_cierre            AS fecha_cierre
  ,a.tipo_proceso            AS tipo_proceso
  ,a.rut_cliente             AS rut_cliente
  ,a.dv_rut_cliente          AS dv_rut_cliente
  ,a.tipo_operacion          AS tipo_operacion  
  ,a.operacion               AS operacion
  ,a.sistema                 AS sistema
  ,a.segmento                AS segmento
  ,a.tipo_credito            AS tipo_credito
  ,a.familia_ope             AS familia_ope
  ,'min_mes' as evaluacion
  ,CASE WHEN trim(b.criterio_salida) IS NULL then 0 else b.flag_resultado_regla end as resultado
FROM 
  tmp_RES_sal_ope_familia a 
LEFT JOIN
  tmp_EXT_tbl_cartdet_crit_sal_ope_campo b
ON
  a.operacion = b.operacion
  AND a.sistema = b.sistema
  AND a.fecha_cierre = b.fecha_cierre
WHERE
  b.criterio_salida = 66
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
"""

# COMMAND ----------

sqlSafe(paso_query120)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Sin_Lir (67) 

# COMMAND ----------

paso_query130 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_salida_familia
SELECT 
   a.periodo_cierre          AS periodo_cierre
  ,a.fecha_cierre            AS fecha_cierre
  ,a.tipo_proceso            AS tipo_proceso
  ,a.rut_cliente             AS rut_cliente
  ,a.dv_rut_cliente          AS dv_rut_cliente
  ,a.tipo_operacion          AS tipo_operacion  
  ,a.operacion               AS operacion
  ,a.sistema                 AS sistema
  ,a.segmento                AS segmento
  ,a.tipo_credito            AS tipo_credito
  ,a.familia_ope             AS familia_ope
  ,'sin_lir'                 AS evaluacion
  ,CASE WHEN trim(b.rut_cliente) IS NULL THEN 1 ELSE 0 END AS resultado
FROM 
  tmp_RES_sal_ope_familia a 
LEFT JOIN
  tmp_EXT_tbl_LIR b
ON
  a.rut_cliente = b.rut_cliente
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
"""  


# COMMAND ----------

sqlSafe(paso_query130)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Query borrar

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_salida_familia --where rut_cliente = 16205768 and operacion = 'F90002994185'
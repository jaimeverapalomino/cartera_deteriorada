# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: Cuadratura Deterioro, Segmento Grupal.
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_51_CarDet_Cuadratura_Grupal.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/10/2022
# MAGIC * Descripcion: Cuadratura operaciones deterioradas segmento grupal
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
# MAGIC * riesgobdu_silver_db.tbl_cartdet_grupal_stock
# MAGIC * riesgobdu_golden_db.tbl_segmentacion_d00_segmentado
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db. 
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios
# MAGIC ********************************************************************************************************
# MAGIC ##### Pendientes:
# MAGIC * parametrizar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Dependencias

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga funciones comunes

# COMMAND ----------

# MAGIC %run "../../segmentacion/Funciones_Comunes" 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setea Parámetros

# COMMAND ----------

#dbutils.widgets.removeAll()
dbutils.widgets.text("FechaW","","01-Fecha: {FechaX}")
dbutils.widgets.text("PeriodoW","","02-Periodo: {PeriodoX}")
dbutils.widgets.text("bd_silverW","","03-Nombre BD Silver: base_silverX}")
dbutils.widgets.text("ruta_silverW","","04-Ruta adss Silver: {ruta_silverX}")
dbutils.widgets.text("path_bdW","","05-Ruta adss de la BD Silver: {bd_ruta_silverX}")
dbutils.widgets.text("tipo_procesoW","","06-Tipo de Proceso (C o PC): {tipo_procesoX}")
dbutils.widgets.text("bd_goldW","","07-Nombre BD Gold:")

FechaX = dbutils.widgets.get("FechaW") 
PeriodoX = dbutils.widgets.get("PeriodoW")
base_silverX = dbutils.widgets.get("bd_silverW")
ruta_silverX = dbutils.widgets.get("ruta_silverW")
bd_ruta_silverX = dbutils.widgets.get("path_bdW")
tipo_procesoX = dbutils.widgets.get("tipo_procesoW")
base_goldX = dbutils.widgets.get("bd_goldW")

spark.conf.set("bci.Fecha", FechaX)
spark.conf.set("bci.Periodo", PeriodoX)
spark.conf.set("bci.dbnamesilver", base_silverX)
spark.conf.set("bci.rutasilver", ruta_silverX)
spark.conf.set("bci.bdRutasilver", bd_ruta_silverX)
spark.conf.set("bci.tipo_proceso", tipo_procesoX)
spark.conf.set("bci.dbnameGold", base_goldX)

print(f"Fecha de Proceso: {FechaX}")
print(f"Periodo: {PeriodoX}")      
print(f"Nombre BD Silver: {base_silverX}")
print(f"Ruta abfss Silver: {ruta_silverX}")
print(f"Ruta abfss de la BD Silver: {bd_ruta_silverX}")
print(f"Tipo de Proceso: {tipo_procesoX}")
print(f"Nombre BD Gold: {base_goldX}")

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

# DBTITLE 1,Valida parámetro "ruta_silverX"
validaParametro(ruta_silverX)

# COMMAND ----------

# DBTITLE 1,Valida parámetro "bd_ruta_silverX"
validaParametro(bd_ruta_silverX)

# COMMAND ----------

# DBTITLE 1,Valida parámetro "tipo_procesoX"
validaParametro(tipo_procesoX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Valida Ruta abfss

# COMMAND ----------

validaRuta(ruta_silverX,bd_ruta_silverX)

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
# MAGIC ### Obtiene criterios de entrada deterioro Grupal
# MAGIC ---
# MAGIC * Obtiene de la tabla de parametros los codigos de deterioro grupal

# COMMAND ----------

p_GruCritDetEnt = (7,8,9,10,11,12,13)
p_GruCritDetSal = (27,30,34,35,36,37,38,39,40,41)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Stock Deterioro Grupal (tbl_cartdet_grupal_stock)
# MAGIC ---
# MAGIC * Tabla con stock de deterioro grupal  

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_EXT_tbl_cartdet_grupal_stock_Nuevos AS 
SELECT
   CAST(periodo_cierre              AS STRING)
  ,CAST(fecha_cierre           AS STRING)
  ,CAST(tipo_proceso           AS STRING)
  ,CAST(rut_cliente           AS STRING)
  ,CAST(dv_rut_cliente          AS STRING)
  ,CAST(tipo_operacion          AS STRING)
  ,CAST(operacion           AS STRING)
  ,CAST(sistema             AS STRING)
  ,CAST(segmento           AS STRING)
  ,CAST(criterio_entrada           AS STRING)
  ,CAST(origen_deterioro           AS STRING)
  ,CAST(fecha_entrada           AS STRING)
FROM
  {base_silverX}.tbl_cartdet_grupal_stock A
WHERE
    fecha_cierre = {FechaX}  
AND periodo_cierre = {PeriodoX}  
AND tipo_proceso = '{tipo_procesoX}' 
AND criterio_entrada  IN {p_GruCritDetEnt} 
""" 

# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Stock Deterioro Grupal Proceso Productivo (tbl_segmentacion_d00_segmentado)
# MAGIC ---
# MAGIC * Tabla con stock de deterioro grupal calculado con proceso productivo 

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_EXT_tbl_cartdet_grupal_stock_prd AS 
SELECT
  CAST(cast(substr(cast(periodo_cierre as string),1,6) as integer)  AS STRING)       AS periodo_cierre, 
  CAST(fecha_cierre                                                 AS STRING)       AS fecha_cierre, 
  CAST(tipo_proceso                                                 AS STRING)       AS tipo_proceso, 
  CAST((substring(rut_cliente,2,8))                                 AS STRING)       AS rut_cliente, 
  CAST((substring(dv_cliente,10,1))                                 AS STRING)       AS dv_rut_cliente, 
  CAST(tipo_operacion                                               AS STRING)       AS tipo_operacion, 
  CAST(operacion                                                    AS STRING)       AS operacion, 
  CAST(cod_sistema                                                  AS STRING)       AS sistema, 
  CAST(cod_segmento                                                 AS STRING)       AS segmento, 
  CAST(IFNULL(cast(cod_motivo_cartdet as integer),0)                AS STRING)       AS criterio_entrada,
  CAST(IFNULL(cast(cod_cartdet_cliente as integer),0)               AS STRING)       AS origen_deterioro,
  CAST(IFNULL(cast(fecha_cartdet_ope as integer),0)                 AS STRING)       AS fecha_entrada   
FROM
  riesgobdu_silver_db.tbl_d00_segmentado_gnz 
WHERE
    fecha_cierre = {FechaX}  
AND periodo_cierre = {PeriodoX}
/* AND tipo_proceso = '{tipo_procesoX}'  */
AND IFNULL(cast(cod_motivo_cartdet as integer),0)  IN {p_GruCritDetEnt}  
AND ind_cartdet='D'
""" 

# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --SELECT count(1),fecha_cierre,cod_motivo_cartdet FROM riesgobdu_golden_db.tbl_segmentacion_d00_segmentado WHERE  ind_cartdet='D' GROUP BY fecha_cierre,cod_motivo_cartdet
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_cartdet_grupal_stock_prd 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla Temporal Resultado Universo
# MAGIC ---
# MAGIC * marca operaciones para determinar las diferencias de registros entre los dos conjuntos de datos

# COMMAND ----------

paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_cartdet_grupal_universo AS 
SELECT
    COALESCE(A.periodo_cierre, B.periodo_cierre)                                            AS periodo_cierre,
    COALESCE(A.fecha_cierre, B.fecha_cierre)                                                AS fecha_cierre,
    COALESCE(A.tipo_proceso, B.tipo_proceso)                                                AS tipo_proceso,
    COALESCE(A.rut_cliente, B.rut_cliente)                                                  AS rut_cliente,
	COALESCE(A.sistema,B.sistema) 												            AS sistema,
	COALESCE(A.operacion,B.operacion) 												        AS operacion,
    COALESCE(A.tipo_operacion, B.tipo_operacion)                                            AS tipo_operacion,
	CASE WHEN A.operacion IS NOT NULL AND B.operacion IS NOT NULL THEN 1 ELSE 0 END 	    AS GRU_OPE_MATCH,
	CASE WHEN A.operacion IS NULL THEN 1 ELSE 0 END							 			    AS GRU_OPE_NOEN_NEW,
	CASE WHEN B.operacion IS NULL THEN 1 ELSE 0 END 										AS GRU_OPE_NOEN_PRD,
	CASE WHEN A.operacion IS NOT NULL THEN 1 ELSE 0 END 									AS GRU_OPE_NEW,
	CASE WHEN B.operacion IS NOT NULL THEN 1 ELSE 0 END 									AS GRU_OPE_PRD
FROM
  tmp_EXT_tbl_cartdet_grupal_stock_Nuevos A
FULL JOIN  
  tmp_EXT_tbl_cartdet_grupal_stock_prd B
ON
    TRIM(A.operacion) = TRIM(B.operacion)
AND TRIM(A.sistema) = TRIM(B.sistema)
""" 

# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Resumen Cuadratura Universo

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   sum(GRU_OPE_MATCH) AS total_MATCH
# MAGIC   ,sum(GRU_OPE_NOEN_NEW) AS total_NOEN_NEW
# MAGIC   ,sum(GRU_OPE_NOEN_PRD) AS total_NOEN_PRD
# MAGIC   ,sum(GRU_OPE_NEW) AS total_OPE_NEW
# MAGIC   ,sum(GRU_OPE_PRD) AS total_OPE_PRD
# MAGIC   ,sum(case when GRU_OPE_MATCH = 0 then 1 else 0 END) AS total_diferencia
# MAGIC from
# MAGIC   tmp_RES_cartdet_grupal_universo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla Temporal Valida Campos 
# MAGIC ---
# MAGIC * valida campos de operaciones que hacen match entre version new y prd

# COMMAND ----------

paso_query40 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_cartdet_grupal_campo1 AS 
SELECT
  U.periodo_cierre                                                                   AS periodo_cierre,
  U.fecha_cierre                                                                     AS fecha_cierre,
  U.tipo_proceso                                                                     AS tipo_proceso,
  U.rut_cliente                                                                      AS rut_cliente,
  U.sistema                                                                          AS sistema,
  U.operacion                                                                        AS operacion,
  'rut_cliente'                                                                      AS NAME_FIELD,
  A.rut_cliente                                                                      AS VAL_NEW,
  B.rut_cliente                                                                      AS VAL_PRD,
  CASE WHEN COALESCE(A.rut_cliente,'')<>COALESCE(B.rut_cliente,'') THEN 1 ELSE 0 END AS IND_DIF_1

FROM
    tmp_RES_cartdet_grupal_universo U
LEFT JOIN
  tmp_EXT_tbl_cartdet_grupal_stock_Nuevos A ON U.sistema = A.sistema AND U.operacion = A.operacion
LEFT JOIN  
  tmp_EXT_tbl_cartdet_grupal_stock_prd B ON U.sistema = B.sistema AND U.operacion = B.operacion
WHERE
  U.GRU_OPE_MATCH=1
""" 

# COMMAND ----------

sqlSafe(paso_query40) 

# COMMAND ----------

paso_query41 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_cartdet_grupal_campo2 AS 
SELECT
  U.periodo_cierre                                                                         AS periodo_cierre,
  U.fecha_cierre                                                                           AS fecha_cierre,
  U.tipo_proceso                                                                           AS tipo_proceso,
  U.rut_cliente                                                                            AS rut_cliente,
  U.sistema                                                                                AS sistema,
  U.operacion                                                                              AS operacion,
  'dv_rut_cliente'                                                                         AS NAME_FIELD,
  A.dv_rut_cliente                                                                         AS VAL_NEW,
  B.dv_rut_cliente                                                                         AS VAL_PRD,
  CASE WHEN COALESCE(A.dv_rut_cliente,'')<>COALESCE(B.dv_rut_cliente,'') THEN 1 ELSE 0 END AS IND_DIF_1

FROM
    tmp_RES_cartdet_grupal_universo U
LEFT JOIN
  tmp_EXT_tbl_cartdet_grupal_stock_Nuevos A ON U.sistema = A.sistema AND U.operacion = A.operacion
LEFT JOIN  
  tmp_EXT_tbl_cartdet_grupal_stock_prd B ON U.sistema = B.sistema AND U.operacion = B.operacion
WHERE
  U.GRU_OPE_MATCH=1
""" 

# COMMAND ----------

sqlSafe(paso_query41) 

# COMMAND ----------

paso_query42 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_cartdet_grupal_campo3 AS 
SELECT
  U.periodo_cierre                                                                         AS periodo_cierre,
  U.fecha_cierre                                                                           AS fecha_cierre,
  U.tipo_proceso                                                                           AS tipo_proceso,
  U.rut_cliente                                                                            AS rut_cliente,
  U.sistema                                                                                AS sistema,
  U.operacion                                                                              AS operacion,
  'tipo_operacion'                                                                         AS NAME_FIELD,
  A.tipo_operacion                                                                         AS VAL_NEW,
  B.tipo_operacion                                                                         AS VAL_PRD,
  CASE WHEN COALESCE(A.tipo_operacion,'')<>COALESCE(B.tipo_operacion,'') THEN 1 ELSE 0 END AS IND_DIF_1

FROM
    tmp_RES_cartdet_grupal_universo U
LEFT JOIN
  tmp_EXT_tbl_cartdet_grupal_stock_Nuevos A ON U.sistema = A.sistema AND U.operacion = A.operacion
LEFT JOIN  
  tmp_EXT_tbl_cartdet_grupal_stock_prd B ON U.sistema = B.sistema AND U.operacion = B.operacion
WHERE
  U.GRU_OPE_MATCH=1
""" 

# COMMAND ----------

sqlSafe(paso_query42) 

# COMMAND ----------

paso_query43 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_cartdet_grupal_campo4 AS 
SELECT
  U.periodo_cierre                                                             AS periodo_cierre,
  U.fecha_cierre                                                               AS fecha_cierre,
  U.tipo_proceso                                                               AS tipo_proceso,
  U.rut_cliente                                                                AS rut_cliente,
  U.sistema                                                                    AS sistema,
  U.operacion                                                                  AS operacion,
  'segmento'                                                                   AS NAME_FIELD,
  A.segmento                                                                   AS VAL_NEW,
  B.segmento                                                                   AS VAL_PRD,
  CASE WHEN COALESCE(A.segmento,'')<>COALESCE(B.segmento,'') THEN 1 ELSE 0 END AS IND_DIF_1

FROM
    tmp_RES_cartdet_grupal_universo U
LEFT JOIN
  tmp_EXT_tbl_cartdet_grupal_stock_Nuevos A ON U.sistema = A.sistema AND U.operacion = A.operacion
LEFT JOIN  
  tmp_EXT_tbl_cartdet_grupal_stock_prd B ON U.sistema = B.sistema AND U.operacion = B.operacion
WHERE
  U.GRU_OPE_MATCH=1
""" 

# COMMAND ----------

sqlSafe(paso_query43) 

# COMMAND ----------

paso_query44 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_cartdet_grupal_campo5 AS 
SELECT
  U.periodo_cierre                                                                             AS periodo_cierre,
  U.fecha_cierre                                                                               AS fecha_cierre,
  U.tipo_proceso                                                                               AS tipo_proceso,
  U.rut_cliente                                                                                AS rut_cliente,
  U.sistema                                                                                    AS sistema,
  U.operacion                                                                                  AS operacion,
  'criterio_entrada'                                                                           AS NAME_FIELD,
  A.criterio_entrada                                                                           AS VAL_NEW,
  B.criterio_entrada                                                                           AS VAL_PRD,
  CASE WHEN COALESCE(A.criterio_entrada,'')<>COALESCE(B.criterio_entrada,'') THEN 1 ELSE 0 END AS IND_DIF_1

FROM
    tmp_RES_cartdet_grupal_universo U
LEFT JOIN
  tmp_EXT_tbl_cartdet_grupal_stock_Nuevos A ON U.sistema = A.sistema AND U.operacion = A.operacion
LEFT JOIN  
  tmp_EXT_tbl_cartdet_grupal_stock_prd B ON U.sistema = B.sistema AND U.operacion = B.operacion
WHERE
  U.GRU_OPE_MATCH=1
""" 

# COMMAND ----------

sqlSafe(paso_query44) 

# COMMAND ----------

paso_query45 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_cartdet_grupal_campo6 AS 
SELECT
  U.periodo_cierre                                                                             AS periodo_cierre,
  U.fecha_cierre                                                                               AS fecha_cierre,
  U.tipo_proceso                                                                               AS tipo_proceso,
  U.rut_cliente                                                                                AS rut_cliente,
  U.sistema                                                                                    AS sistema,
  U.operacion                                                                                  AS operacion,
  'origen_deterioro'                                                                           AS NAME_FIELD,
  A.origen_deterioro                                                                           AS VAL_NEW,
  B.origen_deterioro                                                                           AS VAL_PRD,
  CASE WHEN COALESCE(A.origen_deterioro,'')<>COALESCE(B.origen_deterioro,'') THEN 1 ELSE 0 END AS IND_DIF_1  

FROM
    tmp_RES_cartdet_grupal_universo U
LEFT JOIN
  tmp_EXT_tbl_cartdet_grupal_stock_Nuevos A ON U.sistema = A.sistema AND U.operacion = A.operacion
LEFT JOIN  
  tmp_EXT_tbl_cartdet_grupal_stock_prd B ON U.sistema = B.sistema AND U.operacion = B.operacion
WHERE
  U.GRU_OPE_MATCH=1
""" 

# COMMAND ----------

sqlSafe(paso_query45) 

# COMMAND ----------

paso_query46 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_cartdet_grupal_campo7 AS 
SELECT
  U.periodo_cierre                                                                       AS periodo_cierre,
  U.fecha_cierre                                                                         AS fecha_cierre,
  U.tipo_proceso                                                                         AS tipo_proceso,
  U.rut_cliente                                                                          AS rut_cliente,
  U.sistema                                                                              AS sistema,
  U.operacion                                                                            AS operacion,
  'fecha_entrada'                                                                        AS NAME_FIELD,
  A.fecha_entrada                                                                        AS VAL_NEW,
  B.fecha_entrada                                                                        AS VAL_PRD,
  CASE WHEN COALESCE(A.fecha_entrada,'')<>COALESCE(B.fecha_entrada,'') THEN 1 ELSE 0 END AS IND_DIF_1
FROM
    tmp_RES_cartdet_grupal_universo U
LEFT JOIN
  tmp_EXT_tbl_cartdet_grupal_stock_Nuevos A ON U.sistema = A.sistema AND U.operacion = A.operacion
LEFT JOIN  
  tmp_EXT_tbl_cartdet_grupal_stock_prd B ON U.sistema = B.sistema AND U.operacion = B.operacion
WHERE
  U.GRU_OPE_MATCH=1
""" 

# COMMAND ----------

sqlSafe(paso_query46) 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE tmp_RES_cartdet_grupal_campo6 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Tablas de Salidas
# MAGIC --------------------------------------
# MAGIC * carga resultados a tablas de salidas del notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Tabla Validacion Universo (riesgobdu_silver_db.tbl_cartdet_grupal_universo_val)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos)

# COMMAND ----------

paso_query47 = f"""DELETE FROM {base_silverX}.tbl_cartdet_grupal_universo_val where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} and tipo_proceso = '{tipo_procesoX}' """


# COMMAND ----------

sqlSafe(paso_query47)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *  FROM riesgobdu_silver_db.tbl_cartdet_grupal_universo_val

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query48 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_grupal_universo_val
SELECT 
 periodo_cierre	 	 
,fecha_cierre	 	 
,tipo_proceso	 	 
,rut_cliente	 	 
,sistema	 	 
,operacion	 	 
,tipo_operacion	 	 
,GRU_OPE_MATCH	 	 
,GRU_OPE_NOEN_NEW	 	 
,GRU_OPE_NOEN_PRD	 	 
,GRU_OPE_NEW	 	 
,GRU_OPE_PRD	 	 
FROM
  tmp_RES_cartdet_grupal_universo
"""  

# COMMAND ----------

sqlSafe(paso_query48)

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC --ESTADISTICAS
# MAGIC select periodo_cierre, GRU_OPE_MATCH, count(*) from riesgobdu_silver_db.tbl_cartdet_grupal_universo_val group by 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Tabla Validacion Campos (riesgobdu_silver_db.tbl_cartdet_grupal_campo_val)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos)

# COMMAND ----------

paso_query400 = f"""DELETE FROM {base_silverX}.tbl_cartdet_grupal_campo_val where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} and tipo_proceso = '{tipo_procesoX}' """


# COMMAND ----------

sqlSafe(paso_query400)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_grupal_campo_val 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query430 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_grupal_campo_val
SELECT * FROM tmp_RES_cartdet_grupal_campo1 
UNION
SELECT * FROM tmp_RES_cartdet_grupal_campo2
UNION
SELECT * FROM tmp_RES_cartdet_grupal_campo3 
UNION
SELECT * FROM tmp_RES_cartdet_grupal_campo4 
UNION
SELECT * FROM tmp_RES_cartdet_grupal_campo5 
UNION
SELECT * FROM tmp_RES_cartdet_grupal_campo6 
UNION
SELECT * FROM tmp_RES_cartdet_grupal_campo7 
"""  

# COMMAND ----------

sqlSafe(paso_query430)

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
# MAGIC --ESTADISTICAS
# MAGIC 
# MAGIC select NAME_FIELD, GRU_DIF_1, count(*) from riesgobdu_silver_db.tbl_cartdet_grupal_campo_val  GROUP BY 1,2 ORDER BY NAME_FIELD

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1),VAL_NEW,VAL_PRD from riesgobdu_silver_db.tbl_cartdet_grupal_campo_val where NAME_FIELD = 'criterio_entrada' and GRU_DIF_1 = 1 group by VAL_NEW,VAL_PRD
# MAGIC 
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_grupal_campo_val where VAL_PRD in (11,12,13) 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_grupal_campo_val where NAME_FIELD = 'criterio_entrada' and  GRU_DIF_1 = 1
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_grupal_campo_val where NAME_FIELD = 'origen_deterioro' and  GRU_DIF_1 = 0
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo where rut_cliente = 12254846
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit  where rut_cliente = 12254846
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin where rut_cliente = 12254846
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_d00_segmentado_gnz  where CAST(substring(rut_cliente,1,9) AS int) = 12254846  --  ind_cartdet = 'D'
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_d00_segmentado_gnz  where rut_cliente like '%12254846%'   --  ind_cartdet = 'D'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_grupal_campo_val where rut_cliente like '%13883811%'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC /* select count(1),criterio_entrada from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo group by criterio_entrada */
# MAGIC /* delete from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo where criterio_entrada in (7,8,9,10,11,12,13) */ 
# MAGIC 
# MAGIC /* select distinct criterio_entrada from  riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit */
# MAGIC /* delete from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where criterio_entrada in (7,8,9,10,11,12,13) */ 
# MAGIC 
# MAGIC /* select distinct criterio_entrada from  riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin */
# MAGIC /* delete from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin where criterio_entrada in (7,8,9,10,11,12,13) */
# MAGIC 
# MAGIC /*  select distinct criterio_salida from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo  */
# MAGIC /* select distinct criterio_salida from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo where criterio_salida not in (2,3) */
# MAGIC /* delete  from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo where criterio_salida not in (2,3) */
# MAGIC 
# MAGIC 
# MAGIC /* select distinct criterio_salida from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit  */
# MAGIC /* select count(1),criterio_salida from  riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_prin group by criterio_salida */
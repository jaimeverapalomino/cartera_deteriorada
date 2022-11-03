# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: Criterio de Entrada Deterioro Segmento Grupal
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_09_CarDet_Gru_Crit_Ent_9.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 26/08/2022
# MAGIC * Descripcion: Cliente con alguna operación de crédito Renegociado. Criterio 9.
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
# MAGIC * riesgobdu_golden_db.tbl_segmentacion_d00_segmentado
# MAGIC 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo
# MAGIC 
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios
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
# MAGIC ### Extrae Criterios de Deterioro Entrada Grupal

# COMMAND ----------

p_GruCritDetEnt = 9
p_fecOtor = 20090101

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operaciones D00 (tbl_segmentacion_d00_segmentado)
# MAGIC --------------------------------------
# MAGIC - Se extrae operaciones para periodo actual

# COMMAND ----------

#Extrae las operaciones de la tabla riesgobdu_golden_db.tbl_segmentacion_d00_segmentado para el periodo actual
paso_query10 = f"""
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
  b.dias_mora                           AS dias_mora,
  b.tipo_operacion                      AS tipo_operacion,
  b.fecha_otorgamiento                  AS fecha_otorgamiento,
  b.cod_renegociado                     AS cod_renegociado
FROM
  {base_goldX}.tbl_segmentacion_d00_segmentado b
WHERE 
    b.fecha_cierre =  {FechaX}
AND b.periodo_cierre = {PeriodoX}
AND b.tipo_proceso = '{tipo_procesoX}'  
"""


# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

#Extrae las operaciones Gruaples 

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_GRU_ACT AS
SELECT 
  cast(substr(cast(b.periodo_cierre as string),1,6) as integer)          AS periodo_cierre,  /*toda esta transformacion es temporal*/
  b.fecha_cierre                        AS fecha_cierre,
  b.tipo_proceso                        AS tipo_proceso,
  b.cod_segmento                        AS cod_segmento,
  b.operacion                           AS operacion,
  b.cod_sistema                         AS cod_sistema,
  b.rut_cliente                         AS rut_cliente,
  b.dv_cliente                          AS dv_cliente,
  b.tipo_operacion                      AS tipo_operacion,
  b.dias_mora                           AS dias_mora,
  b.fecha_otorgamiento                  AS fecha_otorgamiento,
  b.cod_renegociado                     AS cod_renegociado
FROM
  tmp_EXT_tbl_segmentacion_d00_segmentado b
WHERE 
  trim(b.cod_segmento) = 'G'
  and fecha_otorgamiento >= '{p_fecOtor}'

"""


# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

#Extrae las operaciones de la tabla riesgobdu_golden_db.tbl_segmentacion_d00_segmentado para el periodo anterior
paso_query100 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado_ant AS
SELECT 
  b.periodo_cierre                      AS periodo_cierre,
  b.fecha_cierre                        AS fecha_cierre,
  b.tipo_proceso                        AS tipo_proceso,
  b.cod_segmento                        AS cod_segmento,
  b.operacion                           AS operacion,
  b.cod_sistema                         AS cod_sistema,
  b.rut_cliente                         AS rut_cliente,
  b.dv_cliente                          AS dv_cliente,
  b.tipo_operacion                      AS tipo_operacion,
  b.dias_mora                           AS dias_mora,
  b.fecha_otorgamiento                  AS fecha_otorgamiento,
  b.cod_renegociado                     AS cod_renegociado
FROM
  {base_goldX}.tbl_segmentacion_d00_segmentado b
WHERE 
  b.fecha_cierre = '20220531'
"""



# COMMAND ----------

sqlSafe(paso_query100)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla con operaciones que cumplen y no cumplen condicion deterioro del periodo actual

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crédito Renegociado

# COMMAND ----------

#Tabla con operaciones que cumplen y no cumplen condicion deterioro del periodo actual
paso_query30 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL as
SELECT
 A.periodo_cierre                     AS periodo_cierre,
 A.fecha_cierre                       AS fecha_cierre,
 A.tipo_proceso                       AS tipo_proceso, 
 A.cod_segmento                       AS cod_segmento,
 A.operacion                          AS operacion,
 A.cod_sistema                        AS cod_sistema,
 A.rut_cliente                        AS rut_cliente,
 A.dv_cliente                         AS dv_cliente,
 A.tipo_operacion                     AS tipo_operacion,
 A.cod_renegociado                    AS cod_renegociado,
'cod_renegociado'                     AS nombre_campo,
 cast(A.cod_renegociado as string)    AS valor_campo,
 " = "                                AS condicion_regla,     
 "S,C"                                AS valor_regla,           /* Este valor debe ser parametrico */
 CASE WHEN trim (A.cod_renegociado) in ('S','C') THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_RES_D00_OPE_GRU_ACT A
"""


# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal (tmp_tbl_cartdet_crit_ent_ope_campo)

# COMMAND ----------

#Crea tabla temporal final con la evaluacion por campo de cada criterio de evaluacion
paso_query80 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_ent_ope_campo AS
SELECT
	 IFNULL(A.periodo_cierre,190001)       AS periodo_cierre    
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_cierre 
	,IFNULL(A.tipo_proceso,' ')            AS tipo_proceso 
	,IFNULL(A.rut_cliente,0)               AS rut_cliente 
	,IFNULL(A.dv_cliente,' ')              AS dv_rut_cliente 
	,IFNULL(A.tipo_operacion,' ')          AS tipo_operacion
	,IFNULL(A.operacion,' ')               AS operacion 
	,IFNULL(A.cod_sistema,' ')             AS sistema   
	,IFNULL(A.cod_segmento,' ')            AS segmento 
	,'9'                                   AS criterio_entrada 
	,'3'                                   AS origen_deterioro 
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_entrada 
	,IFNULL(A.nombre_campo,' ')            AS nombre_campo
	,IFNULL(A.valor_campo,' ')             AS valor_campo 
	,IFNULL(A.condicion_regla,' ')         AS condicion_regla  
	,IFNULL(A.valor_regla,' ')             AS valor_regla
	,IFNULL(A.flag_resultado_regla,0)      AS flag_resultado_regla
FROM
  tmp_RES_D00_OPE_CAMPO_EVAL A
"""  

# COMMAND ----------

sqlSafe(paso_query80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borrado e insercion en Tabla (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo)

# COMMAND ----------

paso_query90 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_campo where fecha_cierre = {FechaX} AND criterio_entrada = {p_GruCritDetEnt} AND tipo_proceso = '{tipo_procesoX}' """

# COMMAND ----------

sqlSafe(paso_query90)

# COMMAND ----------


paso_query100 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_ent_ope_campo
SELECT 
  periodo_cierre    
  ,fecha_cierre 
  ,tipo_proceso 
  ,rut_cliente 
  ,dv_rut_cliente
  ,tipo_operacion
  ,operacion 
  ,sistema   
  ,segmento 
  ,criterio_entrada 
  ,origen_deterioro 
  ,fecha_entrada 
  ,nombre_campo
  ,valor_campo 
  ,condicion_regla  
  ,valor_regla
  ,flag_resultado_regla
FROM
  tmp_tbl_cartdet_crit_ent_ope_campo
"""  


# COMMAND ----------

sqlSafe(paso_query100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1), criterio_entrada from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo where flag_resultado_regla = 1 group by criterio_entrada
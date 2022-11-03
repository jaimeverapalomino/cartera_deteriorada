# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: Criterio de Salida Deterioro Segmento Individual
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_04_CarDet_Ind_Crit_Sal.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 17/08/2022
# MAGIC * Descripcion: Clientes con calificación de riesgo considerada de salida deterioro. Criterio 2.
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
# MAGIC * tbl_segmentacion_clientes
# MAGIC * tbl_segmentacion_cliente_consolidado
# MAGIC * tbl_D00_extendido
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios
# MAGIC ********************************************************************************************************
# MAGIC ##### Pendientes:
# MAGIC * confirmar campo de segmento en tabla tbl_cartdet_clientes.
# MAGIC * Consulta (Jonatan), en el caso de que el cliente sea Ind y tenga operaciones grupales e ind. ¿Que algoritmo se usa para deteriorar?, se debe deteriorar   dependiendo de la segmentacion del cliente o de la segmentacion de la operación.
# MAGIC * Ver como programar la fecha de suspencion devengo.
# MAGIC * Momentaneamente se esta apuntando a periodo_cierre = 202206100
# MAGIC * filtros para {base_silverX}.tbl_cartdet_crit_sal_ope_campo
# MAGIC * Crear en la tabla de paramtero una variable que contengan todas las calificaciones posible (1... 16)

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
# MAGIC ### Extrae Calificaciones de Deterioro Individuales

# COMMAND ----------

calificaciones = obtieneParametro('SEGMENTACION', 'CAL_DET')

param_cal = calificaciones.split(',')
param_cal = ','.join([f"'{i}'" for i in param_cal])
param_cal

# COMMAND ----------

p_IndCritDetEnt = 1
p_Clasificaciones = 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
p_Excepcion = 'HIP', 'CAE'
p_IndCritDetSal = 2,3,30,50


p_fecha_cierre_ant=20220531
p_periodo_cierre_ant=202205     
p_tipo_proceso_ant ='C'


# COMMAND ----------

print(f"calificaciones: {calificaciones}")
print(f"p_IndCritDetEnt: {p_IndCritDetEnt}")
print(f"p_Clasificaciones: {p_Clasificaciones}")
print(f"p_Excepcion: {p_Excepcion}")
print(f"p_IndCritDetSal: {p_IndCritDetSal}")

print(f"p_fecha_cierre_ant: {p_fecha_cierre_ant}")
print(f"p_periodo_cierre_ant: {p_periodo_cierre_ant}")
print(f"p_tipo_proceso_ant: {p_tipo_proceso_ant}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones Deterioradas Individuales (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)
# MAGIC --------------------------------------
# MAGIC - Se extrae Operaciones Deterioradas Individual periodo actual, para evaluar salida

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_ope_prin as
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
  ,criterio_entrada   AS criterio_entrada
  ,origen_deterioro   AS origen_deterioro
  ,fecha_entrada      AS fecha_entrada
FROM
    riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin
WHERE 
    fecha_cierre = {FechaX}  
AND periodo_cierre = {PeriodoX}  
AND tipo_proceso = '{tipo_procesoX}'
AND criterio_entrada  IN ({p_IndCritDetEnt})  
"""

# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Datos Clientes  Periodo Actual (tbl_segmentacion_cliente_consolidado)
# MAGIC --------------------------------------
# MAGIC - Se extrae clientes para el periodo actual con la clasificacion bci desde la tabla tbl_segmentacion_cliente_consolidado 

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_cliente_consolidado as
SELECT 
  periodo_cierre                      AS periodo_cierre,
  fecha_cierre                        AS fecha_cierre,
  tipo_proceso                        AS tipo_proceso,
  rut_cliente                         AS rut_cliente,
  dv_cliente                          AS dv_rut_cliente, 
  calificacion_bci                    AS calificacion_bci,
  calificacion_regulador              AS calificacion_regulador,    
  fecha_calificacion                  as fecha_calificacion,
  cic_cliente                         AS cic_cliente
FROM
  riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado
WHERE 
    fecha_cierre = {FechaX}  
AND periodo_cierre = {PeriodoX}  
AND tipo_proceso = '{tipo_procesoX}'
"""


# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solo clientes con clasificacion valida Bci
# MAGIC --------------------------------------
# MAGIC * Se extrae clientes para el periodo actual con la clasificacion bci validas. 
# MAGIC * Se usara para distingir los clientes que salen de deterioro por clasificación o porque no tiene clasificación.

# COMMAND ----------

paso_query25 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cliente_con_calificacion as
SELECT 
  periodo_cierre                      AS periodo_cierre,
  fecha_cierre                        AS fecha_cierre,
  tipo_proceso                        AS tipo_proceso,
  rut_cliente                         AS rut_cliente,
  dv_rut_cliente                      AS dv_rut_cliente, 
  calificacion_bci                    AS calificacion_bci,
  calificacion_regulador              AS calificacion_regulador,    
  fecha_calificacion                  as fecha_calificacion,
  cic_cliente                         AS cic_cliente
FROM
  tmp_EXT_tbl_segmentacion_cliente_consolidado
WHERE 
  cast(calificacion_bci as integer) in {p_Clasificaciones}
 """

# COMMAND ----------

sqlSafe(paso_query25)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion salida clientes con clasificacion 
# MAGIC ----------------------
# MAGIC * Marca operaciones de clientes que salen de deterioro por tener una clasificacion no deteriorada

# COMMAND ----------

paso_query30 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL as
SELECT
 A.periodo_cierre,
 A.fecha_cierre,
 A.tipo_proceso,
 A.segmento,
 A.tipo_operacion,
 A.operacion,
 A.sistema,
 A.rut_cliente,
 A.dv_rut_cliente ,
'calificacion_bci' AS nombre_campo,
 cast(B.calificacion_bci as string) AS valor_campo,
 " NOT IN "  AS condicion_regla,     
 "{param_cal}"  AS valor_regla,
 CASE WHEN trim (B.calificacion_bci) NOT IN ({param_cal}) THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_ope_prin A
LEFT JOIN
    tmp_RES_cliente_con_calificacion B 
ON A.rut_cliente = B.rut_cliente
"""


# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tmp_RES_D00_OPE_CAMPO_EVAL where flag_resultado_regla = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones Periodo Actual (riesgobdu_golden_db.tbl_segmentacion_d00_segmentado)
# MAGIC ----------------------
# MAGIC * Obtiene operaciones con el objetivo de distinguir entre operaciones que salen porque no vienen informadas o cambian de segmento

# COMMAND ----------

#Extrae las operaciones de la tabla riesgobdu_golden_db.tbl_segmentacion_d00_segmentado para el periodo actual
paso_query40 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado AS
SELECT 
  b.periodo_cierre                      AS periodo_cierre,
  b.fecha_cierre                        AS fecha_cierre,
  b.tipo_proceso                        AS tipo_proceso,
  b.cod_segmento                        AS segmento,
  b.tipo_operacion                      AS tipo_operacion,
  b.operacion                           AS operacion,
  b.cod_sistema                         AS sistema,
  b.rut_cliente                         AS rut_cliente,
  b.dv_cliente                          AS dv_cliente
FROM
  riesgobdu_golden_db.tbl_segmentacion_d00_segmentado b
WHERE 
    b.fecha_cierre = {FechaX}  
AND b.periodo_cierre = {PeriodoX}  
AND b.tipo_proceso = '{tipo_procesoX}'
"""


# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion salida operaciones no informadas en periodo actual
# MAGIC ----------------------
# MAGIC * Evalua si las operaciones deterioradas estan informadas en periodo actual

# COMMAND ----------

paso_query50 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_2 as
SELECT
 A.periodo_cierre,
 A.fecha_cierre,
 A.tipo_proceso,
 A.segmento,
 A.tipo_operacion,
 A.operacion,
 A.sistema,
 A.rut_cliente,
 A.dv_rut_cliente ,
 'operacion - sistema'                AS nombre_campo,
 concat(A.operacion,'-',A.sistema)    AS valor_campo,
 "IS NULL "                           AS condicion_regla,     
 "NULL"                               AS valor_regla,
 CASE WHEN trim (B.operacion) IS NULL THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_ope_prin A
LEFT JOIN
    tmp_EXT_tbl_segmentacion_d00_segmentado B 
ON  A.operacion = B.operacion
AND A.sistema = B.sistema
"""

# COMMAND ----------

sqlSafe(paso_query50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones Periodo Anterior (riesgobdu_golden_db.tbl_segmentacion_d00_segmentado)
# MAGIC ----------------------
# MAGIC * Obtiene operaciones periodo anterior con el objetivo de distinguir entre operaciones que salen porque no vienen informadas o cambian de segmento

# COMMAND ----------

#Extrae operaciones  periodo anterior
paso_query55 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado_pant as
SELECT
	 periodo_cierre                                        as periodo_cierre
	,fecha_cierre                                          as fecha_cierre
	,tipo_proceso                                          as tipo_proceso
	,rut_cliente                                           as rut_cliente
	,dv_cliente                                            as dv_cliente
    ,tipo_operacion                                        as tipo_operacion
	,operacion                                             as operacion
	,cod_sistema                                           as sistema
	,cod_segmento                                          as segmento
    ,IFNULL(cast(cod_cartdet_cliente as integer),0)        as origen_deterioro
	,IFNULL(cast(cod_motivo_cartdet as integer),0)         as criterio_entrada
	,IFNULL(cast(fecha_cartdet_ope as integer),0)          as fecha_entrada
    ,ind_cartdet                                           as ind_cartdet
FROM 
	riesgobdu_golden_db.tbl_segmentacion_d00_segmentado
WHERE
     fecha_cierre={p_fecha_cierre_ant}
and  periodo_cierre={p_periodo_cierre_ant}     
and  tipo_proceso = '{p_tipo_proceso_ant}'
"""

# COMMAND ----------

sqlSafe(paso_query55)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion salida operaciones que cambian de segmento
# MAGIC ----------------------
# MAGIC * Evalua si las operaciones deterioradas en periodo actual y tuvo cambio de segmento

# COMMAND ----------


paso_query60 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_3 as
SELECT
 A.periodo_cierre,
 A.fecha_cierre,
 A.tipo_proceso,
 A.segmento,
 A.tipo_operacion,
 A.operacion,
 A.sistema,
 A.rut_cliente,
 A.dv_rut_cliente ,
 'segmento'                                AS nombre_campo,
 A.segmento                                AS valor_campo,
 " <> "                                    AS condicion_regla,     
 B.segmento                                AS valor_regla,
 CASE WHEN B.operacion is not null and trim (B.segmento) <> trim (A.segmento) THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_ope_prin A
LEFT JOIN
    tmp_EXT_tbl_segmentacion_d00_segmentado_pant B 
ON A.operacion = B.operacion
AND A.sistema = B.sistema
"""

# COMMAND ----------

sqlSafe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion salida clientes sin clasificacion
# MAGIC ----------------------
# MAGIC * Evalua si las operaciones deterioradas en periodo actual cuyos clientes no tienen clasificacion

# COMMAND ----------

paso_query65 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_4 as
SELECT
 A.periodo_cierre,
 A.fecha_cierre,
 A.tipo_proceso,
 A.segmento,
 A.tipo_operacion,
 A.operacion,
 A.sistema,
 A.rut_cliente,
 A.dv_rut_cliente ,
'calificacion_bci'                         AS nombre_campo,
 cast(B.calificacion_bci as string)        AS valor_campo,
 " NOT IN "                                AS condicion_regla,     
 '{p_Clasificaciones}'       AS valor_regla,
 CASE WHEN B.rut_cliente is not null AND cast(B.calificacion_bci as integer) NOT IN {p_Clasificaciones} THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_ope_prin A
LEFT JOIN
    tmp_EXT_tbl_segmentacion_cliente_consolidado B 
ON A.rut_cliente = B.rut_cliente
"""


# COMMAND ----------

sqlSafe(paso_query65)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion salida operaciones por excepcion
# MAGIC ----------------------
# MAGIC * Evalua si las operaciones son excepciones
# MAGIC * Las operaciones que no se deterioran por irradiacion son las hipotecarias vivienda (HIP) y prestamos estudiantiles (CAE)

# COMMAND ----------

paso_query66 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_5 as
SELECT
 A.periodo_cierre,
 A.fecha_cierre,
 A.tipo_proceso,
 A.segmento,
 A.tipo_operacion,
 A.operacion,
 A.sistema,
 A.rut_cliente,
 A.dv_rut_cliente ,
'tipo_operacion'                           AS nombre_campo,
 substr(A.tipo_operacion,1,3)              AS valor_campo,
 " IN "                                    AS condicion_regla,     
 CAST({p_Excepcion}  AS STRING)            AS valor_regla,
 CASE WHEN substr(A.tipo_operacion,1,3) IN {p_Excepcion} THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_ope_prin A
"""


# COMMAND ----------

sqlSafe(paso_query66)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Campo Evaludado (tmp_tbl_cartdet_crit_sal_ope_campo)
# MAGIC ------------------
# MAGIC * generar salida temporal a nivel de campo evaluado. 
# MAGIC * se registran todas las operaciones y condiciones evaluadas

# COMMAND ----------

#evaluacion cliente con clasificacion de salida
paso_query70 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_ope_campo_1 AS
SELECT
	 IFNULL(A.periodo_cierre,190001)       AS periodo_cierre    
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_cierre 
	,IFNULL(A.tipo_proceso,' ')            AS tipo_proceso 
	,IFNULL(A.rut_cliente,0)               AS rut_cliente 
	,IFNULL(A.dv_rut_cliente,' ')          AS dv_rut_cliente 
    ,IFNULL(A.tipo_operacion,' ')          AS tipo_operacion 
	,IFNULL(A.operacion,' ')               AS operacion 
	,IFNULL(A.sistema,' ')                 AS sistema   
	,IFNULL(A.segmento,' ')                AS segmento 
	,2                                     AS criterio_salida 
	,0                                     AS origen_deterioro 
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_salida 
	,IFNULL(A.nombre_campo,' ')            AS nombre_campo
	,IFNULL(A.valor_campo,' ')             AS valor_campo 
	,IFNULL(A.condicion_regla,' ')         AS condicion_regla  
	,IFNULL(A.valor_regla,' ')             AS valor_regla
	,IFNULL(A.flag_resultado_regla,0)      AS flag_resultado_regla
FROM
  tmp_RES_D00_OPE_CAMPO_EVAL A
"""

# COMMAND ----------

sqlSafe(paso_query70)

# COMMAND ----------

#evaluacion operaciones deterioradas mes anterior y que no son informadas en mes actual
paso_query72 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_ope_campo_2 AS
SELECT
	 IFNULL(A.periodo_cierre,190001)       AS periodo_cierre    
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_cierre 
	,IFNULL(A.tipo_proceso,' ')            AS tipo_proceso 
	,IFNULL(A.rut_cliente,0)               AS rut_cliente 
	,IFNULL(A.dv_rut_cliente,' ')          AS dv_rut_cliente 
    ,IFNULL(A.tipo_operacion,' ')          AS tipo_operacion 
	,IFNULL(A.operacion,' ')               AS operacion 
	,IFNULL(A.sistema,' ')                 AS sistema   
	,IFNULL(A.segmento,' ')                AS segmento 
	,30                                    AS criterio_salida 
	,0                                     AS origen_deterioro 
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_salida 
	,IFNULL(A.nombre_campo,' ')            AS nombre_campo
	,IFNULL(A.valor_campo,' ')             AS valor_campo 
	,IFNULL(A.condicion_regla,' ')         AS condicion_regla  
	,IFNULL(A.valor_regla,' ')             AS valor_regla
	,IFNULL(A.flag_resultado_regla,0)      AS flag_resultado_regla
FROM
  tmp_RES_D00_OPE_CAMPO_EVAL_2 A
""" 

# COMMAND ----------

sqlSafe(paso_query72)

# COMMAND ----------

#evaluacion operaciones deterioradas mes anterior como individual y que se informan como grupal en mes actual
paso_query74 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_ope_campo_3 AS
SELECT
	 IFNULL(A.periodo_cierre,190001)       AS periodo_cierre    
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_cierre 
	,IFNULL(A.tipo_proceso,' ')            AS tipo_proceso 
	,IFNULL(A.rut_cliente,0)               AS rut_cliente 
	,IFNULL(A.dv_rut_cliente,' ')          AS dv_rut_cliente 
    ,IFNULL(A.tipo_operacion,' ')          AS tipo_operacion 
	,IFNULL(A.operacion,' ')               AS operacion 
	,IFNULL(A.sistema,' ')                 AS sistema   
	,IFNULL(A.segmento,' ')                AS segmento 
	,30                                    AS criterio_salida 
	,0                                     AS origen_deterioro 
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_salida 
	,IFNULL(A.nombre_campo,' ')            AS nombre_campo
	,IFNULL(A.valor_campo,' ')             AS valor_campo 
	,IFNULL(A.condicion_regla,' ')         AS condicion_regla  
	,IFNULL(A.valor_regla,' ')             AS valor_regla
	,IFNULL(A.flag_resultado_regla,0)      AS flag_resultado_regla
FROM
  tmp_RES_D00_OPE_CAMPO_EVAL_3 A
""" 

# COMMAND ----------

sqlSafe(paso_query74)

# COMMAND ----------

#evaluacion operaciones deterioradas mes anterior y en mes actual cliente no tiene clasificacion 
paso_query76 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_ope_campo_4 AS
SELECT
	 IFNULL(A.periodo_cierre,190001)       AS periodo_cierre    
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_cierre 
	,IFNULL(A.tipo_proceso,' ')            AS tipo_proceso 
	,IFNULL(A.rut_cliente,0)               AS rut_cliente 
	,IFNULL(A.dv_rut_cliente,' ')          AS dv_rut_cliente 
    ,IFNULL(A.tipo_operacion,' ')          AS tipo_operacion 
	,IFNULL(A.operacion,' ')               AS operacion 
	,IFNULL(A.sistema,' ')                 AS sistema   
	,IFNULL(A.segmento,' ')                AS segmento 
	,3                                     AS criterio_salida 
	,0                                     AS origen_deterioro 
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_salida 
	,IFNULL(A.nombre_campo,' ')            AS nombre_campo
	,IFNULL(A.valor_campo,' ')             AS valor_campo 
	,IFNULL(A.condicion_regla,' ')         AS condicion_regla  
	,IFNULL(A.valor_regla,' ')             AS valor_regla
	,IFNULL(A.flag_resultado_regla,0)      AS flag_resultado_regla
FROM
  tmp_RES_D00_OPE_CAMPO_EVAL_4 A
""" 

# COMMAND ----------

sqlSafe(paso_query76)

# COMMAND ----------

#evaluacion operaciones por excepcion hip y cae
paso_query78 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_ope_campo_5 AS
SELECT
	 IFNULL(A.periodo_cierre,190001)       AS periodo_cierre    
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_cierre 
	,IFNULL(A.tipo_proceso,' ')            AS tipo_proceso 
	,IFNULL(A.rut_cliente,0)               AS rut_cliente 
	,IFNULL(A.dv_rut_cliente,' ')          AS dv_rut_cliente 
    ,IFNULL(A.tipo_operacion,' ')          AS tipo_operacion 
	,IFNULL(A.operacion,' ')               AS operacion 
	,IFNULL(A.sistema,' ')                 AS sistema   
	,IFNULL(A.segmento,' ')                AS segmento 
	,50                                    AS criterio_salida 
	,0                                     AS origen_deterioro 
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_salida 
	,IFNULL(A.nombre_campo,' ')            AS nombre_campo
	,IFNULL(A.valor_campo,' ')             AS valor_campo 
	,IFNULL(A.condicion_regla,' ')         AS condicion_regla  
	,IFNULL(A.valor_regla,' ')             AS valor_regla
	,IFNULL(A.flag_resultado_regla,0)      AS flag_resultado_regla
FROM
  tmp_RES_D00_OPE_CAMPO_EVAL_5 A
""" 

# COMMAND ----------

sqlSafe(paso_query78) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Tablas de Salidas
# MAGIC --------------------------------------
# MAGIC * carga resultados a tablas de salidas del notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Tabla Evaluacion por Campo (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos)

# COMMAND ----------

paso_query300 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_sal_ope_campo where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} and tipo_proceso = '{tipo_procesoX}' and criterio_salida  IN {p_IndCritDetSal } """


# COMMAND ----------

sqlSafe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query310 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_sal_ope_campo
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
  ,criterio_salida 
  ,origen_deterioro 
  ,fecha_salida
  ,nombre_campo
  ,valor_campo 
  ,condicion_regla  
  ,valor_regla
  ,flag_resultado_regla
FROM
  tmp_tbl_cartdet_crit_sal_ope_campo_1
"""  


# COMMAND ----------

sqlSafe(paso_query310)

# COMMAND ----------

paso_query312 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_sal_ope_campo
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
  ,criterio_salida 
  ,origen_deterioro 
  ,fecha_salida
  ,nombre_campo
  ,valor_campo 
  ,condicion_regla  
  ,valor_regla
  ,flag_resultado_regla
FROM
  tmp_tbl_cartdet_crit_sal_ope_campo_2
"""  


# COMMAND ----------

sqlSafe(paso_query312)

# COMMAND ----------

paso_query314 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_sal_ope_campo
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
  ,criterio_salida 
  ,origen_deterioro 
  ,fecha_salida
  ,nombre_campo
  ,valor_campo 
  ,condicion_regla  
  ,valor_regla
  ,flag_resultado_regla
FROM
  tmp_tbl_cartdet_crit_sal_ope_campo_3
"""  

# COMMAND ----------

sqlSafe(paso_query314)

# COMMAND ----------

paso_query316 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_sal_ope_campo
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
  ,criterio_salida 
  ,origen_deterioro 
  ,fecha_salida
  ,nombre_campo
  ,valor_campo 
  ,condicion_regla  
  ,valor_regla
  ,flag_resultado_regla
FROM
  tmp_tbl_cartdet_crit_sal_ope_campo_4
"""  

# COMMAND ----------

sqlSafe(paso_query316)

# COMMAND ----------

paso_query318 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_sal_ope_campo
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
  ,criterio_salida 
  ,origen_deterioro 
  ,fecha_salida
  ,nombre_campo
  ,valor_campo 
  ,condicion_regla  
  ,valor_regla
  ,flag_resultado_regla
FROM
  tmp_tbl_cartdet_crit_sal_ope_campo_5
"""  

# COMMAND ----------

sqlSafe(paso_query318)

# COMMAND ----------

# MAGIC %sql
# MAGIC --ESTADISTICAS
# MAGIC SELECT periodo_cierre, nombre_campo, criterio_salida, flag_resultado_regla, COUNT(*) FROM riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo GROUP BY 1,2,3,4

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo  where rut_cliente=7776581 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")
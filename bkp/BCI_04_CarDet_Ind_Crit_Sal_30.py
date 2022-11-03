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
# MAGIC * Nombre: BCI_04_CarDet_Ind_Crit_Sal_30.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 19/08/2022
# MAGIC * Descripcion: Operaciones NO informadas en D00 Actual o cambia de segmento. Criterio 30.
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
# MAGIC ### Carga Ingestas Temporales

# COMMAND ----------

# MAGIC %run "../segmentacion/BCI_AUX_Carga_Ingesta_Temporal"

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
dbutils.widgets.text("ruta_silverW","","04-Ruta adss Silver:")
dbutils.widgets.text("path_bdW","","05-Ruta adss de la BD Silver:")

FechaX = dbutils.widgets.get("FechaW") 
PeriodoX = dbutils.widgets.get("PeriodoW")
base_silverX = dbutils.widgets.get("bd_silverW")
ruta_silverX = dbutils.widgets.get("ruta_silverW")
bd_ruta_silverX = dbutils.widgets.get("path_bdW")

spark.conf.set("bci.Fecha", FechaX)
spark.conf.set("bci.Periodo", PeriodoX)
spark.conf.set("bci.dbnamesilver", base_silverX)
spark.conf.set("bci.rutasilver", ruta_silverX)
spark.conf.set("bci.bdRutasilver", bd_ruta_silverX)

print(f"Fecha de Proceso: {FechaX}")
print(f"Periodo: {PeriodoX}")      
print(f"Nombre BD Silver: {base_silverX}")
print(f"Ruta abfss Silver: {ruta_silverX}")
print(f"Ruta abfss de la BD Silver: {bd_ruta_silverX}")

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
# MAGIC ### Operaciones Deterioradas (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)
# MAGIC --------------------------------------
# MAGIC - Se extrae Operaciones Deterioradas Individual periodo anterior

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_ope_crit as
SELECT 
  periodo_cierre      AS periodo_cierre
  ,fecha_cierre       AS fecha_cierre
  ,tipo_proceso       AS tipo_proceso 
  ,rut_cliente        AS rut_cliente
  ,dv_rut_cliente     AS dv_rut_cliente
  ,operacion          AS operacion
  ,sistema            AS sistema
  ,segmento           AS segmento
  ,criterio_entrada   AS criterio_entrada
  ,origen_deterioro   AS origen_deterioro
  ,fecha_entrada      AS fecha_entrada
FROM
  riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit
WHERE 
  segmento = 'I'
 AND criterio_entrada = 1
 AND fecha_cierre = 202206100
"""

# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clientes con Calificacion Bci (tbl_segmentacion_cliente_consolidado)
# MAGIC --------------------------------------
# MAGIC - Se extrae clientes para el periodo actual con la clasificacion bci desde la tabla tbl_segmentacion_cliente_consolidado 

# COMMAND ----------

#Extrae clientes para el periodo con la clasificacion bci

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_cliente_consolidado as
SELECT 
  periodo_cierre                      AS periodo_cierre,
  fecha_cierre                        AS fecha_entrada,
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
  fecha_cierre = '20220630' 
"""


# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_segmentacion_cliente_consolidado
# MAGIC --DESCRIBE riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operaciones D00 (tbl_segmentacion_d00_segmentado)
# MAGIC --------------------------------------
# MAGIC - Se extrae operaciones para periodo actual

# COMMAND ----------

#Extrae las operaciones de la tabla riesgobdu_golden_db.tbl_segmentacion_d00_segmentado para el periodo actual
paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado AS
SELECT 
  b.periodo_cierre                      AS periodo_cierre,
  b.fecha_cierre                        AS fecha_cierre,
  b.tipo_proceso                        AS tipo_proceso,
  b.cod_segmento                        AS cod_segmento,
  b.operacion                           AS operacion,
  b.cod_sistema                         AS cod_sistema,
  b.rut_cliente                         AS rut_cliente,
  b.dv_cliente                          AS dv_cliente
FROM
  riesgobdu_golden_db.tbl_segmentacion_d00_segmentado b
WHERE 
  b.fecha_cierre = '20220630'
  AND b.periodo_cierre = '202206100'
"""


# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   a.*
# MAGIC from
# MAGIC   tmp_EXT_tbl_segmentacion_d00_segmentado a
# MAGIC join
# MAGIC   tmp_EXT_tbl_segmentacion_cliente_consolidado b
# MAGIC on 
# MAGIC   a.rut_cliente = b.rut_cliente
# MAGIC where
# MAGIC   a.rut_cliente = 1934592

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC  --SELECT to_date('May 31 2022 12:00:00:000AM', 'yyyy-MM-dd');
# MAGIC  
# MAGIC  SELECT date('2016-12-31');
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla con operaciones clasificadas que deterioran y no deterioran del periodo actual

# COMMAND ----------

#tabla con operaciones clasificadas que deterioran y no deterioran del periodo actual
paso_query40 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL as
SELECT
 B.periodo_cierre,
 B.fecha_cierre,
 B.tipo_proceso,
 B.segmento,
 B.operacion,
 B.sistema,
 B.rut_cliente,
 B.dv_rut_cliente ,
 ' ' AS calificacion_regulador,
 '19000101' fecha_calificacion,
'operacion' AS nombre_campo,
 cast(B.operacion as string) AS valor_campo,
 " No informada en D00 Actual "  AS condicion_regla,     
 " 'is null' "  AS valor_regla,
 CASE WHEN A.operacion is null THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_ope_crit B
LEFT JOIN
    tmp_EXT_tbl_segmentacion_d00_segmentado A 
ON   A.operacion = B.operacion
 AND A.cod_sistema = B.sistema
"""


# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_cartdet_crit_ent_ope_crit where operacion = 'H00000093905'
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_segmentacion_d00_segmentado where operacion = 'H00000000777'
# MAGIC 
# MAGIC 
# MAGIC insert into tmp_EXT_tbl_cartdet_crit_ent_ope_crit
# MAGIC select 
# MAGIC   '202205100',
# MAGIC   '20220531',
# MAGIC   tipo_proceso,
# MAGIC   '13774599',
# MAGIC   '7',
# MAGIC   'H00000000777'
# MAGIC   cod_sistema,
# MAGIC   cod_segmento,
# MAGIC   1,
# MAGIC   0,
# MAGIC   '20220531'
# MAGIC from 
# MAGIC   tmp_EXT_tbl_cartdet_crit_ent_ope_crit 
# MAGIC where 
# MAGIC   operacion = 'H00000093905' 
# MAGIC     
# MAGIC 
# MAGIC 
# MAGIC --select * from tmp_RES_D00_OPE_CAMPO_EVAL where flag_resultado_regla = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal (tmp_tbl_cartdet_crit_sal_ope_campo)

# COMMAND ----------

#Crea tabla temporal final con la evaluacion por campo de cada criterio de evaluacion
paso_query60 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_ope_campo AS
SELECT
	 IFNULL(A.periodo_cierre,190001)       AS periodo_cierre    
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_cierre 
	,IFNULL(A.tipo_proceso,' ')            AS tipo_proceso 
	,IFNULL(A.rut_cliente,0)               AS rut_cliente 
	,IFNULL(A.dv_cliente,' ')              AS dv_rut_cliente 
	,IFNULL(A.operacion,' ')               AS operacion 
	,IFNULL(A.cod_sistema,' ')             AS sistema   
	,IFNULL(A.cod_segmento,' ')            AS segmento 
	,'3'                                   AS criterio_salida 
	,'0'                                   AS origen_deterioro 
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

sqlSafe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC select * from tmp_RES_D00_OPE_CAMPO_EVAL -- where flag_resultado_regla = 1 
# MAGIC 
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where operacion = 'E08099054045'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_cartdet_crit_ent_ope_crit

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal (tmp_RES_cartdet_crit_sal_ope_crit)

# COMMAND ----------

#Crea tabla temporal final por operacion con los criterios de salida deterioro que cumple la operacion
paso_query70 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_sal_ope_crit AS
select  
	 a.periodo_cierre    
	,a.fecha_cierre 
	,a.tipo_proceso 
	,a.rut_cliente 
	,a.dv_rut_cliente 
	,a.operacion 
	,a.sistema   
	,a.segmento 
	,a.criterio_salida 
	,a.origen_deterioro 
	,a.fecha_salida  
FROM
  tmp_tbl_cartdet_crit_sal_ope_campo a
JOIN
  tmp_EXT_tbl_cartdet_crit_ent_ope_crit b
ON
  a.operacion = b.operacion
 AND a.sistema = b.sistema 
WHERE  
  a.flag_resultado_regla = 1
"""


# COMMAND ----------

sqlSafe(paso_query70)

# COMMAND ----------

paso_query80 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_sal_ope_crit where fecha_cierre = {FechaX} AND criterio_entrada = 2 """

# COMMAND ----------

sqlSafe(paso_query80)

# COMMAND ----------

paso_query90 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_sal_ope_crit
SELECT 
  periodo_cierre    
  ,fecha_cierre 
  ,tipo_proceso 
  ,rut_cliente 
  ,dv_rut_cliente 
  ,operacion 
  ,sistema   
  ,segmento 
  ,criterio_salida 
  ,origen_deterioro 
  ,fecha_salida 
FROM
  tmp_RES_cartdet_crit_sal_ope_crit
"""  

# COMMAND ----------

sqlSafe(paso_query90)

# COMMAND ----------

# MAGIC %md
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo where operacion ='E08099054045'

# COMMAND ----------

paso_query100 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_sal_ope_campo where fecha_cierre = {FechaX} AND criterio_entrada = 2"""

# COMMAND ----------

sqlSafe(paso_query100)

# COMMAND ----------


paso_query110 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_sal_ope_campo
SELECT 
  periodo_cierre    
  ,fecha_cierre 
  ,tipo_proceso 
  ,rut_cliente 
  ,dv_rut_cliente 
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
  tmp_tbl_cartdet_crit_sal_ope_campo
"""  


# COMMAND ----------

sqlSafe(paso_query110)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")
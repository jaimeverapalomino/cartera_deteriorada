# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook para realziar consultas y cuadratura
# MAGIC 
# MAGIC ***************************************
# MAGIC 
# MAGIC ##### Tablas de input:
# MAGIC 
# MAGIC * tbl_segmentacion_clientes
# MAGIC * tbl_segmentacion_cliente_consolidado
# MAGIC * tbl_D00_extendido
# MAGIC 
# MAGIC ##### Tablas de output:
# MAGIC 
# MAGIC ##### Pendientes:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ##### Mapeo Link:
# MAGIC * Mapeo
# MAGIC https://docs.google.com/spreadsheets/d/1n8IUxUtDIGsMXC7yktDjv9GV7aqt7Jcn/edit#gid=1974089785
# MAGIC 
# MAGIC * Reglas de Negocio
# MAGIC https://docs.google.com/spreadsheets/d/1m03Jg3dBhJ3EuC_xQIC0jmfrKuQygUUISLxAYI7x04E/edit#gid=956642919
# MAGIC 
# MAGIC ##### Ayuda Link:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Notebook con funciones genéricas

# COMMAND ----------

# MAGIC %run "../segmentacion/Funciones_Comunes" 

# COMMAND ----------

# MAGIC %run "../segmentacion/BCI_AUX_Carga_Ingesta_Temporal"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setea Parámetros

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
# MAGIC ### Calificación Deterioro

# COMMAND ----------

calificaciones = obtieneParametro('SEGMENTACION', 'CAL_DET')

param_cal = calificaciones.split(',')
param_cal = ','.join([f"'{i}'" for i in param_cal])
param_cal

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evalua calificación BCI

# COMMAND ----------


#Extrae clientes Individuales 
paso_query1 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_SEG_IND as
SELECT 
  periodo_cierre                    AS periodo_cierre,
  fecha_cierre                      AS fecha_cierre,
  tipo_proceso                      AS tipo_proceso,
  rut_cliente                       AS rut_cliente,
  dv_rut_cliente                    AS dv_rut_cliente, 
  segmento_cliente                  AS segmento_cliente,
  cic_cliente                       AS cic_cliente
FROM
  riesgobdu_golden_db.tbl_segmentacion_clientes a
WHERE
    segmento_cliente = 'I'
  AND fecha_cierre = '20220630' 
"""


# COMMAND ----------

sqlSafe(paso_query1)

# COMMAND ----------

#Extrae clientes Individuales 

paso_query2 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_SEG_CLI_CAL as
SELECT 
  periodo_cierre                      AS periodo_cierre,
  fecha_cierre                        AS fecha_entrada,
  tipo_proceso                        AS tipo_proceso,
  rut_cliente                         AS rut_cliente,
  dv_cliente                          AS dv_rut_cliente, 
  calificacion_bci                    AS calificacion_bci,
  calificacion_regulador              AS calificacion_regulador,    
  cic_cliente                         AS cic_cliente
FROM
  riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado
WHERE 
  fecha_cierre = '20220630' 
  AND trim (calificacion_bci) IN ({param_cal}) 
"""


# COMMAND ----------


sqlSafe(paso_query2)


# COMMAND ----------

#Extrae las operaciones del cliente

paso_query3 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_SEG_D00_OPE AS
SELECT 
  periodo_cierre                      AS periodo_cierre,
  fecha_cierre                        AS fecha_cierre,
  tipo_proceso                        AS tipo_proceso,
  cod_segmento                        AS cod_segmento,
  operacion                           AS operacion,
  cod_sistema                         AS cod_sistema,
  rut_cliente                         AS rut_cliente,
  dv_cliente                          AS dv_cliente  
FROM
  riesgobdu_golden_db.tbl_segmentacion_d00_segmentado
WHERE 
  fecha_cierre = '20220630'
"""


# COMMAND ----------

sqlSafe(paso_query3)

# COMMAND ----------


paso_query4 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_SEG_CAL AS
SELECT 
  a.periodo_cierre                    AS periodo_cierre,
  a.fecha_cierre                      AS fecha_cierre,
  a.tipo_proceso                      AS tipo_proceso,
  a.rut_cliente                       AS rut_cliente,
  a.dv_rut_cliente                    AS dv_rut_cliente, 
  a.segmento_cliente                  AS segmento_cliente,
  a.cic_cliente                       AS cic_cliente,
  b.calificacion_bci                  AS calificacion_bci,
  b.fecha_entrada                     AS fecha_entrada,
   '1'                                AS criterio_entrada,
   '0'                                AS origen_deterioro
FROM 
  tmp_EXT_tbl_SEG_IND a
JOIN 
  tmp_EXT_tbl_SEG_CLI_CAL b
  ON TRIM(a.cic_cliente) = TRIM(b.cic_cliente)
"""



# COMMAND ----------

sqlSafe(paso_query4)

# COMMAND ----------


paso_query5 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_CRIT_ENT AS
SELECT 
  a.periodo_cierre                        AS periodo_cierre,
  a.fecha_cierre                          AS fecha_cierre,
  IFNULL(a.tipo_proceso,' ')              AS tipo_proceso,
  IFNULL(a.rut_cliente,0)                 AS rut_cliente,
  IFNULL(a.dv_rut_cliente,' ')            AS dv_rut_cliente, 
  IFNULL(b.operacion,' ')                 AS operacion,
  IFNULL(b.cod_sistema,' ')               AS cod_sistema,
  IFNULL(b.cod_segmento,' ')              AS segmento_cliente,
  IFNULL(a.criterio_entrada,' ')          AS criterio_entrada,
  IFNULL(a.origen_deterioro,' ')          AS origen_deterioro,
  a.fecha_entrada                         AS fecha_entrada
FROM
  tmp_RES_tbl_SEG_CAL a
LEFT JOIN 
  tmp_EXT_tbl_SEG_D00_OPE b 
ON a.rut_cliente = b.rut_cliente
"""


# COMMAND ----------

sqlSafe(paso_query5)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from tmp_RES_tbl_CRIT_ENT where rut_cliente like '%76717740'
# MAGIC 
# MAGIC 
# MAGIC --select * from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado where rut_cliente like '%76717740' and fecha_cierre = '20220630'
# MAGIC 
# MAGIC --select distinct cod_segmento from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado where fecha_cierre = '20220630'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   a.cic_cliente,
# MAGIC   a.rut_cliente
# MAGIC from 
# MAGIC   tmp_EXT_tbl_SEG_CLI_CAL a 
# MAGIC where 
# MAGIC   not exists (select b.cic_cliente from tmp_RES_tbl_SEG_CAL b where a.cic_cliente = b.cic_cliente  )
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_SEG_IND WHERE cic_cliente like '%7116497' 
# MAGIC select * from tmp_EXT_tbl_SEG_IND WHERE rut_cliente like '%8048584'
# MAGIC 
# MAGIC --5916
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_SEG_CLI_CAL WHERE cic_cliente = '    07116497'
# MAGIC --784
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_SEG_D00_OPE 
# MAGIC --11599119
# MAGIC 
# MAGIC --select COUNT(1) from tmp_RES_tbl_SEG_CAL 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tmp_RES_tbl_SEG_CAL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC SELECT 
# MAGIC       count(1)
# MAGIC FROM 
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_clientes a
# MAGIC JOIN 
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado b
# MAGIC   ON a.fecha_cierre = b.fecha_cierre
# MAGIC   AND a.cic_cliente = b.cic_cliente
# MAGIC WHERE
# MAGIC   a.segmento_cliente = 'I'
# MAGIC -- AND trim (b.calificacion_bci) IN (SELECT parametro1 FROM riesgobdu_silver_db.tbl_cierreriesgo_parametro WHERE tipo_parametro = 'CAL_DET' AND Vigencia = 'V')
# MAGIC -- AND a.fecha_cierre = '20220630'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select count(1) from riesgobdu_golden_db.tbl_segmentacion_clientes where fecha_cierre = '20220630'
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cierreriesgo_parametro WHERE proceso = 'SEGMENTACION' AND tipo_parametro = 'MESES_RESEGMENTAR' and vigencia = 'V'
# MAGIC 
# MAGIC SELECT * FROM riesgobdu_silver_db.tbl_cierreriesgo_parametro --WHERE tipo_parametro = 'CAL_DET' AND Vigencia = 'V'
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cierreriesgo_parametro 

# COMMAND ----------

# MAGIC %sql
# MAGIC --(SELECT parametro1 FROM riesgobdu_silver_db.tbl_cierreriesgo_parametro WHERE tipo_parametro = 'CAL_DET' AND Vigencia = 'V')
# MAGIC 
# MAGIC --select * from riesgobdu_golden_db.tbl_segmentacion_clientes 
# MAGIC 
# MAGIC SELECT * FROM riesgobdu_silver_db.tbl_cierreriesgo_control_procesos 
# MAGIC 
# MAGIC --select count(1) from riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado 
# MAGIC 
# MAGIC --select * from tbl_clasificacion_cliente where cal_cic like '%1395855%'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado where cic_cliente like '%1395855%'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1),periodo_cierre,segmento_cliente from riesgobdu_golden_db.tbl_segmentacion_clientes group by 2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC select count(1),periodo_cierre,fecha_cierre,cod_segmento from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado group by 2,3,4

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Query ${bci.dbnameSilver}.tbl_cartdet_crit_ent_ope_crit

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select count(1), fecha_cierre from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit group by 2
# MAGIC 
# MAGIC --Sep 30 2021 12:00:00:000AM
# MAGIC --select * from tbl_c4_hist_ope_crit where nro_crit = 7 and fec_ini_cd = 'Sep 30 2021 12:00:00:000AM'
# MAGIC 
# MAGIC --select * from tbl_c4_hist_ope_crit where nro_crit = 7 and fec_ini_cd =  'May 31 2022 12:00:00:000AM' and ori_det = 1
# MAGIC 
# MAGIC --select * from tbl_c4_hist_ope_crit where rut_cli = 4868942 and fec_ini_cd =  'May 31 2022 12:00:00:000AM'
# MAGIC 
# MAGIC select cuenta_contable,saldo_cartera_venc,dias_mora from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado where rut_cliente = 4868942 and periodo_cierre = '202206100'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tbl_c4_hist_cart_det where rut_cli = 4868942 and fec_proc = 'May 31 2022 12:00:00:000AM'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado where rut_cliente = 4868942

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")
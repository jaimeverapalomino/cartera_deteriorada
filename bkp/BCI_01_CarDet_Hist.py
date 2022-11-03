# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: Genera Historia Para Tablas Cartera Deteriodada
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_01_CarDet_Hist.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gonzalo Arias (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 24/08/2022
# MAGIC * Descripcion: Genera datos historicos para cargar en tablas de cartera deteriorada
# MAGIC * Documentacion: 
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
# MAGIC * tbl_c4_hist_cart_det
# MAGIC * tbl_c4_hist_dat_cli
# MAGIC * tbl_c4_hist_ope_crit
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios
# MAGIC ********************************************************************************************************
# MAGIC ##### Pendientes:
# MAGIC * carga historia para grupales

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
dbutils.widgets.text("tipo_procesoW","","06-Tipo de Proceso (C o PC):")

FechaX = dbutils.widgets.get("FechaW") 
PeriodoX = dbutils.widgets.get("PeriodoW")
base_silverX = dbutils.widgets.get("bd_silverW")
ruta_silverX = dbutils.widgets.get("ruta_silverW")
bd_ruta_silverX = dbutils.widgets.get("path_bdW")
tipo_procesoX = dbutils.widgets.get("tipo_procesoW")

spark.conf.set("bci.Fecha", FechaX)
spark.conf.set("bci.Periodo", PeriodoX)
spark.conf.set("bci.dbnamesilver", base_silverX)
spark.conf.set("bci.rutasilver", ruta_silverX)
spark.conf.set("bci.bdRutasilver", bd_ruta_silverX)
spark.conf.set("bci.tipo_proceso", tipo_procesoX)

print(f"Fecha de Proceso: {FechaX}")
print(f"Periodo: {PeriodoX}")      
print(f"Nombre BD Silver: {base_silverX}")
print(f"Ruta abfss Silver: {ruta_silverX}")
print(f"Ruta abfss de la BD Silver: {bd_ruta_silverX}")
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
# MAGIC ### Operaciones D00 DETERIORADAS (tbl_segmentacion_d00_segmentado)
# MAGIC --------------------------------------
# MAGIC - Se extrae operaciones DETERIORADAS para periodo 20220531

# COMMAND ----------

#Extrae operaciones deteriorioradas PERIODO 20220531
paso_query10 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado as
SELECT
	 periodo_cierre
	,fecha_cierre
	,tipo_proceso
	,rut_cliente
	,dv_cliente
	,operacion
	,cod_sistema
	,cod_segmento
    ,cod_cartdet_cliente
	,cod_motivo_cartdet
	,fecha_cartdet_ope	
FROM 
	riesgobdu_golden_db.tbl_segmentacion_d00_segmentado
WHERE
	ind_cartdet = 'D' /*DETERIORADAS*/
and fecha_cierre=20220531    
"""

# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtiene criterios deterioro  (tbl_c4_hist_ope_crit)
# MAGIC --------------------------------------
# MAGIC - Se extrae operaciones deterioradas para periodo 20220531

# COMMAND ----------

#Extrae stock de operaciones deterioradas del cierre anterior
paso_query11 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_c4_hist_ope_crit as
SELECT 
  20220531                      AS fecha_cierre,
  rut_cli                       AS rut_cliente,
  num_interno_ident             AS operacion,
  sistema                       AS sistema,
  ind_tipo_seg                  AS segmento,
  nro_crit                      AS criterio_entrada,
  ori_det                       AS origen_deterioro,
  fec_ini_cd                    AS fecha_entrada,
  fec_susp_dev                  AS fecha_susp_devengo
FROM
  tbl_c4_hist_ope_crit 
WHERE
    fec_proc = 'May 31 2022 12:00:00:000AM'
"""


# COMMAND ----------

sqlSafe(paso_query11)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from tmp_EXT_tbl_c4_hist_ope_crit where rut_cliente=15412189
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_segmentacion_d00_segmentado where rut_cliente=15412189
# MAGIC 
# MAGIC --select * from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado where rut_cliente=15412189 and periodo_cierre=202205
# MAGIC 
# MAGIC --describe riesgobdu_golden_db.tbl_segmentacion_d00_segmentado

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

# MAGIC %sql
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_segmentacion_cliente_consolidado
# MAGIC --DESCRIBE riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clientes Individuales (tbl_segmentacion_clientes)
# MAGIC --------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #Extrae clientes Individuales. Esta tabla solo la utilizamos de emergencia porque la tabla d00 segmentado NO tiene el segmento
# MAGIC paso_query30 = """
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_clientes_Ind as
# MAGIC SELECT 
# MAGIC   periodo_cierre                    AS periodo_cierre,
# MAGIC   fecha_cierre                      AS fecha_cierre,
# MAGIC   tipo_proceso                      AS tipo_proceso,
# MAGIC   rut_cliente                       AS rut_cliente,
# MAGIC   dv_rut_cliente                    AS dv_rut_cliente, 
# MAGIC   segmento_cliente                  AS segmento_cliente,
# MAGIC   cic_cliente                       AS cic_cliente
# MAGIC FROM
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_clientes 
# MAGIC WHERE
# MAGIC     segmento_cliente = 'I'
# MAGIC   AND fecha_cierre = '20220630'
# MAGIC   AND periodo_cierre = '202206100'
# MAGIC """

# COMMAND ----------

#sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select count(1) as num,rut_cliente from tmp_EXT_tbl_segmentacion_d00_segmentado where cod_segmento = 'I' and fecha_cierre = '20220630' group by 2
# MAGIC  -- # 7732746   # 51894   "I"
# MAGIC --select * from tmp_EXT_tbl_segmentacion_d00_segmentado
# MAGIC  
# MAGIC --select count(1) from tmp_EXT_tbl_segmentacion_clientes_Ind where segmento_cliente = 'I' and fecha_cierre = '20220630' 
# MAGIC  
# MAGIC --E08099401964
# MAGIC --E18099401964
# MAGIC 
# MAGIC  
# MAGIC --select * from tmp_EXT_tbl_segmentacion_d00_segmentado where operacion = 'E08099401964'
# MAGIC 
# MAGIC --select fecha_cierre,count(*) from riesgobdu_golden_db.tbl_segmentacion_clientes group by 1
# MAGIC 
# MAGIC --select cod_segmento, count(*) from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado group by 1
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from tmp_EXT_tbl_segmentacion_clientes_Ind where rut_cliente = 76057558

# COMMAND ----------

#Extrae las operaciones individuales 

paso_query40 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_IND AS
SELECT 
  cast(substr(cast(b.periodo_cierre as string),1,6) as integer)          AS periodo_cierre,  /*toda esta transformacion es temporal*/
  b.fecha_cierre                        AS fecha_cierre,
  b.tipo_proceso                        AS tipo_proceso,
  b.cod_segmento                        AS cod_segmento,
  b.operacion                           AS operacion,
  b.cod_sistema                         AS cod_sistema,
  b.rut_cliente                         AS rut_cliente,
  b.dv_cliente                          AS dv_cliente
FROM
  tmp_EXT_tbl_segmentacion_d00_segmentado b
WHERE 
  cod_segmento = 'I'
"""


# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from tmp_RES_D00_OPE_IND where rut_cliente = 76057558 
# MAGIC 
# MAGIC --E08099401964
# MAGIC --E18099401964
# MAGIC 
# MAGIC --select count(1), periodo_cierre from tmp_RES_D00_OPE_IND group by 2
# MAGIC 
# MAGIC --select count(1) from tmp_EXT_tbl_segmentacion_clientes_Ind  --where segmento_cliente = 'I' 
# MAGIC --select fecha_cierre, periodo_cierre,count(*) from tmp_EXT_tbl_segmentacion_d00_segmentado group by 1,2
# MAGIC --describe tmp_EXT_tbl_segmentacion_d00_segmentado

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla con operaciones clasificadas que deterioran y no deterioran del periodo actual

# COMMAND ----------

#tabla con operaciones clasificadas que deterioran y no deterioran del periodo actual
paso_query50 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL as
SELECT
 A.periodo_cierre,
 A.fecha_cierre,
 A.tipo_proceso,
 A.cod_segmento,
 A.operacion,
 A.cod_sistema,
 A.rut_cliente,
 A.dv_cliente ,
 B.calificacion_regulador,
 B.fecha_calificacion,
'calificacion_bci' AS nombre_campo,
 cast(B.calificacion_bci as string) AS valor_campo,
 " IN "  AS condicion_regla,     
 "{param_cal}"  AS valor_regla,
 CASE WHEN trim (B.calificacion_bci) IN ({param_cal}) THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_RES_D00_OPE_IND A
LEFT JOIN
    tmp_EXT_tbl_segmentacion_cliente_consolidado B 
ON A.rut_cliente = B.rut_cliente
"""


# COMMAND ----------

sqlSafe(paso_query50)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select flag_resultado_regla, count(*) from tmp_RES_D00_OPE_CAMPO_EVAL group by 1
# MAGIC 
# MAGIC --select count(*) from tmp_RES_D00_OPE_CAMPO_EVAL
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal (tmp_tbl_cartdet_crit_ent_ope_campo)

# COMMAND ----------

#Crea tabla temporal final con la evaluacion por campo de cada criterio de evaluacion
paso_query60 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_ent_ope_campo AS
SELECT
	 IFNULL(A.periodo_cierre,190001)       AS periodo_cierre    
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_cierre 
	,IFNULL(A.tipo_proceso,' ')            AS tipo_proceso 
	,IFNULL(A.rut_cliente,0)               AS rut_cliente 
	,IFNULL(A.dv_cliente,' ')              AS dv_rut_cliente 
	,IFNULL(A.operacion,' ')               AS operacion 
	,IFNULL(A.cod_sistema,' ')             AS sistema   
	,IFNULL(A.cod_segmento,' ')            AS segmento 
	,'1'                                   AS criterio_entrada 
	,'0'                                   AS origen_deterioro 
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

sqlSafe(paso_query60)

# COMMAND ----------

# MAGIC %sql
# MAGIC select flag_resultado_regla, count(*) from tmp_tbl_cartdet_crit_ent_ope_campo group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC --lect nro_crit, ori_det, count(*) from tbl_c4_hist_ope_crit where nro_crit = 1 and ind_tipo_seg = 'G'  AND fec_proc = 'May 31 2022 12:00:00:000AM' group by 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stock de operaciones deterioradas del cierre anterior

# COMMAND ----------


#Extrae stock de operaciones deterioradas del cierre anterior
paso_query70 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_c4_hist_ope_crit as
SELECT 
  20220531                      AS fecha_cierre,
  rut_cli                       AS rut_cliente,
  num_interno_ident             AS operacion,
  sistema                       AS sistema,
  ind_tipo_seg                  AS segmento,
  nro_crit                      AS criterio_entrada,
  ori_det                       AS origen_deterioro,
  fec_ini_cd                    AS fecha_entrada,
  fec_susp_dev                  AS fecha_susp_devengo
FROM
  tbl_c4_hist_ope_crit 
WHERE
    nro_crit = 1 /*criterio deterioro individual*/
  AND fec_proc = 'May 31 2022 12:00:00:000AM'
"""

# COMMAND ----------

sqlSafe(paso_query70)

# COMMAND ----------

#Extrae campos complemetarios para el stock
paso_query80 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_c4_hist_cart_det as
SELECT 
  20220531            AS fecha_cierre,
  sistema             AS sistema,
  num_interno_ident   AS operacion,
  ind_tipo_seg        AS segmento,
  rut_cli             AS rut_cliente,
  dv_rut_cli          AS dv_rut_cliente
FROM
  tbl_c4_hist_cart_det
WHERE
  fec_proc = 'May 31 2022 12:00:00:000AM'
"""  
  

# COMMAND ----------

sqlSafe(paso_query80)

# COMMAND ----------

paso_query90 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_DET_IND_MES_ANT AS
SELECT 
   {PeriodoX}             AS periodo_cierre   
  ,{FechaX}               AS fecha_cierre
  ,'{tipo_procesoX}'      AS tipo_proceso      
  ,a.rut_cliente          AS rut_cliente
  ,b.dv_rut_cliente       AS dv_rut_cliente
  ,a.operacion            AS operacion
  ,a.sistema              AS sistema
  ,a.segmento             AS segmento
  ,a.criterio_entrada     AS criterio_entrada
  ,a.origen_deterioro     AS origen_deterioro
  ,a.fecha_entrada        AS fecha_entrada
  ,a.fecha_susp_devengo   AS fecha_susp_devengo
FROM 
  tmp_EXT_tbl_c4_hist_ope_crit a
JOIN
  tmp_EXT_tbl_c4_hist_cart_det b
ON
  a.operacion = b.operacion
AND a.sistema = b.sistema
"""  

# COMMAND ----------

sqlSafe(paso_query90)

# COMMAND ----------

# MAGIC %sql
# MAGIC select segmento, count(*) from tmp_RES_cartdet_stock group by 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal (tmp_RES_cartdet_crit_ent_ope_crit)

# COMMAND ----------

#Crea tabla temporal final por operacion con los criterios de deterioro que cumple la operacion
paso_query100 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_ent_ope_crit AS
select  
	 periodo_cierre    
	,fecha_cierre 
	,tipo_proceso 
	,rut_cliente 
	,dv_rut_cliente 
	,operacion 
	,sistema   
	,segmento 
	,criterio_entrada 
	,origen_deterioro 
	,fecha_entrada 
FROM 
    tmp_tbl_cartdet_crit_ent_ope_campo
WHERE 
   flag_resultado_regla=1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
"""


# COMMAND ----------

sqlSafe(paso_query100)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1), fecha_entrada from tmp_RES_cartdet_crit_ent_ope_crit   group by 2

# COMMAND ----------

#Para las deteriroradas del mes actual, mantiene fecha de deterioro del mes anterior
paso_query110 = """
CREATE OR REPLACE TEMPORARY VIEW  tmp_tbl_cartdet_crit_ent_ope_crit_act AS 
select  
	 substr(a.periodo_cierre,1,6) as periodo_cierre
	,a.fecha_cierre 
	,a.tipo_proceso 
	,a.rut_cliente 
	,a.dv_rut_cliente 
	,a.operacion 
	,a.sistema   
	,a.segmento 
	,a.criterio_entrada 
	,a.origen_deterioro 
	,CASE WHEN b.operacion IS NOT NULL THEN b.fecha_entrada else a.fecha_entrada END AS fecha_entrada
FROM 
    tmp_RES_cartdet_crit_ent_ope_crit a 
  left join
    tmp_RES_cartdet_stock b
ON
  a.operacion = b.operacion
 AND a.sistema = b.sistema
 AND a.criterio_entrada = b.criterio_entrada
""" 
 
 



# COMMAND ----------

sqlSafe(paso_query110)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tmp_tbl_cartdet_crit_ent_ope_crit_act

# COMMAND ----------

paso_query120 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_tbl_cartdet_crit_ent_ope_crit_stk AS 
select  
	 {PeriodoX} AS periodo_cierre   
	,{FechaX} AS fecha_cierre
	,a.tipo_proceso 
	,a.rut_cliente 
	,a.dv_rut_cliente 
	,a.operacion 
	,a.sistema   
	,a.segmento 
	,a.criterio_entrada 
	,a.origen_deterioro 
	,a.fecha_entrada
FROM 
    tmp_RES_cartdet_stock a
  left join
    tmp_RES_cartdet_crit_ent_ope_crit b
ON
  a.operacion = b.operacion
 AND a.sistema = b.sistema
 AND a.criterio_entrada = b.criterio_entrada
WHERE
     b.operacion is null
""" 
 

# COMMAND ----------

sqlSafe(paso_query120)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from tmp_tbl_cartdet_crit_ent_ope_crit_stk
# MAGIC --select count(*) from tmp_tbl_cartdet_crit_ent_ope_crit_act

# COMMAND ----------

paso_query130 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_crit where fecha_cierre = {FechaX} AND criterio_entrada = 1 """


# COMMAND ----------

sqlSafe(paso_query130)

# COMMAND ----------

paso_query140 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_ent_ope_crit
SELECT 
  periodo_cierre    
  ,fecha_cierre 
  ,tipo_proceso 
  ,rut_cliente 
  ,dv_rut_cliente 
  ,operacion 
  ,sistema   
  ,segmento 
  ,criterio_entrada 
  ,origen_deterioro 
  ,fecha_entrada 
FROM
  tmp_tbl_cartdet_crit_ent_ope_crit_act
"""  

# COMMAND ----------

sqlSafe(paso_query140)

# COMMAND ----------

#proceso se cancela al ser el campo fecha_entrada INT y la fecha trae caracteres 12:00 AM
paso_query150 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_ent_ope_crit
SELECT 
  periodo_cierre    
  ,fecha_cierre 
  ,tipo_proceso 
  ,rut_cliente 
  ,dv_rut_cliente 
  ,operacion 
  ,sistema   
  ,segmento 
  ,criterio_entrada 
  ,origen_deterioro 
  ,fecha_cierre AS fecha_entrada 
FROM
  tmp_tbl_cartdet_crit_ent_ope_crit_stk
""" 

# COMMAND ----------

sqlSafe(paso_query150)

# COMMAND ----------

# MAGIC %sql
# MAGIC select periodo_cierre, count(*) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit group by 1

# COMMAND ----------

paso_query160 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_campo where fecha_cierre = {FechaX} AND criterio_entrada = 1 """

# COMMAND ----------

sqlSafe(paso_query160)

# COMMAND ----------


paso_query170 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_ent_ope_campo
SELECT 
  periodo_cierre    
  ,fecha_cierre 
  ,tipo_proceso 
  ,rut_cliente 
  ,dv_rut_cliente 
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

sqlSafe(paso_query170)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo where rut_cliente=95967000
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where  rut_cliente=78799620
# MAGIC 
# MAGIC ---select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where  fecha_entrada <> 20220630
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where rut_cliente = 96922100
# MAGIC 
# MAGIC 
# MAGIC select * from riesgobdu_silver_db.tbl_segmentacion_clientes where rut_cliente = 96922100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")
# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: Stock Deterioro, Segmento grupal.
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_50_CarDet_Stock_Grupal.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Genera stock de operaciones deterioradas segmento Grupal.
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
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_prin
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_grupal_stock
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

# MAGIC %run "../Notebook_Apoyo/BCI_AUX_Carga_Ingesta_Temporal_CD"

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
# MAGIC ### Obtiene criterios de entrada deterioro grupal
# MAGIC ---
# MAGIC * Obtiene de la tabla de parametros los codigos de deterioro grupal

# COMMAND ----------

p_GruCritDetEnt = (7,8,9,10,11,12,13)
p_GruCritDetSal = (27,30,34,35,36,37,38,39,40,41)

# COMMAND ----------

print(f"p_GruCritDetEnt: {p_GruCritDetEnt}")
print(f"p_GruCritDetSal: {p_GruCritDetSal}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Criterios Deterioro Entrada grupal (tbl_cartdet_crit_ent_ope_prin)
# MAGIC ---
# MAGIC * Tabla con criterios deterioro de entrada grupal

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_EXT_tbl_cartdet_crit_ent_ope_crit AS 
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
FROM
  {base_silverX}.tbl_cartdet_crit_ent_ope_prin A
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
# MAGIC ### Extrae Criterios Deterioro Salida Grupal (tbl_cartdet_crit_sal_ope_prin)
# MAGIC ---
# MAGIC * Tabla con criterios deterioro de salida grupal

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_EXT_tbl_cartdet_crit_sal_ope_crit AS 
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
FROM
  {base_silverX}.tbl_cartdet_crit_sal_ope_prin A
WHERE
    fecha_cierre = {FechaX}  
AND periodo_cierre = {PeriodoX}  
AND tipo_proceso = '{tipo_procesoX}' 
AND criterio_salida  IN {p_GruCritDetSal}  
""" 

# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla Temporal resultado de deterioro grupal (tbl_cartdet_grupal_stock)
# MAGIC ---
# MAGIC * marca las operaciones deterioradas en tabla entrada con las operaciones que salen de la tabla de salida
# MAGIC * marca 1 significa que queda deteriorada y 0 implica que sale de deterioro 
# MAGIC * sale de deterioro si la operacion en la tabla de entrada se encuentra en la tabla de salida

# COMMAND ----------

paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_tbl_cartdet_grupal_stock AS 
SELECT
   A.periodo_cierre    
  ,A.fecha_cierre 
  ,A.tipo_proceso 
  ,A.rut_cliente 
  ,A.dv_rut_cliente
  ,A.tipo_operacion
  ,A.operacion 
  ,A.sistema   
  ,A.segmento 
  ,A.criterio_entrada 
  ,A.origen_deterioro 
  ,A.fecha_entrada
  ,IFNULL(B.criterio_salida,0) AS criterio_salida
  ,CASE WHEN B.operacion is null THEN 1 ELSE 0 END ind_ope_det
FROM
  tmp_EXT_tbl_cartdet_crit_ent_ope_crit A
LEFT JOIN  
  tmp_EXT_tbl_cartdet_crit_sal_ope_crit B
ON
    TRIM(A.operacion) = TRIM(B.operacion)
AND TRIM(A.sistema) = TRIM(B.sistema)
""" 

# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Tablas de Salidas
# MAGIC --------------------------------------
# MAGIC * carga resultados a tablas de salidas del notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Tabla Evaluacion por Criterio (riesgobdu_silver_db.tbl_cartdet_grupal_stock)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos)

# COMMAND ----------

paso_query40 = f"""DELETE FROM {base_silverX}.tbl_cartdet_grupal_stock where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX}  AND tipo_proceso = '{tipo_procesoX}' AND criterio_entrada  IN {p_GruCritDetEnt} """


# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query50 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_grupal_stock
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
FROM
  tmp_RES_tbl_cartdet_grupal_stock
WHERE
 ind_ope_det = 1 
"""  

# COMMAND ----------

sqlSafe(paso_query50)

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
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_grupal_stock where rut_cliente = 8613361
# MAGIC 
# MAGIC select count(1),criterio_entrada from riesgobdu_silver_db.tbl_cartdet_grupal_stock group by criterio_entrada 
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_grupal_stock
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC --Jun 30 2022
# MAGIC --select * from tbl_c4_hist_cart_det where  rut_cli = 7847712 and fec_proc like '%Jun 30 2022%' 
# MAGIC 
# MAGIC select * from tbl_c4_hist_ope_crit where  rut_cli = 7847712 and fec_proc like '%Jun 30 2022%' 
# MAGIC 
# MAGIC --select count(1),segmento from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin group by segmento
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_prin where rut_cliente = 18009741
# MAGIC 
# MAGIC --select * from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado where periodo_cierre = 202206100 
# MAGIC 
# MAGIC --select * from tbl_D00_Extendido
# MAGIC 
# MAGIC /*
# MAGIC SELECT
# MAGIC   
# MAGIC   periodo_cierre
# MAGIC   ,fecha_cierre
# MAGIC   ,tipo_proceso
# MAGIC   ,cod_segmento
# MAGIC   ,cod_motivo_cartdet
# MAGIC   ,cod_cartdet_cliente
# MAGIC   ,count(1)
# MAGIC FROM
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_d00_segmentado
# MAGIC where
# MAGIC     fecha_cierre = 20220630  
# MAGIC AND periodo_cierre = 202206
# MAGIC AND tipo_proceso = 'C'
# MAGIC AND   ind_cartdet = 'D'
# MAGIC group by 1,2,3,4,5,6
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC d0p_fec_fpro
# MAGIC 
# MAGIC SELECT
# MAGIC    d0p_fec_fper
# MAGIC   ,d0p_fec_fpro
# MAGIC   ,d0p_cod_clas
# MAGIC   ,cod_motivo_cartdet
# MAGIC   ,cod_cartdet_cliente
# MAGIC   ,count(1)
# MAGIC FROM
# MAGIC   tbl_D00_Extendido
# MAGIC where
# MAGIC     d0p_fec_fpro = 20220630  
# MAGIC AND d0p_fec_fper = 202206
# MAGIC AND   d0p_ind_cdet = 'D'
# MAGIC group by 1,2,3,4,5

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin where operacion like '%6081122103%'
# MAGIC 
# MAGIC select count(1),tipo_proceso from riesgobdu_silver_db.tbl_cartdet_grupal_stock group by tipo_proceso
# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: Stock Deterioro, Segmento Individual.
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_01_CarDet_Stock_Individual.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Genera stock de operaciones deterioradas segmento individual
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
# MAGIC * riesgobdu_silver_db.tbl_cartdet_individual_stock
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
# MAGIC ### Obtiene criterios de entrada deterioro individual
# MAGIC ---
# MAGIC * Obtiene de la tabla de parametros los codigos de deterioro individual

# COMMAND ----------

p_IndCritDetEnt = 1
p_IndCritDetSal = 2,3,30,50

# COMMAND ----------

print(f"p_IndCritDetEnt: {p_IndCritDetEnt}")
print(f"p_IndCritDetSal: {p_IndCritDetSal}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Criterios Deterioro Entrada Individuales (tbl_cartdet_crit_ent_ope_prin)
# MAGIC ---
# MAGIC * Tabla con criterios deterioro de entrada individual
# MAGIC * Para individual solo hay un criterio de deterioro

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
AND criterio_entrada  IN ({p_IndCritDetEnt})  
""" 

# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Criterios Deterioro Salida Individuales (tbl_cartdet_crit_sal_ope_prin)
# MAGIC ---
# MAGIC * Tabla con criterios deterioro de salida individual

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
AND criterio_salida  IN {p_IndCritDetSal}  
""" 

# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla Temporal resultado de deterioro individual (tbl_cartdet_individual_stock)
# MAGIC ---
# MAGIC * marca las operaciones deterioradas en tabla entrada con las operaciones que salen de la tabla de salida
# MAGIC * marca 1 significa que queda deteriorada y 0 implica que sale de deterioro 
# MAGIC * sale de deterioro si la operacion en la tabla de entrada se encuentra en la tabla de salida

# COMMAND ----------

paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_tbl_cartdet_individual_stock AS 
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
# MAGIC ### Carga Tabla Evaluacion por Criterio (riesgobdu_silver_db.tbl_cartdet_individual_stock)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos)

# COMMAND ----------

paso_query300 = f"""DELETE FROM {base_silverX}.tbl_cartdet_individual_stock where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} and tipo_proceso = '{tipo_procesoX}' and criterio_entrada  = {p_IndCritDetEnt} """


# COMMAND ----------

sqlSafe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query330 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_individual_stock
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
  tmp_RES_tbl_cartdet_individual_stock
WHERE
 ind_ope_det = 1 --operaciones deterioradas
"""  

# COMMAND ----------

sqlSafe(paso_query330)

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC --ESTADISTICAS
# MAGIC select periodo_cierre, criterio_entrada, origen_deterioro, count(*) from riesgobdu_silver_db.tbl_cartdet_individual_stock group by 1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_individual_stock where rut_cliente=7776581 order by operacion, sistema

# COMMAND ----------

# MAGIC %sql
# MAGIC select periodo_cierre, fecha_cierre, tipo_proceso, rut_cliente, dv_cliente as dv_rut_cliente, tipo_operacion, operacion, cod_sistema as sistema, cod_segmento as segmento, 
# MAGIC IFNULL(cast(cod_motivo_cartdet as integer),0) as criterio_entrada,
# MAGIC IFNULL(cast(cod_cartdet_cliente as integer),0) as origen_deterioro,
# MAGIC IFNULL(cast(fecha_cartdet_ope as integer),0) as fecha_entrada
# MAGIC from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado 
# MAGIC where rut_cliente =7776581 and periodo_cierre=202206100 and fecha_cierre=20220630 and tipo_proceso='C' and ind_cartdet='D'
# MAGIC order by operacion, sistema

# COMMAND ----------

# MAGIC %sql
# MAGIC --select segmento, count(*) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin group by 1
# MAGIC 
# MAGIC select segmento, count(*) from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_prin group by 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")
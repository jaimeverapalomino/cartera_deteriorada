# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: Criterio de Entrada Deterioro Principal, Segmento Individual.
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_03_CarDet_Ind_Crit_Ent_Prin.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Calcula el deterioro principal de deterioro individual por operacion.
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
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin
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

# MAGIC %run "../segmentacion/Funciones_Comunes" 

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
# MAGIC ### Obtiene criterios de entrada deterioro individual
# MAGIC ---
# MAGIC * Obtiene de la tabla de parametros los codigos de deterioro individual

# COMMAND ----------

p_IndCritDetEnt = 1

# COMMAND ----------

print(f"p_IndCritDetEnt: {p_IndCritDetEnt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Criterios Deterioro Entrada Individuales (tbl_cartdet_crit_ent_ope_crit)
# MAGIC ---
# MAGIC * Talba con criterios deterioro de entrada individual
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
  {base_silverX}.tbl_cartdet_crit_ent_ope_crit A
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
# MAGIC ### Salida Temporal a Nivel de Criterio Deterioro (tmp_tbl_cartdet_crit_ent_ope_crit)
# MAGIC ---
# MAGIC * tabla temporal que contiene todas los criterios de deterioro que pueda tener una operacion
# MAGIC * para individual solo hay un criterio de deterioro

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_tbl_cartdet_crit_ent_ope_prin AS 
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
  A.fecha_entrada          AS fecha_entrada,
  RANK() OVER (PARTITION BY operacion, sistema ORDER BY criterio_entrada asc) AS id_rank
FROM 
  tmp_EXT_tbl_cartdet_crit_ent_ope_crit A
""" 

# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Tablas de Salidas
# MAGIC --------------------------------------
# MAGIC * carga resultados a tablas de salidas del notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Tabla Evaluacion por Criterio (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos)

# COMMAND ----------

paso_query300 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_prin where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} and tipo_proceso = '{tipo_procesoX}' and criterio_entrada  = {p_IndCritDetEnt} """


# COMMAND ----------

sqlSafe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query330 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_ent_ope_prin
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
  ,'' AS criterio_operacion
FROM
  tmp_tbl_cartdet_crit_ent_ope_prin
WHERE
 id_rank = 1
"""  

# COMMAND ----------

sqlSafe(paso_query330)

# COMMAND ----------

# MAGIC %sql
# MAGIC --ESTADISTICAS FINALES TABLA SALIDA
# MAGIC select periodo_cierre, fecha_cierre, tipo_proceso, segmento, criterio_entrada, origen_deterioro, count(*) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin group by 1,2,3,4,5,6;

# COMMAND ----------

# MAGIC %sql
# MAGIC --select periodo_cierre, criterio_entrada, origen_deterioro, count(*) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit group by 1,2,3
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin where rut_cliente=7776581

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC truncate table riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")
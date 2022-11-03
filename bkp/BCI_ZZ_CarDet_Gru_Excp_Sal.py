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
# MAGIC * Nombre: BCI_ZZ_CarDet_Gru_Excp_Sal.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/10/2022
# MAGIC * Descripcion: Obtiene excepciones de operaciones con monto cero que no debieran salir de CD..
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
# MAGIC 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC 
# MAGIC 
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios
# MAGIC * 
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
dbutils.widgets.text("ruta_silverW","","04-Ruta adss Silver:")
dbutils.widgets.text("path_bdW","","05-Ruta adss de la BD Silver:")
dbutils.widgets.text("tipo_procesoW","","06-Tipo de Proceso (C o PC):")
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
spark.conf.set("bci.dbnamegold", base_goldX)

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

# DBTITLE 1,Valida parámetro "base_goldX"
validaParametro(base_goldX)

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
# MAGIC ### Extrae Criterios de Deterioro Salida Grupal

# COMMAND ----------


p_GruCritDetSal = 27
p_GruCritDetSal


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones con criterio de salida 27 (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)
# MAGIC --------------------------------------
# MAGIC - Se extrae Operaciones con criterio de salida 27

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_sal_ope_crit AS
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
  {base_silverX}.tbl_cartdet_crit_sal_ope_crit
WHERE
    fecha_cierre = {FechaX}  
AND periodo_cierre = {PeriodoX}  
AND tipo_proceso = '{tipo_procesoX}'
AND criterio_salida = {p_GruCritDetSal}
"""  


# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtiene excepciones de operaciones con monto cero que no debieran salir de CD

# COMMAND ----------

#Tabla con operaciones que cumplen y no cumplen condicion de salida deterioro del periodo actual
paso_query30 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL as
SELECT
     A.periodo_cierre,
     A.fecha_cierre,
     A.tipo_proceso,
     A.operacion,
     A.sistema,
     A.rut_cliente
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_ope_prin A 
LEFT JOIN 
    tbl_cartdet_SSFF B
ON   
    A.rut_cliente = B.rut
"""

# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tbl_excp_mto_cerodefault.excp_mto_cero

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borrado e insercion en Tabla (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)

# COMMAND ----------


paso_query50 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_sal_ope_campo where fecha_cierre = {FechaX} AND criterio_salida = {p_GruCritDetSal} """


# COMMAND ----------

sqlSafe(paso_query50)

# COMMAND ----------

paso_query60 = f"""
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
  tmp_tbl_cartdet_crit_sal_ope_campo 

"""   

# COMMAND ----------

sqlSafe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Query borrado

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_cartdet_crit_ent_ope_prin 
# MAGIC 
# MAGIC --20	CCN455                   
# MAGIC --01	CON731                   
# MAGIC --01	COM732                   
# MAGIC 
# MAGIC --30    CON580
# MAGIC --08    SGN001
# MAGIC 
# MAGIC 
# MAGIC --select * from tmp_RES_D00_OPE_CAMPO_EVAL 
# MAGIC 
# MAGIC select 
# MAGIC   * 
# MAGIC from 
# MAGIC   riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit 
# MAGIC where 
# MAGIC   criterio_salida = 27 
# MAGIC   and sistema = 01
# MAGIC   and tipo_operacion = 'COM732'
# MAGIC 
# MAGIC 
# MAGIC --    and sistema = 08
# MAGIC --    and tipo_operacion = 'SGN001'
# MAGIC --    and sistema in (30,08)
# MAGIC --    and tipo_operacion in ('CON580','SGN001')
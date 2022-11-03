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
# MAGIC * Nombre: BCI_28_CarDet_Gru_Crit_Sal_Crit.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/09/2022
# MAGIC * Descripcion: Calcula el deterioro principal de la operacion. 
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
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo
# MAGIC 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit
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
# MAGIC ### Extrae Criterios de Deterioro Salida Grupal

# COMMAND ----------


p_GruCritDetSal = (27,30,34,35,36,37,38,39,40,41)
p_GruCritDetSal


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Evaluacion de Criterios Deterioro Grupal (riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo)
# MAGIC --------------------------------------
# MAGIC - Obtiene todos los registros de la evaluacion grupal de salida del periodo actual

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_sal_ope_campo AS
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
  {base_silverX}.tbl_cartdet_crit_sal_ope_campo
WHERE
    fecha_cierre = {FechaX}  
AND periodo_cierre = {PeriodoX}  
AND tipo_proceso = '{tipo_procesoX}'
AND criterio_salida IN {p_GruCritDetSal}
"""  


# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_cartdet_crit_sal_ope_campo 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal Criterios Deterioro Por Operacion

# COMMAND ----------


paso_query20 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_ope_crit AS
select  
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
FROM 
    tmp_EXT_tbl_cartdet_crit_sal_ope_campo 
WHERE 
   flag_resultado_regla=1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
"""


# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borrado e insercion en Tabla (riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit)

# COMMAND ----------

paso_query30 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_sal_ope_crit where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} AND tipo_proceso = '{tipo_procesoX}' AND criterio_salida IN {p_GruCritDetSal} """


# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

paso_query40 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_sal_ope_crit
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
  tmp_tbl_cartdet_crit_sal_ope_crit 

"""


# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### QUERY (borrar) 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select count(1),criterio_salida,periodo_cierre from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit group by criterio_salida,periodo_cierre
# MAGIC -- no esta el 35 y 38
# MAGIC 
# MAGIC --select count(1),criterio_salida from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo  group by criterio_salida
# MAGIC 
# MAGIC --select  count(1),criterio_salida from tmp_EXT_tbl_cartdet_crit_sal_ope_campo group by criterio_salida
# MAGIC 
# MAGIC select count(1), criterio_salida from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit group by criterio_salida
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo  where criterio_salida = 38
# MAGIC 
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_cartdet_crit_sal_ope_campo where operacion = '3080209028' 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")
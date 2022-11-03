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
# MAGIC * Nombre: BCI_28_CarDet_Gru_Crit_Sal_Prin.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/09/2022
# MAGIC * Descripcion: Evaluacion final de criterio de salida de deterioro. 
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
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit
# MAGIC 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_prin
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
AND criterio_salida IN {p_GruCritDetSal}
"""  


# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal Criterios Salida Principal Por Operacion
# MAGIC --------------------------------------
# MAGIC * Genera salida temporal el criterio de salida principal por operacion. 

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_ope_prin as
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
    ,RANK() OVER (PARTITION BY operacion, sistema ORDER BY criterio_salida asc) AS id_rank
FROM
    tmp_EXT_tbl_cartdet_crit_sal_ope_crit
"""


# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_ope_uniq AS
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
  tmp_tbl_cartdet_crit_sal_ope_prin
WHERE
  id_rank=1
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
# MAGIC ### Borrado e insercion en Tabla (riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit)

# COMMAND ----------

paso_query40 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_sal_ope_prin where fecha_cierre = {FechaX} AND criterio_salida IN {p_GruCritDetSal} AND tipo_proceso = '{tipo_procesoX}' """


# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

paso_query50 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_sal_ope_prin
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
  tmp_tbl_cartdet_crit_sal_ope_uniq
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
# MAGIC ### Query (borrar)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1),criterio_salida from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_prin group by criterio_salida
# MAGIC 
# MAGIC --delete from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_prin where criterio_salida in (40,41)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin where rut_cliente = 13901002
# MAGIC 
# MAGIC 
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_prin where rut_cliente = 13901002

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select count(1),criterio_salida  from tmp_tbl_cartdet_crit_sal_ope_uniq group by criterio_salida
# MAGIC 
# MAGIC --select *  from tmp_tbl_cartdet_crit_sal_ope_uniq where criterio_salida = '27' 
# MAGIC 
# MAGIC --select *  from tmp_tbl_cartdet_crit_sal_ope_uniq where rut_cliente = 12439307
# MAGIC 
# MAGIC --select *  from tmp_EXT_tbl_cartdet_crit_sal_ope_crit where rut_cliente = 12439307
# MAGIC 
# MAGIC /*
# MAGIC SELECT 
# MAGIC   b.*
# MAGIC FROM
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_d00_segmentado b
# MAGIC WHERE 
# MAGIC   b.fecha_cierre = 20220630 AND b.periodo_cierre = 202206 AND b.tipo_proceso = 'C' AND b.rut_cliente = 12439307
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin where rut_cliente = 12439307
# MAGIC --select count(1) from tmp_tbl_cartdet_crit_sal_ope_prin
# MAGIC --500966
# MAGIC 
# MAGIC /*
# MAGIC select 
# MAGIC   a.rut_cliente 
# MAGIC   ,count(a.operacion) AS cant_det_total
# MAGIC   ,count(b.operacion) AS cant_ope_sal_total
# MAGIC from 
# MAGIC   riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin a
# MAGIC left join
# MAGIC   tmp_tbl_cartdet_crit_sal_ope_uniq b
# MAGIC on
# MAGIC   a.rut_cliente = b.rut_cliente
# MAGIC   and a.sistema = b.sistema
# MAGIC   and a.operacion = b.operacion
# MAGIC group by a.rut_cliente
# MAGIC */
# MAGIC 
# MAGIC --select * from tmp_tbl_cartdet_crit_sal_ope_uniq where rut_cliente = '12439307'
# MAGIC --262305
# MAGIC 
# MAGIC --select b.rut_cliente, count(b.operacion) AS cant_ope_sal_total from tmp_tbl_cartdet_crit_sal_ope_uniq b where rut_cliente = '12439307' group by b.rut_cliente
# MAGIC 
# MAGIC 
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin where rut_cliente =  7847712

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tbl_c4_hist_cart_det
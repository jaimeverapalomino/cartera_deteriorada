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

p_GruCritDetEnt = (7,8,9,10,11,12,13) 
p_GruCritDetSal = (27,30,34,35,36,37,38,39,40,41)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones Deterioradas Grupales (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)
# MAGIC --------------------------------------
# MAGIC - Se extrae Operaciones Deterioradas Grupales periodo actual

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_cant_ope AS
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
    {base_silverX}.tbl_cartdet_crit_ent_ope_prin
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
# MAGIC ### Extrae Evaluacion de Criterios Deterioro Grupal (riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo)
# MAGIC --------------------------------------
# MAGIC - Obtiene todos los registros de la evaluacion grupal de salida del periodo actual

# COMMAND ----------

paso_query20 = f"""
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
  ,RANK() OVER (PARTITION BY operacion, sistema ORDER BY criterio_salida asc) AS id_rank
FROM
  {base_silverX}.tbl_cartdet_crit_sal_ope_crit
WHERE
    fecha_cierre = {FechaX}  
AND periodo_cierre = {PeriodoX}  
AND tipo_proceso = '{tipo_procesoX}'
AND criterio_salida IN {p_GruCritDetSal}
"""  


# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal Criterios Salida Principal Por Operacion
# MAGIC --------------------------------------
# MAGIC * Genera salida temporal el criterio de salida principal por operacion. Aca debe aplicarse la jerarquia.

# COMMAND ----------

paso_query30 = f"""
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

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tmp_tbl_cartdet_crit_sal_ope_prin where rut_cliente = 13061139 and id_rank = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Tablas de Salidas
# MAGIC --------------------------------------
# MAGIC * carga resultados a tablas de salidas del notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borrado e insercion en Tabla (riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit)

# COMMAND ----------

paso_query30 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_sal_ope_prin where fecha_cierre = {FechaX} AND criterio_salida IN {p_GruCritDetSal} AND tipo_proceso = '{tipo_procesoX}' """


# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

paso_query40 = f"""
INSERT INTO riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_prin
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

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### QUERY Borrar

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC select 
# MAGIC   a.*,b.*
# MAGIC from 
# MAGIC   riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin a
# MAGIC Join  
# MAGIC   riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit b
# MAGIC ON
# MAGIC   a.rut_cliente = b.rut_cliente
# MAGIC   AND a.fecha_cierre = b.fecha_cierre
# MAGIC   AND a.periodo_cierre = b.periodo_cierre
# MAGIC WHERE
# MAGIC   a.fecha_cierre = 20220630  AND a.rut_cliente = 13061139 
# MAGIC */
# MAGIC 
# MAGIC --13061139
# MAGIC --12768731
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC /*
# MAGIC SELECT 
# MAGIC   a.rut_cliente
# MAGIC   ,COUNT(a.operacion) AS cant_det_tot
# MAGIC   ,SUM( 
# MAGIC       CASE when b.operacion IS NOT NULL THEN 1 ELSE 0 END
# MAGIC       ) AS cond_sal_tot
# MAGIC FROM
# MAGIC   tmp_EXT_tbl_cartdet_crit_ent_cant_ope a
# MAGIC LEFT JOIN
# MAGIC   tmp_EXT_tbl_cartdet_crit_sal_ope_crit b
# MAGIC ON
# MAGIC   a.rut_cliente = b.rut_cliente
# MAGIC   AND a.sistema = b.sistema
# MAGIC   AND a.operacion = b.operacion
# MAGIC where
# MAGIC   b.id_rank = 1
# MAGIC GROUP BY a.rut_cliente
# MAGIC */
# MAGIC 
# MAGIC --select COUNT(a.operacion) AS cant_det_tot from tmp_EXT_tbl_cartdet_crit_sal_ope_crit a WHERE a.rut_cliente = 13061139
# MAGIC 
# MAGIC 
# MAGIC --select rut_cliente,count(1)  from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin  group by rut_cliente having count(rut_cliente) = 5
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin where rut_cliente = 8613361
# MAGIC 
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit where rut_cliente = 8613361
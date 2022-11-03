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
# MAGIC * Nombre: BCI_xx_CarDet_Gru_Crit_Sal_Fam.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 06/10/2022
# MAGIC * Descripcion: Criterio de Salida Grupo de Familia, criterios (34,35,36,37,38,39).
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
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_sal_familia
# MAGIC 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo
# MAGIC 
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios
# MAGIC 
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

# MAGIC %run "../cartera_deteriorada/Notebook_Apoyo/BCI_AUX_Carga_Ingesta_Temporal_CD"

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/Jerarquia.txt")

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
# MAGIC ### Parametros Criterios de Salida

# COMMAND ----------

p_GruCritDetSal = 60,61,62,63,64,65,66,67


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones riesgobdu_silver_db.tbl_cartdet_crit_sal_familia para periodo actual
# MAGIC --------------------------------------
# MAGIC - Se extrae Operaciones Deterioradas Grupales periodo actual

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_gru_sal_fam as
SELECT 
   a.periodo_cierre          AS periodo_cierre
  ,a.fecha_cierre            AS fecha_cierre
  ,a.tipo_proceso            AS tipo_proceso
  ,a.rut_cliente             AS rut_cliente
  ,a.dv_rut_cliente          AS dv_rut_cliente
  ,a.tipo_operacion          AS tipo_operacion
  ,a.operacion               AS operacion
  ,a.sistema                 AS sistema
  ,a.segmento                AS segmento
  ,a.tipo_credito            AS tipo_credito
  ,a.familia_ope             AS familia_ope
  ,a.evaluacion              AS evaluacion
  ,a.resultado               AS resultado
FROM
  {base_silverX}.tbl_cartdet_crit_salida_familia a
WHERE 
    a.fecha_cierre = {FechaX}  
AND a.periodo_cierre = {PeriodoX}  
AND a.tipo_proceso = '{tipo_procesoX}'

"""

# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

paso_query20 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_condicion_salida AS
SELECT 
    a.familia_ope       AS familia_ope
   ,a.evaluacion        AS evaluacion 
   ,a.resultado         AS resultado
   ,a.criterio_salida   AS criterio_salida
   ,sum(a.resultado) over(partition by a.familia_ope )  AS cant_res
FROM
  tbl_condicion_salida a

"""   

# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Criterios de Salida por Familia

# COMMAND ----------

paso_query30 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_crit_gru_sal_familia AS
SELECT 
   a.periodo_cierre              AS periodo_cierre
   ,a.fecha_cierre               AS fecha_cierre
   ,a.tipo_proceso               AS tipo_proceso
   ,a.rut_cliente                AS rut_cliente
   ,a.dv_rut_cliente             AS dv_rut_cliente
   ,a.tipo_operacion             AS tipo_operacion
   ,a.operacion                  AS operacion
   ,a.sistema                    AS sistema
   ,a.segmento                   AS segmento
   ,a.tipo_credito               AS tipo_credito
   ,a.familia_ope                AS familia_ope
   ,a.evaluacion                 AS evaluacion
   ,a.resultado                  AS resultado
   ,b.familia_ope                AS par_familia_ope
   ,b.evaluacion                 AS par_evaluacion
   ,b.resultado                  AS par_resultado
   ,b.criterio_salida            AS criterio_salida
   ,case when a.resultado = b.resultado then 1 else 0 end                                                 AS ind_cumple_eval
   ,SUM (case when a.resultado = b.resultado then 1 else 0 end) over (partition by a.operacion,a.sistema) AS cant_cumple_sal  
   ,SUM (b.resultado) over (partition by a.operacion,a.sistema)                                           AS cant_espe_cumple_sal
FROM
  tmp_EXT_tbl_cartdet_crit_gru_sal_fam a
FULL JOIN
  tmp_EXT_tbl_condicion_salida b
ON
   a.familia_ope = b.familia_ope
 AND  a.evaluacion = b.evaluacion
WHERE
    b.resultado = 1
"""

# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal con criterio para insertar en riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo

# COMMAND ----------

paso_query40 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_crit_gru_sal_fam_resul AS
SELECT 
   a.periodo_cierre             AS periodo_cierre
  ,a.fecha_cierre               AS fecha_cierre
  ,a.tipo_proceso               AS tipo_proceso
  ,a.rut_cliente                AS rut_cliente
  ,a.operacion                  AS operacion
  ,a.dv_rut_cliente             AS dv_rut_cliente
  ,a.tipo_operacion             AS tipo_operacion
  ,a.sistema                    AS sistema
  ,a.segmento                   AS segmento 
  ,a.tipo_credito               AS tipo_credito
  ,a.criterio_salida            AS criterio_salida
   ,max(a.familia_ope)          AS nombre_campo
   ,CAST(max(a.cant_cumple_sal) AS string) as valor_campo 
   ,' = '                                            AS condicion_regla  
   ,CAST(max(a.cant_espe_cumple_sal) AS string)       AS valor_regla
   ,max(CASE WHEN a.cant_cumple_sal = a.cant_espe_cumple_sal THEN 1 ELSE 0 END) AS flag_resultado_regla
FROM
  tmp_tbl_crit_gru_sal_familia a
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
"""


# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borrado e insercion en Tabla (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)

# COMMAND ----------

paso_query50 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_sal_ope_campo where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} AND tipo_proceso = '{tipo_procesoX}' AND criterio_salida IN {p_GruCritDetSal} """


# COMMAND ----------

sqlSafe(paso_query50)

# COMMAND ----------

paso_query80 = f"""
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
  ,'0'                 AS origen_deterioro
  ,{FechaX}            AS fecha_salida
  ,nombre_campo
  ,valor_campo 
  ,condicion_regla  
  ,valor_regla
  ,flag_resultado_regla
FROM
  tmp_tbl_crit_gru_sal_fam_resul  

"""  

# COMMAND ----------

sqlSafe(paso_query80)

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
# MAGIC /*
# MAGIC select 
# MAGIC   *
# MAGIC FROM
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_d00_segmentado b
# MAGIC WHERE 
# MAGIC     b.fecha_cierre = 20220630 
# MAGIC   and b.rut_cliente = 17974334 
# MAGIC   */
# MAGIC   
# MAGIC   select count(1),criterio_salida from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo where flag_resultado_regla = 1 group by criterio_salida

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from tmp_tbl_crit_gru_sal_familia  where operacion = 'D22307200289' and sistema = '20'
# MAGIC 
# MAGIC 
# MAGIC --select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo where criterio_salida = 27 and flag_resultado_regla = 1 
# MAGIC --select count(1),criterio_salida from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo group by criterio_salida 
# MAGIC --## 209070
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_salida_familia where rut_cliente = 15305374
# MAGIC 
# MAGIC 
# MAGIC select * from tmp_tbl_crit_gru_sal_fam_resul  
# MAGIC 
# MAGIC --select criterio_entrada from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit group by criterio_entrada
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC --select * from tmp_tbl_crit_gru_sal_fam_resul where operacion = 'D22307200289' and sistema = '20' 
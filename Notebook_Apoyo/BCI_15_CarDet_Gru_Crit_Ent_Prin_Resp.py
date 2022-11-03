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
# MAGIC * Nombre: BCI_15_CarDet_Gru_Crit_Ent_Prin.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 06/09/2022
# MAGIC * Descripcion: Calcula el deterioro principal de deterioro Grupal por operacion.
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
# MAGIC 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin
# MAGIC 
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
# MAGIC ### Extrae Criterios de Deterioro Entrada Grupal

# COMMAND ----------

p_GruCritDetEnt = (7,8,9,10,11,12,13)
p_GruCritDetEnt


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Evaluacion de Criterios Deterioro Grupal (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)
# MAGIC --------------------------------------
# MAGIC - Obtiene todos los registros de la evaluacion grupal de entrada del periodo actual

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_ope_crit AS
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
AND criterio_entrada  IN {p_GruCritDetEnt}  
""" 


# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado AS
SELECT 
  b.periodo_cierre                      AS periodo_cierre,
  b.fecha_cierre                        AS fecha_cierre,
  b.tipo_proceso                        AS tipo_proceso,
  b.cod_segmento                        AS cod_segmento,
  b.operacion                           AS operacion,
  b.cod_sistema                         AS cod_sistema,
  b.rut_cliente                         AS rut_cliente,
  b.dv_cliente                          AS dv_cliente,
  b.cuenta_contable                     AS cuenta_contable,
  b.saldo_cartera_venc                  AS saldo_cartera_venc,
  b.dias_mora                           AS dias_mora,
  b.tipo_operacion                      AS tipo_operacion,
  b.tipo_credito                        AS tipo_credito,
  '19000101'                            AS fec_cart_ven
FROM
  {base_goldX}.tbl_segmentacion_d00_segmentado b
WHERE 
  b.fecha_cierre = {FechaX}
  AND b.periodo_cierre = {PeriodoX}
  AND b.tipo_proceso = '{tipo_procesoX}'
  AND b.cod_segmento = 'G'
"""

# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Aplica Jerarquia por Operacion para determinar el motivo deterioro final por operacion

# COMMAND ----------


paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_crit_prin_ope AS
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
  ,row_number() over(partition by a.operacion,a.sistema order by b.jerarquia asc ) AS Min_id
FROM 
  tmp_EXT_tbl_cartdet_crit_ent_ope_crit a
LEFT JOIN
  tbl_jer_cri_ent b
ON
  a.criterio_entrada = b.criterio
"""
 

# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Tabla con cirterio principal de deterioro por operación

# COMMAND ----------

paso_query40 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_crit_prin_ope_uniq AS
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
  tmp_EXT_tbl_crit_prin_ope 
WHERE 
  Min_id = 1
"""

# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------


paso_query50 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_crit_prin_cli AS
SELECT    
   periodo_cierre    
  ,fecha_cierre 
  ,tipo_proceso 
  ,rut_cliente 
  ,dv_rut_cliente
  ,criterio_entrada 
  ,row_number() over(partition by a.rut_cliente order by b.jerarquia asc ) AS Min_id
FROM 
  tmp_EXT_tbl_crit_prin_ope_uniq a
LEFT JOIN
  tbl_jer_cri_ent b
ON
  a.criterio_entrada = b.criterio
"""

# COMMAND ----------

sqlSafe(paso_query50)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_crit_prin_cli_uniq where rut_cliente = 12439307

# COMMAND ----------

paso_query60 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_crit_prin_cli_uniq AS
SELECT    
   periodo_cierre    
  ,fecha_cierre 
  ,tipo_proceso 
  ,rut_cliente 
  ,dv_rut_cliente
  ,criterio_entrada 
FROM 
  tmp_EXT_tbl_crit_prin_cli
WHERE
  Min_id = 1
"""

# COMMAND ----------

sqlSafe(paso_query60)

# COMMAND ----------


paso_query70 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_crit_prin_ope_sal AS

select
   a.periodo_cierre      AS periodo_cierre
  ,a.fecha_cierre        AS fecha_cierre
  ,a.tipo_proceso        AS tipo_proceso
  ,a.rut_cliente         AS rut_cliente 
  ,a.dv_cliente          AS dv_rut_cliente
  ,a.tipo_operacion      AS tipo_operacion
  ,a.operacion           AS operacion     
  ,a.cod_sistema         AS sistema       
  ,a.cod_segmento        AS segmento 
  ,b.criterio_entrada    AS criterio_entrada 
  ,CASE WHEN c.operacion IS NOT NULL THEN c.origen_deterioro ELSE '5' END AS origen_deterioro  
  ,CASE WHEN c.operacion IS NOT NULL THEN c.fecha_entrada ELSE a.fecha_cierre END AS fecha_entrada
  ,CASE WHEN c.operacion IS NOT NULL THEN c.criterio_entrada ELSE ' ' END     AS criterio_operacion
FROM 
  tmp_EXT_tbl_segmentacion_d00_segmentado a
left join
  tmp_EXT_tbl_crit_prin_cli_uniq b
ON 
  a.rut_cliente = b.rut_cliente  
left join
  tmp_EXT_tbl_crit_prin_ope_uniq c
on
  a.operacion = c.operacion
  and a.cod_sistema = c.sistema
where 
  b.rut_cliente IS NOT NULL /*Solo para cliente deteriorado */
 
"""


# COMMAND ----------

sqlSafe(paso_query70)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1) from tmp_EXT_tbl_crit_prin_ope_sal 

# COMMAND ----------

# MAGIC %md
# MAGIC ### tmp_EXT_tbl_crit_prin_ori

# COMMAND ----------

paso_query40 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_crit_prin_ori AS
SELECT    
   a.periodo_cierre      AS periodo_cierre
  ,a.fecha_cierre        AS fecha_cierre
  ,a.tipo_proceso        AS tipo_proceso
  ,a.rut_cliente         AS rut_cliente 
  ,a.dv_rut_cliente      AS dv_rut_cliente
  ,a.tipo_operacion      AS tipo_operacion
  ,a.operacion           AS operacion     
  ,a.sistema             AS sistema       
  ,a.segmento            AS segmento      
  ,b.criterio_entrada    AS criterio_entrada 
  ,a.origen_deterioro    AS origen_deterioro
  ,a.fecha_entrada       AS fecha_entrada
FROM 
  tmp_EXT_tbl_cartdet_crit_ent_ope_crit a
LEFT JOIN
  tmp_EXT_tbl_crit_prin b
ON
  a.rut_cliente = b.rut_cliente
Where 
  a.rut_cliente = 12439307
  and b.Min_id = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
"""


# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_crit_prin_ori  where rut_cliente = 12439307
# MAGIC 
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_cartdet_crit_ent_ope_crit where rut_cliente = 12439307 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC SELECT    
# MAGIC    a.periodo_cierre      AS periodo_cierre
# MAGIC   ,a.fecha_cierre        AS fecha_cierre
# MAGIC   ,a.tipo_proceso        AS tipo_proceso
# MAGIC   ,a.rut_cliente         AS rut_cliente 
# MAGIC   ,a.dv_rut_cliente      AS dv_rut_cliente
# MAGIC   ,a.tipo_operacion      AS tipo_operacion
# MAGIC   ,a.operacion           AS operacion     
# MAGIC   ,a.sistema             AS sistema       
# MAGIC   ,a.segmento            AS segmento      
# MAGIC   ,a.criterio_entrada    AS criterio_entrada 
# MAGIC   ,a.origen_deterioro    AS origen_deterioro
# MAGIC   ,a.fecha_entrada       AS fecha_entrada
# MAGIC FROM 
# MAGIC   tmp_EXT_tbl_crit_prin_ori a
# MAGIC GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
# MAGIC 
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extarae las operaciones que se irradian
# MAGIC --------------------------------------

# COMMAND ----------

# MAGIC %sql
# MAGIC --paso_query50 = f"""
# MAGIC --CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_prueba AS
# MAGIC SELECT
# MAGIC    B.periodo_cierre    
# MAGIC   ,B.fecha_cierre 
# MAGIC   ,B.tipo_proceso 
# MAGIC   ,B.rut_cliente 
# MAGIC   ,B.dv_cliente
# MAGIC   ,B.tipo_operacion
# MAGIC   ,B.operacion 
# MAGIC   ,B.cod_sistema   
# MAGIC   ,B.cod_segmento 
# MAGIC   ,CASE WHEN A.operacion IS NULL THEN A.criterio_entrada ELSE A.criterio_entrada END AS criterio_entrada 
# MAGIC   ,CASE WHEN A.operacion IS NULL THEN B.origen_deterioro ELSE A.origen_deterioro END AS origen_deterioro  
# MAGIC   ,'20220630' AS fecha_entrada
# MAGIC FROM
# MAGIC   tmp_EXT_tbl_segmentacion_d00_segmentado B
# MAGIC LEFT JOIN 
# MAGIC   tmp_EXT_tbl_crit_prin_ori A  
# MAGIC ON
# MAGIC   A.operacion = B.operacion
# MAGIC   AND A.sistema =  B.cod_sistema
# MAGIC WHERE
# MAGIC   B.rut_cliente  = 12439307
# MAGIC --GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC 
# MAGIC    a.periodo_cierre      AS periodo_cierre
# MAGIC   ,a.fecha_cierre        AS fecha_cierre
# MAGIC   ,a.tipo_proceso        AS tipo_proceso
# MAGIC   ,a.rut_cliente         AS rut_cliente 
# MAGIC   ,a.dv_cliente          AS dv_rut_cliente
# MAGIC   ,a.tipo_operacion      AS tipo_operacion
# MAGIC   ,a.operacion           AS operacion     
# MAGIC   ,a.cod_sistema         AS sistema       
# MAGIC   ,a.cod_segmento        AS segmento 
# MAGIC   ,CASE WHEN b.operacion IS NULL THEN c.criterio_entrada ELSE b.criterio_entrada END AS criterio_entrada 
# MAGIC   ,CASE WHEN b.operacion IS NULL THEN a.origen_deterioro ELSE b.origen_deterioro END AS origen_deterioro  
# MAGIC --  ,CASE WHEN a.fecha_cartdet_ope = '19000101' THEN '20220630' ELSE a.fecha_cartdet_ope END AS fecha_entrada 
# MAGIC FROM
# MAGIC   tmp_EXT_tbl_segmentacion_d00_segmentado a
# MAGIC LEFT JOIN
# MAGIC   tmp_EXT_tbl_cartdet_crit_ent_ope_crit b
# MAGIC ON 
# MAGIC     A.operacion = B.operacion
# MAGIC   AND A.cod_sistema =  B.sistema
# MAGIC JOIN
# MAGIC   tmp_EXT_tbl_crit_prin c
# MAGIC ON
# MAGIC   a.rut_cliente = c.rut_cliente
# MAGIC Where 
# MAGIC   a.rut_cliente = 12439307
# MAGIC GROUP BY 1,2,3,4,5,6,7,8,9,10,11

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_crit_prin_ori where rut_cliente = 12439307

# COMMAND ----------

sqlSafe(paso_query50)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_cartdet_crit_ent_ope_crit_irra  where rut_cliente = 12439307 
# MAGIC 
# MAGIC select * from  riesgobdu_golden_db.tbl_segmentacion_d00_segmentado where rut_cliente = 12439307

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal Criterios Deterioro Por Operacion

# COMMAND ----------

paso_query40 = """
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
  A.fecha_entrada          AS fecha_entrada
FROM 
  tmp_EXT_tbl_cartdet_crit_ent_ope_crit A
"""


# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borrado e insercion en Tabla (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)

# COMMAND ----------

paso_query30 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_prin where fecha_cierre = {FechaX} AND criterio_entrada in {p_GruCritDetEnt} """


# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin

# COMMAND ----------

paso_query40 = f"""
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
FROM
  tmp_tbl_cartdet_crit_ent_ope_prin 
WHERE 
  id_rank = 1 
"""  

# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin where rut_cliente = '12085834'
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC --select criterio, jerarquia from tbl_jerarquia
# MAGIC /*
# MAGIC SELECT 
# MAGIC    a.rut_cliente
# MAGIC    ,b.criterio
# MAGIC FROM
# MAGIC   riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin a,
# MAGIC   (select criterio, min(jerarquia) from tbl_jerarquia group by 1) b
# MAGIC where
# MAGIC   a.criterio_entrada = b.criterio
# MAGIC and rut_cliente = '12085834'
# MAGIC */
# MAGIC 
# MAGIC /*
# MAGIC update riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin 
# MAGIC set criterio_entrada = 9
# MAGIC where rut_cliente = '12085834'
# MAGIC and tipo_operacion = 'CON731'
# MAGIC */
# MAGIC --'12085834' 9 y 7 
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo where rut_cliente = '12085834' 
# MAGIC 
# MAGIC /*
# MAGIC SELECT
# MAGIC    periodo_cierre    
# MAGIC   ,fecha_cierre 
# MAGIC   ,tipo_proceso 
# MAGIC   ,rut_cliente 
# MAGIC   ,dv_cliente
# MAGIC   ,tipo_operacion
# MAGIC   ,operacion 
# MAGIC   ,sistema   
# MAGIC   ,segmento 
# MAGIC   ,criterio_entrada 
# MAGIC   ,origen_deterioro 
# MAGIC   ,fecha_entrada 
# MAGIC from */
# MAGIC 
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where rut_cliente = 7430953
# MAGIC 
# MAGIC --select * from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado  where rut_cliente = 7430953 and fecha_cierre = 20220630

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Query Pruebas (borrar)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT    
# MAGIC   a.rut_cliente
# MAGIC   ,b.criterio
# MAGIC   ,row_number() over(partition by a.rut_cliente order by b.jerarquia asc ) AS Min_id
# MAGIC FROM 
# MAGIC   tmp_EXT_tbl_cartdet_crit_ent_ope_crit a
# MAGIC LEFT JOIN
# MAGIC   tbl_jer_cri_ent b
# MAGIC ON
# MAGIC   a.criterio_entrada = b.criterio
# MAGIC Where 
# MAGIC   a.rut_cliente = 12439307
# MAGIC --group by 1,2,3
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_cartdet_crit_ent_ope_crit where rut_cliente = 12439307
# MAGIC 
# MAGIC --select criterio, jerarquia from tbl_jer_cri_ent
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_cartdet_crit_ent_ope_crit_irra  where rut_cliente = 12439307 
# MAGIC 
# MAGIC 
# MAGIC SELECT  
# MAGIC   a.rut_cliente
# MAGIC   ,b.criterio
# MAGIC FROM 
# MAGIC   tmp_EXT_tbl_cartdet_crit_ent_ope_crit_irra a,
# MAGIC   ( select criterio, jerarquia from tbl_jer_cri_ent ) b
# MAGIC WHERE
# MAGIC   a.criterio_entrada = b.criterio
# MAGIC   GROUP BY a.rut_cliente,a.criterio_entrada
# MAGIC   HAVING b.jerarquia = min(b.jerarquia)
# MAGIC 
# MAGIC 
# MAGIC --select criterio, jerarquia from tbl_jer_cri_ent
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC SELECT
# MAGIC   a.rut_cliente
# MAGIC   ,b.criterio
# MAGIC   ,b.jerarquia
# MAGIC FROM
# MAGIC   tmp_EXT_tbl_cartdet_crit_ent_ope_crit a
# MAGIC JOIN
# MAGIC   (SELECT 
# MAGIC     criterio,
# MAGIC     jerarquia 
# MAGIC   FROM 
# MAGIC     tbl_jer_cri_ent) b
# MAGIC ON
# MAGIC   a.criterio_entrada = b.criterio
# MAGIC WHERE
# MAGIC   a.rut_cliente = 12439307 
# MAGIC   
# MAGIC    
# MAGIC /*  
# MAGIC SELECT 
# MAGIC     criterio,
# MAGIC      ROW_NUMBER() OVER(ORDER BY jerarquia) AS JERAR 
# MAGIC FROM 
# MAGIC     tbl_jer_cri_ent  
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC select criterio,min(jerarquia) over(partition by criterio) AS MinJerarquia from tbl_jer_cri_ent 
# MAGIC 
# MAGIC 
# MAGIC SELECT  
# MAGIC   a.rut_cliente
# MAGIC   ,b.criterio
# MAGIC   ,min(b.jerarquia) over(partition by a.rut_cliente,b.criterio,b.jerarquia) AS MinJerarquia
# MAGIC FROM 
# MAGIC   tmp_EXT_tbl_cartdet_crit_ent_ope_crit_irra a
# MAGIC LEFT JOIN
# MAGIC   tbl_jer_cri_ent b
# MAGIC ON
# MAGIC   a.criterio_entrada = b.criterio
# MAGIC WHERE
# MAGIC   a.rut_cliente = 12439307
# MAGIC   
# MAGIC   *********************************************************************
# MAGIC   
# MAGIC SELECT    
# MAGIC    a.periodo_cierre      AS periodo_cierre
# MAGIC   ,a.fecha_cierre        AS fecha_cierre
# MAGIC   ,a.tipo_proceso        AS tipo_proceso
# MAGIC   ,a.rut_cliente         AS rut_cliente 
# MAGIC   ,a.dv_rut_cliente      AS dv_rut_cliente
# MAGIC   ,a.tipo_operacion      AS tipo_operacion
# MAGIC   ,a.operacion           AS operacion     
# MAGIC   ,a.sistema             AS sistema       
# MAGIC   ,a.segmento            AS segmento      
# MAGIC   ,b.criterio            AS criterio_entrada 
# MAGIC   ,'5'                   AS origen_deterioro
# MAGIC   ,a.fecha_entrada       AS fecha_entrada
# MAGIC   ,row_number() over(partition by a.rut_cliente order by b.jerarquia asc ) AS Min_id
# MAGIC FROM 
# MAGIC   tmp_EXT_tbl_cartdet_crit_ent_ope_crit a
# MAGIC LEFT JOIN
# MAGIC   tbl_jer_cri_ent b
# MAGIC ON
# MAGIC   a.criterio_entrada = b.criterio
# MAGIC Where 
# MAGIC   a.rut_cliente = 12439307
# MAGIC   
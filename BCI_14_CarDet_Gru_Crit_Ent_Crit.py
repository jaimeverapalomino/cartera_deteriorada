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
# MAGIC * Nombre: BCI_14_CarDet_Gru_Crit_Ent_Crit.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 06/09/2022
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
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo
# MAGIC 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit
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
# MAGIC ### Extrae Criterios de Deterioro Entrada Grupal

# COMMAND ----------


p_GruCritDetEnt = (7,8,9,10,11,12,13)
p_GruCritDetEnt


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Evaluacion de Criterios Deterioro Grupal (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo)
# MAGIC --------------------------------------
# MAGIC - Obtiene todos los registros de la evaluacion grupal de entrada del periodo actual

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_ope_campo AS
SELECT
	 periodo_cierre                  AS  periodo_cierre    
	,fecha_cierre                    AS  fecha_cierre 
	,tipo_proceso                    AS  tipo_proceso 
	,rut_cliente                     AS  rut_cliente 
	,dv_rut_cliente                  AS  dv_rut_cliente 
	,tipo_operacion                  AS  tipo_operacion
	,operacion                       AS  operacion 
	,sistema                         AS  sistema   
	,segmento                        AS  segmento 
	,criterio_entrada                AS  criterio_entrada 
	,origen_deterioro                AS  origen_deterioro 
	,fecha_entrada                   AS  fecha_entrada 
	,nombre_campo                    AS  nombre_campo
	,valor_campo                     AS  valor_campo 
	,condicion_regla                 AS  condicion_regla  
	,valor_regla                     AS  valor_regla
	,flag_resultado_regla            AS  flag_resultado_regla
FROM
  {base_silverX}.tbl_cartdet_crit_ent_ope_campo
WHERE
    fecha_cierre = {FechaX}  
AND periodo_cierre = {PeriodoX}  
AND tipo_proceso = '{tipo_procesoX}' 
AND criterio_entrada IN {p_GruCritDetEnt}
"""  


# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal Criterios Deterioro Por Operacion

# COMMAND ----------

paso_query20 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_ent_ope_crit AS
select  
	 periodo_cierre           AS   periodo_cierre  
	,fecha_cierre             AS   fecha_cierre 
	,tipo_proceso             AS   tipo_proceso 
	,rut_cliente              AS   rut_cliente 
	,dv_rut_cliente           AS   dv_rut_cliente
	,tipo_operacion           AS   tipo_operacion
	,operacion                AS   operacion 
	,sistema                  AS   sistema   
	,segmento                 AS   segmento 
	,criterio_entrada         AS   criterio_entrada
	,origen_deterioro         AS   origen_deterioro
	,fecha_entrada            AS   fecha_entrada 
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_ope_campo
WHERE 
   flag_resultado_regla=1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
"""


# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stock de operaciones deterioradas del cierre anterior

# COMMAND ----------

paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado_pant as

SELECT
	 periodo_cierre             as periodo_cierre
	,fecha_cierre               as fecha_cierre
	,tipo_proceso               as tipo_proceso
	,rut_cliente                as rut_cliente
	,dv_cliente                 as dv_cliente
	,tipo_operacion             as tipo_operacion
	,operacion                  as operacion
	,cod_sistema                as sistema
	,cod_segmento               as segmento
	,cod_cartdet_cliente        as origen_deterioro
	,cod_motivo_cartdet         as criterio_entrada
	,fecha_cartdet_ope	        as fecha_entrada
	,ind_cartdet                as ind_cartdet
FROM 
	{base_goldX}.tbl_segmentacion_d00_segmentado
WHERE
    trim(cod_motivo_cartdet) in {p_GruCritDetEnt}
    AND tipo_proceso = '{tipo_procesoX}'
    AND fecha_cierre=20220531    
"""

# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Salida Temporal (tmp_tbl_cartdet_crit_ent_ope_crit_act)

# COMMAND ----------

paso_query40 = """
CREATE OR REPLACE TEMPORARY VIEW  tmp_tbl_cartdet_crit_ent_ope_crit_act AS 
select  
	 a.periodo_cierre
	,a.fecha_cierre 
	,a.tipo_proceso 
	,a.rut_cliente 
	,a.dv_rut_cliente
	,a.tipo_operacion
	,a.operacion 
	,a.sistema   
	,a.segmento 
	,a.criterio_entrada 
	,a.origen_deterioro 
	,CASE WHEN b.ind_cartdet IS NULL THEN 'D' ELSE b.ind_cartdet END AS ind_cartdet
	,CASE WHEN b.operacion IS NOT NULL and b.ind_cartdet='D' THEN b.fecha_entrada else a.fecha_entrada END AS fecha_entrada
FROM 
    tmp_RES_cartdet_crit_ent_ope_crit a 
  left join
    tmp_EXT_tbl_segmentacion_d00_segmentado_pant b
ON
  a.operacion = b.operacion
 AND a.sistema = b.sistema
""" 

# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal (tmp_tbl_cartdet_crit_ent_ope_crit_stk)

# COMMAND ----------

paso_query50 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_tbl_cartdet_crit_ent_ope_crit_stk AS 
select  
	 {PeriodoX}                  AS periodo_cierre   
	,{FechaX}                    AS fecha_cierre
	,trim(a.tipo_proceso)        AS tipo_proceso  
	,a.rut_cliente               AS rut_cliente
	,trim(a.dv_cliente)          AS dv_rut_cliente
	,trim(a.tipo_operacion)      AS tipo_operacion
	,trim(a.operacion)           AS operacion
	,trim(a.sistema)             AS sistema
	,trim(a.segmento)            AS segmento
	,trim(a.criterio_entrada)    AS criterio_entrada
	,trim(a.origen_deterioro)    AS origen_deterioro
	,a.fecha_entrada             AS fecha_entrada
FROM 
    tmp_EXT_tbl_segmentacion_d00_segmentado_pant a
  left join
    tmp_tbl_cartdet_crit_ent_ope_crit_act b
ON
     a.operacion = b.operacion
 AND a.sistema = b.sistema
WHERE
     a.ind_cartdet = 'D'
AND  a.criterio_entrada in {p_GruCritDetEnt} 
AND  b.operacion IS NULL
""" 

# COMMAND ----------

sqlSafe(paso_query50)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Salida deterioro Actual y Stock
# MAGIC ---
# MAGIC * tabla temporal que contiene todas los criterios de deterioro que pueda tener una operacion

# COMMAND ----------

paso_query60 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_uni_cartdet_crit_ent_ope_crit AS 
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
  tmp_tbl_cartdet_crit_ent_ope_crit_act A
UNION  
SELECT
  B.periodo_cierre         AS periodo_cierre,
  B.fecha_cierre           AS fecha_cierre,
  B.tipo_proceso           AS tipo_proceso,
  B.rut_cliente            AS rut_cliente,
  B.dv_rut_cliente         AS dv_rut_cliente,
  B.tipo_operacion         AS tipo_operacion,  
  B.operacion              AS operacion,
  B.sistema                AS sistema,
  B.segmento               AS segmento,
  B.criterio_entrada       AS criterio_entrada,
  B.origen_deterioro       AS origen_deterioro,
  B.fecha_entrada          AS fecha_entrada
FROM
  tmp_tbl_cartdet_crit_ent_ope_crit_stk B
""" 

# COMMAND ----------

sqlSafe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Criterio Deterioro (tmp_tbl_cartdet_crit_ent_ope_crit)
# MAGIC ---
# MAGIC * tabla temporal que contiene todas los criterios de deterioro que pueda tener una operacion
# MAGIC * para individual solo hay un criterio de deterioro
# MAGIC * nos aseguramos que solo la llave sea operacion, sistema, criterio entrada

# COMMAND ----------

paso_query70 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_tbl_cartdet_crit_ent_ope_crit AS 
SELECT
  A.periodo_cierre             AS  periodo_cierre ,
  A.fecha_cierre               AS  fecha_cierre,
  A.tipo_proceso               AS  tipo_proceso,
  A.rut_cliente                AS  rut_cliente,
  A.dv_rut_cliente             AS  dv_rut_cliente,
  A.tipo_operacion             AS  tipo_operacion,
  A.operacion                  AS  operacion,
  A.sistema                    AS  sistema,
  A.segmento                   AS  segmento,
  A.criterio_entrada           AS  criterio_entrada,
  A.origen_deterioro           AS  origen_deterioro,
  A.fecha_entrada              AS  fecha_entrada,
  RANK() OVER(PARTITION BY A.operacion, A.sistema, A.criterio_entrada ORDER BY A.origen_deterioro ASC) AS rank_id
FROM   
  tmp_RES_uni_cartdet_crit_ent_ope_crit A
"""  

# COMMAND ----------

sqlSafe(paso_query70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borrado e insercion en Tabla (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)

# COMMAND ----------

paso_query80 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_crit where fecha_cierre = {FechaX} AND criterio_entrada IN {p_GruCritDetEnt} AND tipo_proceso = '{tipo_procesoX}' """


# COMMAND ----------

sqlSafe(paso_query80)

# COMMAND ----------

paso_query90 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_ent_ope_crit
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
  tmp_tbl_cartdet_crit_ent_ope_crit 
WHERE 
  rank_id = 1
"""


# COMMAND ----------

sqlSafe(paso_query90)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where criterio_entrada = 7 --113408
# MAGIC 
# MAGIC --select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where criterio_entrada = 8 --70372
# MAGIC 
# MAGIC --select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where criterio_entrada = 9 --12375
# MAGIC 
# MAGIC --select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where criterio_entrada = 10 --6705
# MAGIC 
# MAGIC --select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where criterio_entrada = 11 --8883 
# MAGIC 
# MAGIC --select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where criterio_entrada = 12 --20861
# MAGIC 
# MAGIC --select count(1),criterio_entrada from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit group by criterio_entrada
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo where rut_cliente = '12085834'
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where rut_cliente = '12085834'
# MAGIC 
# MAGIC select count(1),criterio_entrada from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit group by criterio_entrada
# MAGIC 
# MAGIC /*
# MAGIC count(1)	criterio_entrada
# MAGIC   113408    	7
# MAGIC   70372	        8
# MAGIC   12375	        9
# MAGIC   6705	        10
# MAGIC   8883	        11
# MAGIC   20861	        12
# MAGIC   314	        13
# MAGIC */
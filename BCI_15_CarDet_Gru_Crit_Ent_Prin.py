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
# MAGIC ### Extrae Evaluacion de Criterios Deterioro Grupal (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)
# MAGIC --------------------------------------
# MAGIC - Obtiene todos los registros de la evaluacion grupal de entrada del periodo actual

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_ope_crit AS
SELECT
   periodo_cierre            AS   periodo_cierre  
  ,fecha_cierre              AS   fecha_cierre 
  ,tipo_proceso              AS   tipo_proceso 
  ,rut_cliente               AS   rut_cliente 
  ,dv_rut_cliente            AS   dv_rut_cliente
  ,tipo_operacion            AS   tipo_operacion
  ,operacion                 AS   operacion 
  ,sistema                   AS   sistema   
  ,segmento                  AS   segmento 
  ,criterio_entrada          AS   criterio_entrada
  ,origen_deterioro          AS   origen_deterioro
  ,fecha_entrada             AS   fecha_entrada 
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
    b.fecha_cierre =  {FechaX}
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
   periodo_cierre               AS   periodo_cierre   
  ,fecha_cierre                 AS   fecha_cierre 
  ,tipo_proceso                 AS   tipo_proceso 
  ,rut_cliente                  AS   rut_cliente 
  ,dv_rut_cliente               AS   dv_rut_cliente
  ,tipo_operacion               AS   tipo_operacion
  ,operacion                    AS   operacion 
  ,sistema                      AS   sistema   
  ,segmento                     AS   segmento 
  ,criterio_entrada             AS   criterio_entrada 
  ,origen_deterioro             AS   origen_deterioro 
  ,fecha_entrada                AS   fecha_entrada 
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
   periodo_cierre              AS  periodo_cierre    
  ,fecha_cierre                AS  fecha_cierre 
  ,tipo_proceso                AS  tipo_proceso 
  ,rut_cliente                 AS  rut_cliente 
  ,dv_rut_cliente              AS  dv_rut_cliente
  ,tipo_operacion              AS  tipo_operacion
  ,operacion                   AS  operacion 
  ,sistema                     AS  sistema   
  ,segmento                    AS  segmento 
  ,criterio_entrada            AS  criterio_entrada
  ,origen_deterioro            AS  origen_deterioro
  ,fecha_entrada               AS  fecha_entrada 
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
   periodo_cierre          AS periodo_cierre            
  ,fecha_cierre            AS fecha_cierre 
  ,tipo_proceso            AS tipo_proceso 
  ,rut_cliente             AS rut_cliente 
  ,dv_rut_cliente          AS dv_rut_cliente 
  ,criterio_entrada        AS criterio_entrada    
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

paso_query60 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_crit_prin_cli_uniq AS
SELECT    
   periodo_cierre          AS periodo_cierre          
  ,fecha_cierre            AS fecha_cierre 
  ,tipo_proceso            AS tipo_proceso 
  ,rut_cliente             AS rut_cliente 
  ,dv_rut_cliente          AS dv_rut_cliente
  ,criterio_entrada        AS criterio_entrada  
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

# MAGIC %md
# MAGIC ### Borrado e insercion en Tabla (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)

# COMMAND ----------

paso_query80 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_prin where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} AND criterio_entrada in {p_GruCritDetEnt} AND tipo_proceso = '{tipo_procesoX}' """


# COMMAND ----------

sqlSafe(paso_query80)

# COMMAND ----------


paso_query90 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_ent_ope_prin
SELECT 
  periodo_cierre       AS periodo_cierre           
  ,fecha_cierre        AS fecha_cierre      
  ,tipo_proceso        AS tipo_proceso      
  ,rut_cliente         AS rut_cliente       
  ,dv_rut_cliente      AS dv_rut_cliente    
  ,tipo_operacion      AS tipo_operacion    
  ,operacion           AS operacion         
  ,sistema             AS sistema           
  ,segmento            AS segmento          
  ,criterio_entrada    AS criterio_entrada  
  ,origen_deterioro    AS origen_deterioro  
  ,fecha_entrada       AS fecha_entrada     
  ,criterio_operacion  AS criterio_operacion
FROM
  tmp_EXT_tbl_crit_prin_ope_sal  
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

# MAGIC %md
# MAGIC 
# MAGIC ### Query Pruebas (borrar)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_crit_prin_ope_sal
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
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where rut_cliente = 7430953
# MAGIC 
# MAGIC --select * from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado  where rut_cliente = 7430953 and fecha_cierre = 20220630
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC select 
# MAGIC   a.rut_cliente 
# MAGIC   ,count(a.operacion) AS cant_det_total
# MAGIC from 
# MAGIC   riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin a
# MAGIC left join
# MAGIC   
# MAGIC where a.rut_cliente = '12439307'
# MAGIC group by a.rut_cliente

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

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where segmento = 'G'
# MAGIC 
# MAGIC select count(1),criterio_entrada from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin group by criterio_entrada
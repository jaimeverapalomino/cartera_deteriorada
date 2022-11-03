# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: Criterio de Entrada Deterioro Segmento Individual
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_02_CarDet_Ind_Crit_Ent_Crit.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
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
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios
# MAGIC ********************************************************************************************************
# MAGIC ##### Pendientes:
# MAGIC * sin pendientes

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
p_fecha_cierre_ant=20220531
p_periodo_cierre_ant=202205     
p_tipo_proceso_ant ='C'

# COMMAND ----------

print(f"p_IndCritDetEnt: {p_IndCritDetEnt}")
print(f"p_fecha_cierre_ant: {p_fecha_cierre_ant}")
print(f"p_periodo_cierre_ant: {p_periodo_cierre_ant}")
print(f"p_tipo_proceso_ant: {p_tipo_proceso_ant}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Evaluacion de Criterios Deterioro Entrada Individuales (tbl_cartdet_crit_ent_ope_campo)
# MAGIC ---
# MAGIC * Talba con criterios deterioro de entrada individual
# MAGIC * Para individual solo hay un criterio de deterioro

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_EXT_tbl_cartdet_crit_ent_ope_campo AS 
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
  ,nombre_campo
  ,valor_campo 
  ,condicion_regla  
  ,valor_regla
  ,flag_resultado_regla
FROM
  {base_silverX}.tbl_cartdet_crit_ent_ope_campo A
WHERE
    fecha_cierre = {FechaX}  
AND periodo_cierre = {PeriodoX}  
/* AND tipo_proceso = '{tipo_procesoX}' */
AND criterio_entrada  IN ({p_IndCritDetEnt})  
""" 

# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones Periodo Anterior (tbl_segmentacion_d00_segmentado)
# MAGIC --------------------------------------
# MAGIC * Extrae stock de operaciones vigentes proceso anterior. 
# MAGIC * Permitira mantener fecha de entrada de deterioro de las operaciones y analizar mantener en deterioro operaciones que no entran en este periodo.

# COMMAND ----------

#Extrae operaciones  periodo anterior
paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado_pant as
SELECT
	 periodo_cierre                                        as periodo_cierre
	,fecha_cierre                                          as fecha_cierre
	,tipo_proceso                                          as tipo_proceso
	,rut_cliente                                           as rut_cliente
	,dv_cliente                                            as dv_cliente
    ,tipo_operacion                                        as tipo_operacion
	,operacion                                             as operacion
	,cod_sistema                                           as sistema
	,cod_segmento                                          as segmento
    ,IFNULL(cast(cod_cartdet_cliente as integer),0)        as origen_deterioro
	,IFNULL(cast(cod_motivo_cartdet as integer),0)         as criterio_entrada
	,IFNULL(cast(fecha_cartdet_ope as integer),0)          as fecha_entrada
    ,ind_cartdet                                           as ind_cartdet
FROM 
	riesgobdu_golden_db.tbl_segmentacion_d00_segmentado
WHERE
     fecha_cierre={p_fecha_cierre_ant}
and  periodo_cierre={p_periodo_cierre_ant}     
/* and  tipo_proceso = '{p_tipo_proceso_ant}' */
"""

# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operaciones deterioradas periodo actual a Nivel Criterio Deterioro (tmp_RES_cartdet_crit_ent_ope_crit)
# MAGIC ---
# MAGIC * genera salida temporal a nivel de criterio de deterioro periodo actual. 
# MAGIC * solo operaciones con condicion de deterioro

# COMMAND ----------


paso_query30 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_ent_ope_crit AS
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
	,criterio_entrada 
	,origen_deterioro 
	,fecha_entrada 
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_ope_campo
WHERE 
   flag_resultado_regla=1 /* solo operaciones con condicion de deterioro*/
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
"""


# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deterioro Actual Operaciones a Nivel de Criterio de Deterioro (tmp_tbl_cartdet_crit_ent_ope_crit_act)
# MAGIC ---
# MAGIC * Mantiene fecha de inicio cartera deteriorada para operaciones. 
# MAGIC * Si la operacion deteriorada en mes actual existe en mes anterior y esta con marca de deterioro, entonces, mantiene la fecha de deterioro mes anterior

# COMMAND ----------

#Para las deteriroradas del mes actual, mantiene fecha de deterioro del mes anterior
paso_query40 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_Ope_Crit_Act AS 
select  
	 a.periodo_cierre                AS periodo_cierre           
	,a.fecha_cierre                  AS fecha_cierre       
	,trim(a.tipo_proceso)            AS tipo_proceso            
	,a.rut_cliente                   AS rut_cliente       
	,trim(a.dv_rut_cliente)          AS dv_rut_cliente
    ,trim(a.tipo_operacion)          AS tipo_operacion
	,trim(a.operacion)               AS operacion         
	,trim(a.sistema)                 AS sistema         
	,trim(a.segmento)                AS segmento        
	,trim(a.criterio_entrada)        AS criterio_entrada                
	,trim(a.origen_deterioro)        AS origen_deterioro                
	,CASE 
        WHEN b.operacion IS NOT NULL and b.ind_cartdet='D' /*existe mes anterior y esta deteriorada*/
        THEN b.fecha_entrada 
        ELSE a.fecha_entrada 
     END                             AS fecha_entrada
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
# MAGIC ### Stock Deterioro Operaciones a Nivel de Criterio de Deterioro (tmp_tbl_cartdet_crit_ent_ope_crit_stk)
# MAGIC ---
# MAGIC * Obtiene todas las operaciones deterioradas del mes anterior, para los clientes individuales.
# MAGIC * Esto permite que en el proceso de evaluacion de salida las pueda evaluar y marcar con el motivo de salida de deterioro
# MAGIC * Las operaciones se registran con el nuevo periodo

# COMMAND ----------

paso_query50 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_Ope_Crit_Stk AS 
select  
	 {PeriodoX}                       AS periodo_cierre   
	,{FechaX}                         AS fecha_cierre
	,'{tipo_procesoX}'                AS tipo_proceso
	,a.rut_cliente                    AS rut_cliente 
	,trim(a.dv_cliente)               AS dv_rut_cliente 
    ,trim(a.tipo_operacion)           AS tipo_operacion
	,trim(a.operacion)                AS operacion
	,trim(a.sistema)                  AS sistema
	,trim(a.segmento)                 AS segmento
	,trim(a.criterio_entrada)         AS criterio_entrada
	,trim(a.origen_deterioro)         AS origen_deterioro
	,a.fecha_entrada                  AS fecha_entrada
FROM 
    tmp_EXT_tbl_segmentacion_d00_segmentado_pant a /*operaciones del mes anterior*/
  left join
    tmp_RES_Ope_Crit_Act b /*operaciones deterioradas del mes actual*/
ON
     a.operacion = b.operacion
 AND a.sistema = b.sistema
WHERE
     a.ind_cartdet='D'          /*deterioradas mes anterior*/
and  a.criterio_entrada= {p_IndCritDetEnt}       /*deterioradas por movito individual*/
and  b.operacion is null        /*que no exista en las operaciones deterioradas del mes actual*/
""" 


# COMMAND ----------

sqlSafe(paso_query50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genera salida con deterioro actual y stock
# MAGIC ---
# MAGIC * tabla temporal que contiene todas los criterios de deterioro que pueda tener una operacion

# COMMAND ----------

paso_query60 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_cartdet_crit_ent_ope_crit_F AS 
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
  tmp_RES_Ope_Crit_Act A
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
  tmp_RES_Ope_Crit_Stk B
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
  A.periodo_cierre,
  A.fecha_cierre,
  A.tipo_proceso,
  A.rut_cliente,
  A.dv_rut_cliente,
  A.tipo_operacion,
  A.operacion,
  A.sistema,
  A.segmento,
  A.criterio_entrada,
  A.origen_deterioro,
  A.fecha_entrada,
  RANK() OVER(PARTITION BY A.operacion, A.sistema, A.criterio_entrada ORDER BY A.origen_deterioro ASC) AS rank_id
FROM   
  tmp_RES_cartdet_crit_ent_ope_crit_F A
"""  

# COMMAND ----------

sqlSafe(paso_query70)

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

paso_query300 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_crit where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} and tipo_proceso = '{tipo_procesoX}' and criterio_entrada  = {p_IndCritDetEnt} """


# COMMAND ----------

sqlSafe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query330 = f"""
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

sqlSafe(paso_query330)

# COMMAND ----------

# MAGIC %sql
# MAGIC --ESTADISTICAS FINALES TABLA SALIDA
# MAGIC select periodo_cierre, fecha_cierre, tipo_proceso, segmento, criterio_entrada, origen_deterioro, count(*) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit group by 1,2,3,4,5,6;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where rut_cliente =7776581 and criterio_entrada=1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")
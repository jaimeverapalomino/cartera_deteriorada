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
# MAGIC * Nombre: BCI_01_CarDet_Ind_Crit_Ent_1.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Clientes con calificación de riesgo considerada en deterioro. Criterio 1.
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
# MAGIC * riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado
# MAGIC * riesgobdu_golden_db.tbl_segmentacion_clientes 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios
# MAGIC ********************************************************************************************************
# MAGIC ##### Pendientes:
# MAGIC * confirmar campo de segmento en tabla tbl_cartdet_clientes.
# MAGIC * llamado a los otros notebook
# MAGIC * descripcion selecion calificaciones tabla de parametros
# MAGIC * Consulta (Jonatan), en el caso de que el cliente sea Ind y tenga operaciones grupales e ind. ¿Que algoritmo se usa para deteriorar?, se debe deteriorar   dependiendo de la segmentacion del cliente o de la segmentacion de la operación.
# MAGIC * Consulta (Jaime), la tabla riesgobdu_golden_db.tbl_segmentacion_d00_segmentado va a tener la segmentacion?.
# MAGIC * Ver como programar la fecha de suspencion devengo.

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
dbutils.widgets.text("FechaW","","01-Fecha: {FechaX}")
dbutils.widgets.text("PeriodoW","","02-Periodo: {PeriodoX}")
dbutils.widgets.text("bd_silverW","","03-Nombre BD Silver: base_silverX}")
dbutils.widgets.text("ruta_silverW","","04-Ruta adss Silver: {ruta_silverX}")
dbutils.widgets.text("path_bdW","","05-Ruta adss de la BD Silver: {bd_ruta_silverX}")
dbutils.widgets.text("tipo_procesoW","","06-Tipo de Proceso (C o PC): {tipo_procesoX}")

FechaX = dbutils.widgets.get("FechaW") 
PeriodoX = dbutils.widgets.get("PeriodoW")
base_silverX = dbutils.widgets.get("bd_silverW")
ruta_silverX = dbutils.widgets.get("ruta_silverW")
bd_ruta_silverX = dbutils.widgets.get("path_bdW")
tipo_procesoX = dbutils.widgets.get("tipo_procesoW")

spark.conf.set("bci.Fecha", FechaX)
spark.conf.set("bci.Periodo", PeriodoX)
spark.conf.set("bci.dbnamesilver", base_silverX)
spark.conf.set("bci.rutasilver", ruta_silverX)
spark.conf.set("bci.bdRutasilver", bd_ruta_silverX)
spark.conf.set("bci.tipo_proceso", tipo_procesoX)

print(f"Fecha de Proceso: {FechaX}")
print(f"Periodo: {PeriodoX}")      
print(f"Nombre BD Silver: {base_silverX}")
print(f"Ruta abfss Silver: {ruta_silverX}")
print(f"Ruta abfss de la BD Silver: {bd_ruta_silverX}")
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

# DBTITLE 1,Valida parámetro "ruta_silverX"
validaParametro(ruta_silverX)

# COMMAND ----------

# DBTITLE 1,Valida parámetro "bd_ruta_silverX"
validaParametro(bd_ruta_silverX)

# COMMAND ----------

# DBTITLE 1,Valida parámetro "tipo_procesoX"
validaParametro(tipo_procesoX)

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
# MAGIC ### Extrae Calificaciones Individuales
# MAGIC ---
# MAGIC * Obtiene las calificaciones que deterioran desde la tabla de parametros

# COMMAND ----------

calificaciones = obtieneParametro('SEGMENTACION', 'CAL_DET')

param_cal = calificaciones.split(',')
param_cal = ','.join([f"'{i}'" for i in param_cal])
param_cal

# COMMAND ----------

print(f"calificaciones: {calificaciones}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones D00 Periodo Actual (tbl_segmentacion_d00_segmentado)
# MAGIC --------------------------------------
# MAGIC - Se extrae operaciones para periodo actual

# COMMAND ----------

#Extrae las operaciones de la tabla riesgobdu_golden_db.tbl_segmentacion_d00_segmentado para el periodo actual
paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado AS
SELECT 
  cast(substr(cast(b.periodo_cierre as string),1,6) as integer)                      AS periodo_cierre,
  b.fecha_cierre                        AS fecha_cierre,
  b.tipo_proceso                        AS tipo_proceso,
  b.cod_segmento                        AS segmento,
  b.operacion                           AS operacion,
  b.tipo_operacion                      AS tipo_operacion,
  b.cod_sistema                         AS sistema,
  b.rut_cliente                         AS rut_cliente,
  b.dv_cliente                          AS dv_rut_cliente
FROM
  riesgobdu_golden_db.tbl_segmentacion_d00_segmentado b
WHERE 
    b.fecha_cierre =  20220630
AND b.periodo_cierre = 202206100
AND b.tipo_proceso ='C'
"""


# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_EXT_tbl_segmentacion_d00_segmentado where rut_cliente=7776581;
# MAGIC --select * from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado where rut_cliente=7776581 order by fecha_cierre desc, periodo_cierre desc;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Clientes Consolidado Periodo Actual (tbl_segmentacion_cliente_consolidado)
# MAGIC --------------------------------------
# MAGIC - Se extrae clientes para el periodo actual con la clasificacion bci desde la tabla tbl_segmentacion_cliente_consolidado 

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_cliente_consolidado as
SELECT 
  periodo_cierre                      AS periodo_cierre,
  fecha_cierre                        AS fecha_cierre,
  tipo_proceso                        AS tipo_proceso,
  rut_cliente                         AS rut_cliente,
  dv_cliente                          AS dv_rut_cliente, 
  calificacion_bci                    AS calificacion_bci,
  calificacion_regulador              AS calificacion_regulador,    
  fecha_calificacion                  as fecha_calificacion,
  cic_cliente                         AS cic_cliente
FROM
  riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado
WHERE 
  fecha_cierre = 20220630 
and periodo_cierre = 202206
and  tipo_proceso ='C' 
"""


# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_EXT_tbl_segmentacion_cliente_consolidado where rut_cliente=7776581
# MAGIC --select * from riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado where rut_cliente=7776581
# MAGIC --DESCRIBE riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Clientes Segmentacion Periodo Actual (tbl_segmentacion_clientes)
# MAGIC --------------------------------------
# MAGIC - Se extrae clientes para el periodo actual con su segmentacion (la segmentacion del cliente y segmentacion de la operacion pueden ser distintas)

# COMMAND ----------

#Extrae stock de operaciones del mes actual
paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_clientes as
SELECT 
    cast(substr(cast(periodo_cierre as string),1,6) as integer)		 as  periodo_cierre,
    fecha_cierre		 as  fecha_cierre,
    tipo_proceso		 as  tipo_proceso,
    rut_cliente			 as  rut_cliente,
    dv_rut_cliente		 as  dv_rut_cliente,
    cic_cliente			 as  cic_cliente,
    segmento_cliente	 as  segmento_cliente,
    nuevo_segmento		 as  nuevo_segmento,
    tipo_cliente		 as  tipo_cliente,
    motivo_segmentacion	 as  motivo_segmentacion
FROM
    riesgobdu_golden_db.tbl_segmentacion_clientes 
WHERE 
    fecha_cierre = 20220630 
AND periodo_cierre =  202206100
AND tipo_proceso ='C' 
"""

# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %sql
# MAGIC --describe riesgobdu_golden_db.tbl_segmentacion_clientes
# MAGIC select * from tmp_EXT_tbl_segmentacion_clientes where rut_cliente = 7776581
# MAGIC --select segmento_cliente, count(*) from tmp_EXT_tbl_segmentacion_clientes group by 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones Periodo Anterior (tbl_segmentacion_d00_segmentado)
# MAGIC --------------------------------------
# MAGIC * Extrae stock de operaciones vigentes proceso anterior. 
# MAGIC * Permitira mantener fecha de entrada de deterioro de las operaciones y analizar mantener en deterioro operaciones que no entran en este periodo.

# COMMAND ----------

#Extrae operaciones  periodo anterior
paso_query35 = """
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
	,fecha_cartdet_ope          as fecha_entrada
    ,ind_cartdet                as ind_cartdet
FROM 
	riesgobdu_golden_db.tbl_segmentacion_d00_segmentado
WHERE
     fecha_cierre=20220531
and  periodo_cierre=202205     
and  tipo_proceso ='C'     
"""

# COMMAND ----------

sqlSafe(paso_query35)

# COMMAND ----------

# MAGIC %sql
# MAGIC  select * from tmp_EXT_tbl_segmentacion_d00_segmentado_pant WHERE rut_cliente=7776581
# MAGIC --select * from tmp_EXT_tbl_segmentacion_d00_segmentado_pant where ind_cartdet='D' and criterio_entrada='01'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Marca de deterioro por cliente
# MAGIC --------------------------------------
# MAGIC - Marca clientes con deterioro y sin deterioro segun su clasificacion

# COMMAND ----------

#Marca clientes con marca de deterioro
paso_query40 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_Clientes_Flag_Det as
SELECT 
    periodo_cierre,
    fecha_cierre,
    tipo_proceso,
    rut_cliente,
    dv_rut_cliente, 
    calificacion_bci,
    calificacion_regulador,    
    fecha_calificacion,
    cic_cliente,
    CASE WHEN trim(calificacion_bci) IN ({param_cal}) THEN 1 ELSE 0 END AS flag_Cli_Det
FROM
  tmp_EXT_tbl_segmentacion_cliente_consolidado
"""


# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_RES_Clientes_Flag_Det where rut_cliente = 7776581
# MAGIC --select * from tmp_RES_Clientes_Flag_Det where flag_Cli_Det=1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtiene Operaciones Clientes Individuales
# MAGIC --------------------------------------
# MAGIC - Obtiene todas las operaciones de los clientes individuales. Pueden tener operaciones individuales y/o grupales

# COMMAND ----------

#Extrae todas las operaciones de los clientes individuales
paso_query50 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_Operaciones_Clientes_Ind AS
SELECT 
  b.periodo_cierre,
  b.fecha_cierre,
  b.tipo_proceso,
  b.segmento,
  b.operacion,
  b.tipo_operacion,
  b.sistema,
  b.rut_cliente,
  b.dv_rut_cliente,
  c.segmento_cliente
FROM
  tmp_EXT_tbl_segmentacion_d00_segmentado b,
  tmp_EXT_tbl_segmentacion_clientes c
WHERE 
  b.rut_cliente = c.rut_cliente
and c.segmento_cliente='I'
"""


# COMMAND ----------

sqlSafe(paso_query50)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_RES_Operaciones_Clientes_Ind where rut_cliente = 7776581 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Marca operaciones con deterioro periodo actual, segun la clasificacion del cliente.
# MAGIC ---
# MAGIC * Para todas las operaciones de los clientes individuales del periodo actual, marca si quedan deterioradas o no 
# MAGIC * Las excepciones de deterioro se ven en el notebook de salida individual

# COMMAND ----------

#tabla con operaciones clasificadas que deterioran y no deterioran del periodo actual
paso_query60 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL as
SELECT
 A.periodo_cierre,
 A.fecha_cierre,
 A.tipo_proceso,
 A.segmento,
 A.operacion,
 A.tipo_operacion,
 A.sistema,
 A.rut_cliente,
 A.dv_rut_cliente ,
'calificacion_bci'                            AS nombre_campo,
 cast(B.calificacion_bci as string)           AS valor_campo,
 " IN "                                       AS condicion_regla,     
 "{param_cal}"                                AS valor_regla,
 CASE WHEN trim (B.calificacion_bci) IN ({param_cal}) THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_RES_Operaciones_Clientes_Ind A
LEFT JOIN
    tmp_RES_Clientes_Flag_Det B 
ON A.rut_cliente = B.rut_cliente
"""


# COMMAND ----------

sqlSafe(paso_query60)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select flag_resultado_regla, count(*) from tmp_RES_D00_OPE_CAMPO_EVAL group by 1
# MAGIC 
# MAGIC select * from tmp_RES_D00_OPE_CAMPO_EVAL where rut_cliente=7776581
# MAGIC 
# MAGIC --select count(*) from tmp_RES_D00_OPE_CAMPO_EVAL
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Campo Evaludado (tmp_tbl_cartdet_crit_ent_ope_campo)
# MAGIC ------------------
# MAGIC * generar salida temporal a nivel de campo evaluado. 
# MAGIC * se registran todas las operaciones evaluadas

# COMMAND ----------


paso_query70 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_ent_ope_campo AS
SELECT
	 IFNULL(A.periodo_cierre,190001)       AS periodo_cierre    
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_cierre 
	,IFNULL(A.tipo_proceso,' ')            AS tipo_proceso 
	,IFNULL(A.rut_cliente,0)               AS rut_cliente 
	,IFNULL(A.dv_rut_cliente,' ')          AS dv_rut_cliente 
    ,IFNULL(A.tipo_operacion,' ')          AS tipo_operacion 
	,IFNULL(A.operacion,' ')               AS operacion 
	,IFNULL(A.sistema,' ')                 AS sistema   
	,IFNULL(A.segmento,' ')                AS segmento 
	,'01'                                  AS criterio_entrada 
	,CASE 
         WHEN IFNULL(A.segmento,' ') = 'G' THEN '5' 
         WHEN IFNULL(A.segmento,' ') = 'I' THEN '0' 
         ELSE '0' 
     END                                   AS origen_deterioro 
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_entrada 
	,IFNULL(A.nombre_campo,' ')            AS nombre_campo
	,IFNULL(A.valor_campo,' ')             AS valor_campo 
	,IFNULL(A.condicion_regla,' ')         AS condicion_regla  
	,IFNULL(A.valor_regla,' ')             AS valor_regla
	,IFNULL(A.flag_resultado_regla,0)      AS flag_resultado_regla
FROM
  tmp_RES_D00_OPE_CAMPO_EVAL A
"""  

# COMMAND ----------

sqlSafe(paso_query70)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select flag_resultado_regla, count(*) from tmp_tbl_cartdet_crit_ent_ope_campo group by 1
# MAGIC 
# MAGIC select  * from tmp_tbl_cartdet_crit_ent_ope_campo where rut_cliente = 7776581
# MAGIC --describe tmp_tbl_cartdet_crit_ent_ope_campo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operaciones deterioradas periodo actual a Nivel Criterio Deterioro (tmp_RES_cartdet_crit_ent_ope_crit)
# MAGIC ---
# MAGIC * genera salida temporal a nivel de criterio de deterioro periodo actual. 
# MAGIC * solo operaciones con condicion de deterioro

# COMMAND ----------


paso_query100 = """
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
    tmp_tbl_cartdet_crit_ent_ope_campo
WHERE 
   flag_resultado_regla=1 /* solo operaciones con condicion de deterioro*/
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
"""


# COMMAND ----------

sqlSafe(paso_query100)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select count(1), periodo_cierre from tmp_RES_cartdet_crit_ent_ope_crit   group by 2;
# MAGIC 
# MAGIC select * from tmp_RES_cartdet_crit_ent_ope_crit where rut_cliente=7776581;
# MAGIC 
# MAGIC --describe tmp_RES_cartdet_crit_ent_ope_crit

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deterioro Actual Operaciones a Nivel de Criterio de Deterioro (tmp_tbl_cartdet_crit_ent_ope_crit_act)
# MAGIC ---
# MAGIC * Mantiene fecha de inicio cartera deteriorada para operaciones. 
# MAGIC * Si la operacion deteriorada en mes actual existe en mes anterior y esta con marca de deterioro, entonces, mantiene la fecha de deterioro mes anterior

# COMMAND ----------

#Para las deteriroradas del mes actual, mantiene fecha de deterioro del mes anterior
paso_query110 = """
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

sqlSafe(paso_query110)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tmp_RES_Ope_Crit_Act where rut_cliente=7776581;
# MAGIC --describe tmp_RES_Ope_Crit_Act;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stock Deterioro Operaciones a Nivel de Criterio de Deterioro (tmp_tbl_cartdet_crit_ent_ope_crit_stk)
# MAGIC ---
# MAGIC * Obtiene todas las operaciones deterioradas del mes anterior, para los clientes individuales.
# MAGIC * Esto permite que en el proceso de evaluacion de salida las pueda evaluar y marcar con el motivo de salida de deterioro
# MAGIC * Las operaciones se registran con el nuevo periodo

# COMMAND ----------

paso_query120 = f"""
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
and  a.criterio_entrada='01'    /*deterioradas por movito individual*/
and  b.operacion is null        /*que no exista en las operaciones deterioradas del mes actual*/
""" 


# COMMAND ----------

sqlSafe(paso_query120)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from tmp_RES_Ope_Crit_Stk /* 134*/
# MAGIC 
# MAGIC select * from tmp_RES_Ope_Crit_Stk where rut_cliente = 7776581

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Criterio Deterioro (tmp_tbl_cartdet_crit_ent_ope_crit)
# MAGIC ---
# MAGIC * tabla temporal que contiene todas los criterios de deterioro que pueda tener una operacion
# MAGIC * para individual solo hay un criterio de deterioro

# COMMAND ----------

paso_query125 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_tbl_cartdet_crit_ent_ope_crit AS 
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

sqlSafe(paso_query125)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select criterio_entrada, origen_deterioro, count(*) from tmp_tbl_cartdet_crit_ent_ope_crit group by 1,2;
# MAGIC select * from tmp_tbl_cartdet_crit_ent_ope_crit where rut_cliente=7776581

# COMMAND ----------

# MAGIC %md
# MAGIC ### Jerarquia criterio deterioro principal (tmp_tbl_cartdet_crit_ent_ope_crit_prin)
# MAGIC ---
# MAGIC * Calcula el criterior principal de deterioro, para dejar uno solo por operacion

# COMMAND ----------

paso_query200 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_cartdet_crit_ent_ope_crit_prin AS 
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
  RANK() OVER (PARTITION BY A.operacion, A.sistema ORDER BY A.criterio_entrada asc, A.origen_deterioro asc) AS jerarquia
FROM 
  tmp_tbl_cartdet_crit_ent_ope_crit A
""" 

# COMMAND ----------

sqlSafe(paso_query200)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_RES_cartdet_crit_ent_ope_crit_prin where rut_cliente=7776581

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Criterio Deterioro Principal (tmp_tbl_cartdet_crit_ent_ope_crit_prin)
# MAGIC ---
# MAGIC * Tabla temporal que contiene el criterio de deterioro principal por operacion
# MAGIC * Una operacion puede estar solo con un criterio de deterioro

# COMMAND ----------

paso_query210 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_tbl_cartdet_crit_ent_ope_crit_prin AS 
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
  tmp_RES_cartdet_crit_ent_ope_crit_prin A
WHERE
    jerarquia=1
""" 

# COMMAND ----------

sqlSafe(paso_query210)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_tbl_cartdet_crit_ent_ope_crit_prin  where rut_cliente=7776581

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Tablas de Salidas
# MAGIC --------------------------------------
# MAGIC * carga resultados a tablas de salidas del notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Tabla Evaluacion por Campo (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos)

# COMMAND ----------

paso_query300 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_campo where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} and tipo_proceso = '{tipo_procesoX}' and criterio_entrada  = '01' """


# COMMAND ----------

sqlSafe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------


paso_query310 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_ent_ope_campo
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
  tmp_tbl_cartdet_crit_ent_ope_campo
"""  


# COMMAND ----------

sqlSafe(paso_query310)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo where rut_cliente =7776581

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Tabla Evaluacion por Criterio (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos)

# COMMAND ----------

paso_query320 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_crit where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} and tipo_proceso = '{tipo_procesoX}' and criterio_entrada  = '01' """


# COMMAND ----------

sqlSafe(paso_query320)

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
"""  

# COMMAND ----------

sqlSafe(paso_query330)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select periodo_cierre, criterio_entrada, origen_deterioro, count(*) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit group by 1,2,3
# MAGIC 
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit where rut_cliente=7776581

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Tabla Evaluacion por Criterio Principal (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos)

# COMMAND ----------

paso_query340 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_prin where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} and tipo_proceso = '{tipo_procesoX}' and criterio_entrada  = '01' """

# COMMAND ----------

sqlSafe(paso_query340)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query350 = f"""
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
  tmp_tbl_cartdet_crit_ent_ope_crit_prin
"""  

# COMMAND ----------

sqlSafe(paso_query350)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin where rut_cliente=7776581
# MAGIC 
# MAGIC --select periodo_cierre, fecha_cierre, tipo_proceso, operacion, sistema, count(*) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin group by 1,2,3,4,5 having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")
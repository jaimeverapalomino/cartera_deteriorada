# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: Evaluacion Criterios de Entrada Deterioro Segmento Individual
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_01_CarDet_Ind_Crit_Ent.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Evaluacion criterios de entrada a deterioro para operaciones individuales
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
# MAGIC ### Extrae Calificaciones Individuales
# MAGIC ---
# MAGIC * Obtiene las calificaciones que deterioran desde la tabla de parametros

# COMMAND ----------

calificaciones = obtieneParametro('SEGMENTACION', 'CAL_DET')

param_cal = calificaciones.split(',')
param_cal = ','.join([f"'{i}'" for i in param_cal])
param_cal

# COMMAND ----------

p_IndCritDetEnt = 1
p_fecha_cierre_ant=20220531
p_periodo_cierre_ant=202205     
p_tipo_proceso_ant ='C'

# COMMAND ----------

print(f"calificaciones: {calificaciones}")
print(f"p_IndCritDetEnt: {p_IndCritDetEnt}")
print(f"p_fecha_cierre_ant: {p_fecha_cierre_ant}")
print(f"p_periodo_cierre_ant: {p_periodo_cierre_ant}")
print(f"p_tipo_proceso_ant: {p_tipo_proceso_ant}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones D00 Periodo Actual (tbl_segmentacion_d00_segmentado)
# MAGIC --------------------------------------
# MAGIC - Se extrae operaciones para periodo actual

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado AS
SELECT 
  --cast(substr(cast(b.periodo_cierre as string),1,6) as integer)                      AS periodo_cierre,
  b.periodo_cierre                      AS periodo_cierre,
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
    b.fecha_cierre =  {FechaX}
AND b.periodo_cierre = {PeriodoX}
AND b.tipo_proceso = '{tipo_procesoX}'  
"""


# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select periodo_cierre, count(*) from tmp_EXT_tbl_segmentacion_d00_segmentado group by 1;
# MAGIC --select * from tmp_EXT_tbl_segmentacion_d00_segmentado limit 10;
# MAGIC --select * from  riesgobdu_golden_db.tbl_segmentacion_d00_segmentado limit 10
# MAGIC 
# MAGIC select periodo_cierre, cod_segmento, cod_calificacion, count(*) from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado where periodo_cierre = 202206 group by 1,2,3 order by 1,2,3,4

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
    fecha_cierre = {FechaX} 
and periodo_cierre = {PeriodoX}
and tipo_proceso = '{tipo_procesoX}' 
"""


# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select periodo_cierre, count(*) from tmp_EXT_tbl_segmentacion_cliente_consolidado group by 1
# MAGIC --select * from tmp_EXT_tbl_segmentacion_cliente_consolidado limit 10
# MAGIC --select * from tmp_EXT_tbl_segmentacion_cliente_consolidado where fecha_calificacion <> '19000101' limit 10
# MAGIC 
# MAGIC --select periodo_cierre, calificacion_regulador, count(*) from tmp_EXT_tbl_segmentacion_cliente_consolidado where fecha_calificacion <> '19000101'group by 1,2
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_d00_segmentado_gnz limit 10
# MAGIC select periodo_cierre, cod_segmento, cod_calificacion, count(*) from riesgobdu_silver_db.tbl_d00_segmentado_gnz group by 1,2,3 order by 1,2,3,4

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
    fecha_cierre = {FechaX} 
AND periodo_cierre =  {PeriodoX} 
AND tipo_proceso = '{tipo_procesoX}'
"""

# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %sql
# MAGIC select periodo_cierre, count(*) from tmp_EXT_tbl_segmentacion_clientes group by 1;
# MAGIC --select * from tmp_EXT_tbl_segmentacion_clientes limit 10;

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
# MAGIC select periodo_cierre, count(*) from tmp_RES_Clientes_Flag_Det group by 1;
# MAGIC select periodo_cierre, flag_Cli_Det, count(*) from tmp_RES_Clientes_Flag_Det group by 1,2;

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
  c.rut_cliente,
  c.dv_rut_cliente,
  c.segmento_cliente
FROM
  tmp_EXT_tbl_segmentacion_d00_segmentado b,
  tmp_EXT_tbl_segmentacion_clientes c
WHERE 
    (substring(trim(b.rut_cliente),2,8)) = c.rut_cliente
and c.segmento_cliente = 'I' 
"""


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_segmentacion_clientes

# COMMAND ----------

sqlSafe(paso_query50)

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
 ' IN '                                       AS condicion_regla,     
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
	,1                                     AS criterio_entrada 
	,CASE 
         WHEN IFNULL(A.segmento,' ') = 'G' THEN  5 
         WHEN IFNULL(A.segmento,' ') = 'I' THEN  0 
         ELSE 0 
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

paso_query300 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_campo where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} and tipo_proceso = '{tipo_procesoX}' and criterio_entrada  = {p_IndCritDetEnt} """


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
# MAGIC --ESTADISTICAS FINALES TABLA SALIDA
# MAGIC select periodo_cierre, fecha_cierre, tipo_proceso, segmento, criterio_entrada, origen_deterioro, count(*) from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo group by 1,2,3,4,5,6;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo  where rut_cliente =7776581 and criterio_entrada=1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")
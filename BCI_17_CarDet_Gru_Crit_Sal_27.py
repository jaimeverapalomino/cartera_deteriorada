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
# MAGIC * Nombre: BCI_17_CarDet_Gru_Crit_Sal_27.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 07/09/2022
# MAGIC * Descripcion: Cliente sin creditos deteriorados.
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
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo
# MAGIC 
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios
# MAGIC * Cambiar de tipo de dato el campo origen deterioro (de int a string)
# MAGIC ********************************************************************************************************
# MAGIC ##### Pendientes:
# MAGIC * Se deja como vacio el campo B.saldo_total_ifrs en caso que no tenga en la tabla d00 consolidado o se deja como cero??

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
# MAGIC ### Extrae Criterios de Deterioro Salida Grupal

# COMMAND ----------

p_GruCritDetEnt = (7,8,9,10,11,12,13)
p_GruCritDetSal = 27
p_SalTotIfrs = 0


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones Deterioradas Grupales (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)
# MAGIC --------------------------------------
# MAGIC - Se extrae Operaciones Deterioradas Grupales periodo actual

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_ope_prin as
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
  ,criterio_operacion AS criterio_operacion
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
# MAGIC ### Operaciones D00 (tbl_segmentacion_d00_segmentado)
# MAGIC --------------------------------------
# MAGIC - Se extrae operaciones para evaluar condiciones de salida

# COMMAND ----------

#Extrae las operaciones de la tabla riesgobdu_golden_db.tbl_segmentacion_d00_segmentado para el periodo actual
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
  b.monto_original                      AS monto_original,
  b.saldo_contable                      AS saldo_contable,
  b.saldo_total_ifrs                    AS saldo_total_ifrs
FROM
  {base_goldX}.tbl_segmentacion_d00_segmentado b
WHERE 
    b.fecha_cierre =  {FechaX}
AND b.periodo_cierre = 202206
AND b.tipo_proceso = '{tipo_procesoX}'  
"""

# COMMAND ----------

sqlSafe(paso_query20)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla con operaciones que cumplen y no cumplen condicion deterioro del periodo actual

# COMMAND ----------

# MAGIC %md
# MAGIC ### (27) Operacion con monto en deuda igual a cero

# COMMAND ----------

#Tabla con operaciones que cumplen y no cumplen condicion de salida deterioro del periodo actual
paso_query30 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL as
SELECT

     A.periodo_cierre,
     A.fecha_cierre,
     A.tipo_proceso,
     A.segmento,
     A.operacion,
     A.sistema,
     A.rut_cliente,
     A.dv_rut_cliente ,
     A.tipo_operacion,
    'saldo_total_ifrs' AS nombre_campo,
     cast(B.saldo_total_ifrs as string) AS valor_campo,
     " = "  AS condicion_regla,     
     "0"  AS valor_regla,           /* Este valor debe ser parametrico */
     CASE WHEN B.saldo_total_ifrs = {p_SalTotIfrs} THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_ope_prin A 
LEFT JOIN 
    tmp_EXT_tbl_segmentacion_d00_segmentado B
ON   
        A.operacion = B.operacion
    AND A.sistema = B.cod_sistema

"""

# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Campo Evaluado (tmp_tbl_cartdet_crit_sal_ope_campo)
# MAGIC ------------------
# MAGIC * generar salida temporal a nivel de campo evaluado. 
# MAGIC * se registran todas las operaciones y condiciones evaluadas

# COMMAND ----------

#Operacion con monto en deuda igual a cero
paso_query40 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_ope_campo AS
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
	,27                                    AS criterio_salida 
	,0                                     AS origen_deterioro 
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_salida 
	,IFNULL(A.nombre_campo,' ')            AS nombre_campo
	,IFNULL(A.valor_campo,' ')             AS valor_campo 
	,IFNULL(A.condicion_regla,' ')         AS condicion_regla  
	,IFNULL(A.valor_regla,' ')             AS valor_regla
	,IFNULL(A.flag_resultado_regla,0)      AS flag_resultado_regla
FROM
  tmp_RES_D00_OPE_CAMPO_EVAL A
"""

# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borrado e insercion en Tabla (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)

# COMMAND ----------

paso_query50 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_sal_ope_campo where fecha_cierre = {FechaX} AND periodo_cierre = {PeriodoX} AND criterio_salida = {p_GruCritDetSal} AND tipo_proceso = '{tipo_procesoX}' """


# COMMAND ----------

sqlSafe(paso_query50)

# COMMAND ----------

paso_query60 = f"""
INSERT INTO {base_silverX}.tbl_cartdet_crit_sal_ope_campo
SELECT 
  periodo_cierre           AS periodo_cierre        
  ,fecha_cierre            AS fecha_cierre         
  ,tipo_proceso            AS tipo_proceso          
  ,rut_cliente             AS rut_cliente          
  ,dv_rut_cliente          AS dv_rut_cliente       
  ,tipo_operacion          AS tipo_operacion       
  ,operacion               AS operacion            
  ,sistema                 AS sistema              
  ,segmento                AS segmento             
  ,criterio_salida         AS criterio_salida      
  ,origen_deterioro        AS origen_deterioro     
  ,fecha_salida            AS fecha_salida         
  ,nombre_campo            AS nombre_campo         
  ,valor_campo             AS valor_campo          
  ,condicion_regla         AS condicion_regla       
  ,valor_regla             AS valor_regla          
  ,flag_resultado_regla    AS flag_resultado_regla 
FROM
  tmp_tbl_cartdet_crit_sal_ope_campo 

"""  

# COMMAND ----------

sqlSafe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query borrar

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from tmp_EXT_tbl_cartdet_crit_ent_ope_prin where trim(operacion) = '003080209028'
# MAGIC --select * from tmp_EXT_tbl_segmentacion_d00_segmentado --where trim(operacion) = '003080209028'
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo
# MAGIC 
# MAGIC --select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo
# MAGIC --## 209070
# MAGIC --select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo where flag_resultado_regla = 0 and criterio_salida = 27
# MAGIC --## 209049
# MAGIC --select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo where flag_resultado_regla = 1 and criterio_salida = 27
# MAGIC --## 21
# MAGIC 
# MAGIC --select fecha_cierre from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado   group by fecha_cierre   --WHERE b.fecha_cierre = 20220630
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin where rut_cliente = 15050792
# MAGIC 
# MAGIC --select count(1) from tmp_RES_D00_OPE_CAMPO_EVAL
# MAGIC --209070
# MAGIC --select count(1) from tmp_EXT_tbl_segmentacion_d00_segmentado
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo -- where flag_resultado_regla = 1 and criterio_salida = 27
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_prin  
# MAGIC 
# MAGIC select count(1) from tmp_tbl_cartdet_crit_sal_ope_campo where flag_resultado_regla = 1
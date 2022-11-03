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
# MAGIC * Nombre: BCI_19_CarDet_Gru_Crit_Sal_34.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 05/10/2022
# MAGIC * Descripcion: Criterio de Salida 34 "Cliente sin creditos deteriorados".
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
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit
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
# MAGIC ### Parametros Criterios de Salida

# COMMAND ----------

p_Familia = 'contingente'
p_MoraCli = 'mora_cli'
p_MoraSbif = 'mora_sbif'
p_SinRef = 'sin_ref'
p_SinLir = 'sin_lir'
p_ResMcli = 1
p_ResMsbif = 1
p_ResSref = 1
p_ResSlir = 1


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
  ,a.operacion               AS operacion
  ,a.sistema                 AS sistema
  ,a.segmento                AS segmento
  ,a.tipo_credito            AS tipo_credito
  ,a.familia_ope             AS familia_ope
  ,a.evaluacion              AS evaluacion
  ,a.resultado               AS resultado
FROM
  {base_silverX}.tbl_cartdet_crit_sal_familia a
WHERE 
    a.fecha_cierre = {FechaX}  
AND a.periodo_cierre = {PeriodoX}  
AND a.tipo_proceso = '{tipo_procesoX}'
--AND a.resultado = 1
"""

# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------


paso_query20 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_condicion_salida AS
SELECT 
    a.familia_ope
   ,a.evaluacion 
   ,a.resultado
   ,a.criterio_salida
   ,sum(a.resultado) over(partition by a.familia_ope )  as cant_res
FROM
  tbl_condicion_salida a

"""   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_EXT_tbl_condicion_salida where resultado = 0

# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Criterios de Salida por Familia

# COMMAND ----------

/* no va*/ 

paso_query30 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_PRUEBA_1 AS
SELECT 
   a.operacion
   ,a.sistema
   ,a.familia_ope
   ,count(a.resultado)  as cant_resul
   ,b.cant_res
   ,b.criterio_salida
FROM
  tmp_EXT_tbl_cartdet_crit_gru_sal_fam a
LEFT JOIN
  tmp_EXT_tbl_condicion_salida b
ON
  trim(a.familia_ope) = trim(b.familia_ope)
 AND trim(a.evaluacion) = trim(b.evaluacion)
 AND trim(a.resultado) = trim(b.resultado)
GROUP BY a.operacion, a.sistema, a.familia_ope,b.cant_res,b.criterio_salida
HAVING count(a.resultado) = b.cant_res 
"""

# COMMAND ----------

paso_queryTTT = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_crit_gru_sal_familia AS
SELECT 
   a.periodo_cierre
   ,a.fecha_cierre
   ,a.tipo_proceso
   ,a.rut_cliente
   ,a.operacion
   ,a.sistema
   ,a.segmento
   ,a.tipo_credito
   ,a.familia_ope
   ,a.evaluacion
   ,a.resultado
   ,b.familia_ope AS par_familia_ope
   ,b.evaluacion AS par_evaluacion
   ,b.resultado AS par_resultado
   ,b.criterio_salida AS par_criterio_salida
   ,case when a.resultado = b.resultado then 1 else 0 end as ind_cumple_eval
   ,SUM (case when a.resultado = b.resultado then 1 else 0 end) over (partition by a.operacion,a.sistema) AS cant_cumple_sal  
   ,SUM (b.resultado) over (partition by a.operacion,a.sistema) AS cant_espe_cumple_sal
FROM
  tmp_EXT_tbl_cartdet_crit_gru_sal_fam a
FULL JOIN
  tmp_EXT_tbl_condicion_salida b
ON
  trim(a.familia_ope) = trim(b.familia_ope)
 AND trim(a.evaluacion) = trim(b.evaluacion)
WHERE
    b.resultado = 1
"""

# COMMAND ----------

sqlSafe(paso_queryTTT)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_sal_familia a where a.operacion = 'D03410312118'
# MAGIC   --and a.sistema = '01'
# MAGIC   
# MAGIC   select * from tmp_tbl_crit_gru_sal_familia where operacion = 'E16993414787' and sistema = '10'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   a.periodo_cierre
# MAGIC   ,a.fecha_cierre
# MAGIC   ,a.tipo_proceso
# MAGIC   ,a.rut_cliente
# MAGIC   ,a.operacion
# MAGIC   ,a.sistema
# MAGIC   ,a.segmento
# MAGIC   ,a.tipo_credito
# MAGIC   ,a.par_criterio_salida
# MAGIC    ,max(a.familia_ope) as nombre_campo
# MAGIC    ,cast(max(a.cant_cumple_sal) as string) as valor_campo 
# MAGIC    ,' = ' as condicion_regla  
# MAGIC    ,cast(max(a.cant_espe_cumple_sal) as string) as valor_regla
# MAGIC    ,max(case when a.cant_cumple_sal = a.cant_espe_cumple_sal then 1 else 0 END) AS flag_resultado_regla
# MAGIC FROM
# MAGIC   tmp_tbl_crit_gru_sal_familia a
# MAGIC  where 
# MAGIC    a.operacion = 'D03410312118'
# MAGIC GROUP BY   a.periodo_cierre
# MAGIC   ,a.fecha_cierre
# MAGIC   ,a.tipo_proceso
# MAGIC   ,a.rut_cliente
# MAGIC   ,a.operacion
# MAGIC   ,a.sistema
# MAGIC   ,a.segmento
# MAGIC   ,a.tipo_credito
# MAGIC   ,a.par_criterio_salida

# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_cartdet_crit_gru_sal_fam where operacion = 'E16993414787' and sistema = '10'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### QUERY (borrar)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select count(1),familia_ope,criterio_salida from tmp_tbl_PRUEBA_1  group by familia_ope,criterio_salida
# MAGIC 
# MAGIC --select * from tmp_EXT_tbl_condicion_salida where familia_ope = 'Contingente'
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_cartdet_crit_gru_sal_fam where operacion = 'D24007201044' and sistema '20'
# MAGIC 
# MAGIC --select * from tmp_tbl_PRUEBA_1 where criterio_salida = 34
# MAGIC 
# MAGIC --D19401128015  --39

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Campo Evaluado (tmp_tbl_cartdet_crit_sal_ope_campo)
# MAGIC ------------------
# MAGIC * generar salida temporal a nivel de campo evaluado. 
# MAGIC * se registran todas las operaciones y condiciones evaluadas

# COMMAND ----------

#Operacion con monto en deuda igual a cero
paso_query60 = """
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
	,63                                    AS criterio_salida 
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

sqlSafe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borrado e insercion en Tabla (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_crit)

# COMMAND ----------

paso_query70 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_sal_ope_campo where fecha_cierre = {FechaX} AND criterio_salida = {p_GruCritDetSal} """


# COMMAND ----------

sqlSafe(paso_query70)

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
  ,origen_deterioro 
  ,fecha_salida
  ,nombre_campo
  ,valor_campo 
  ,condicion_regla  
  ,valor_regla
  ,flag_resultado_regla
FROM
  tmp_tbl_cartdet_crit_sal_ope_campo 

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

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC select 
# MAGIC   *
# MAGIC FROM
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_d00_segmentado b
# MAGIC WHERE 
# MAGIC     b.fecha_cierre = 20220630 
# MAGIC   and b.rut_cliente = 17974334

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1) from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_campo where criterio_salida = 63
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
# MAGIC * Nombre: BCI_07_CarDet_Gru_Crit_Ent_7.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 23/08/2022
# MAGIC * Descripcion: Cliente con operación en cartera vencida. Criterio 7.
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
# MAGIC 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo
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
# MAGIC ### Extrae Criterios de Deterioro Entrada Grupal

# COMMAND ----------

p_GruCritDetEnt = 7
p_Ccontable = "14%"
p_Saldocv = 0
p_Diasmora = 90
p_fecCv = 19000101

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Operaciones D00 (tbl_segmentacion_d00_segmentado)
# MAGIC --------------------------------------
# MAGIC - Se extrae operaciones para periodo actual

# COMMAND ----------

#Extrae las operaciones de la tabla riesgobdu_golden_db.tbl_segmentacion_d00_segmentado para el periodo actual
paso_query10 = f"""
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
  '19000101'                            AS fec_cart_ven
FROM
  {base_goldX}.tbl_segmentacion_d00_segmentado b
WHERE 
  b.fecha_cierre = {FechaX}
  AND b.periodo_cierre = {PeriodoX}
  AND b.tipo_proceso = '{tipo_procesoX}'
"""


# COMMAND ----------

sqlSafe(paso_query10)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC select * from tmp_EXT_tbl_segmentacion_d00_segmentado 

# COMMAND ----------

#Extrae las operaciones Gruaples 

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_GRU_ACT AS
SELECT 
  cast(substr(cast(b.periodo_cierre as string),1,6) as integer)          AS periodo_cierre,  /*toda esta transformacion es temporal*/
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
  b.fec_cart_ven                        AS fec_cart_ven
FROM
  tmp_EXT_tbl_segmentacion_d00_segmentado b
WHERE 
  cod_segmento = 'G'
"""


# COMMAND ----------

sqlSafe(paso_query20)

# COMMAND ----------

#Extrae las operaciones de la tabla riesgobdu_golden_db.tbl_segmentacion_d00_segmentado para el periodo anterior
paso_query100 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado_ant AS
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
  '19000101'                            AS fec_cart_ven 
FROM
  {base_goldX}.tbl_segmentacion_d00_segmentado b
WHERE 
  b.fecha_cierre = '20220531'
"""



# COMMAND ----------

sqlSafe(paso_query100)

# COMMAND ----------

#Extrae las operaciones Gruaples 

paso_query200 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_GRU_ANT AS
SELECT 
  cast(substr(cast(b.periodo_cierre as string),1,6) as integer)          AS periodo_cierre,  /*toda esta transformacion es temporal*/
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
  b.fec_cart_ven                        AS fec_cart_ven
FROM
  tmp_EXT_tbl_segmentacion_d00_segmentado_ant b
WHERE 
  cod_segmento = 'G'
"""

# COMMAND ----------

sqlSafe(paso_query200)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla con operaciones que cumplen y no cumplen condicion deterioro del periodo actual

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cuenta Contable

# COMMAND ----------

#Tabla con operaciones que cumplen y no cumplen condicion deterioro del periodo actual
paso_query30 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_1 as
SELECT
 A.periodo_cierre,
 A.fecha_cierre,
 A.tipo_proceso,
 A.cod_segmento,
 A.operacion,
 A.cod_sistema,
 A.rut_cliente,
 A.dv_cliente ,
 A.tipo_operacion,
 A.cuenta_contable,
'cuenta_contable' AS nombre_campo,
 cast(A.cuenta_contable as string) AS valor_campo,
 " like "  AS condicion_regla,     
 "14%"  AS valor_regla,           /* Este valor debe ser parametrico */
 CASE WHEN trim (A.cuenta_contable) like '{p_Ccontable}' THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_RES_D00_OPE_GRU_ACT A
"""


# COMMAND ----------

sqlSafe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Saldo en Cartera Vencida

# COMMAND ----------

#Tabla con operaciones que cumplen y no cumplen condicion deterioro del periodo actual
paso_query40 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_2 as
SELECT
 A.periodo_cierre,
 A.fecha_cierre,
 A.tipo_proceso,
 A.cod_segmento,
 A.operacion,
 A.cod_sistema,
 A.rut_cliente,
 A.dv_cliente ,
 A.tipo_operacion,
 A.saldo_cartera_venc,
'saldo_cartera_venc' AS nombre_campo,
 cast(A.saldo_cartera_venc as string) AS valor_campo,
 " > "  AS condicion_regla,     
 " 0 "  AS valor_regla,           /* Este valor debe ser parametrico */
 CASE WHEN trim (A.saldo_cartera_venc) > {p_Saldocv} THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_RES_D00_OPE_GRU_ACT A
"""


# COMMAND ----------

sqlSafe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Días de Mora

# COMMAND ----------

#Tabla con operaciones que cumplen y no cumplen condicion deterioro del periodo actual
paso_query50 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_3 as
SELECT
 A.periodo_cierre,
 A.fecha_cierre,
 A.tipo_proceso,
 A.cod_segmento,
 A.operacion,
 A.cod_sistema,
 A.rut_cliente,
 A.dv_cliente ,
 A.tipo_operacion,
 A.dias_mora,
'dias_mora' AS nombre_campo,
 cast(A.dias_mora as string) AS valor_campo,
 " >= "  AS condicion_regla,     
 " 90 "  AS valor_regla,           /* Este valor debe ser parametrico */
 CASE WHEN trim (A.dias_mora) >= {p_Diasmora} THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_RES_D00_OPE_GRU_ACT A
"""

# COMMAND ----------

sqlSafe(paso_query50)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fecha en Cartera Vencida

# COMMAND ----------


#Tabla con operaciones que cumplen y no cumplen condicion deterioro del periodo actual
paso_query60 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_4 as
SELECT
 A.periodo_cierre,
 A.fecha_cierre,
 A.tipo_proceso,
 A.cod_segmento,
 A.operacion,
 A.cod_sistema,
 A.rut_cliente,
 A.dv_cliente ,
 A.tipo_operacion,
 A.fec_cart_ven,
'fec_cart_ven' AS nombre_campo,
 cast(A.fec_cart_ven as string) AS valor_campo,
 " <> "  AS condicion_regla,     
 cast(B.fec_cart_ven as string)  AS valor_regla,           /* Este valor debe ser parametrico */
 CASE WHEN trim (A.fec_cart_ven) <> '{p_fecCv}' AND trim (A.fec_cart_ven) <> trim (B.fec_cart_ven)  THEN 1 ELSE 0 END AS flag_resultado_regla
FROM 
    tmp_RES_D00_OPE_GRU_ACT A
JOIN
    tmp_RES_D00_OPE_GRU_ANT B
ON
      A.operacion = B.operacion
  AND A.cod_sistema = B.cod_sistema 
"""


# COMMAND ----------

sqlSafe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga de la union de evaluaciones (tmp_RES_D00_OPE_CAMPO_EVAL)

# COMMAND ----------

    paso_query70 = f"""
  CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL as
  select 
    * 
  from 
    tmp_RES_D00_OPE_CAMPO_EVAL_1
  union 
  select 
    *
  from
    tmp_RES_D00_OPE_CAMPO_EVAL_2
   union 
  select 
    *
  from
    tmp_RES_D00_OPE_CAMPO_EVAL_3   
    union 
  select 
    *
  from
    tmp_RES_D00_OPE_CAMPO_EVAL_4
  """

# COMMAND ----------

sqlSafe(paso_query70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal (tmp_tbl_cartdet_crit_ent_ope_campo)

# COMMAND ----------

#Crea tabla temporal final con la evaluacion por campo de cada criterio de evaluacion
paso_query80 = """
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_ent_ope_campo AS
SELECT
	 IFNULL(A.periodo_cierre,190001)       AS periodo_cierre    
	,IFNULL(A.fecha_cierre,19000101)       AS fecha_cierre 
	,IFNULL(A.tipo_proceso,' ')            AS tipo_proceso 
	,IFNULL(A.rut_cliente,0)               AS rut_cliente 
	,IFNULL(A.dv_cliente,' ')              AS dv_rut_cliente 
	,IFNULL(A.tipo_operacion,' ')          AS tipo_operacion
	,IFNULL(A.operacion,' ')               AS operacion 
	,IFNULL(A.cod_sistema,' ')             AS sistema   
	,IFNULL(A.cod_segmento,' ')            AS segmento 
	,'7'                                   AS criterio_entrada 
	,'1'                                   AS origen_deterioro 
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

sqlSafe(paso_query80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borrado e insercion en Tabla (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo)

# COMMAND ----------

paso_query160 = f"""DELETE FROM {base_silverX}.tbl_cartdet_crit_ent_ope_campo where fecha_cierre = {FechaX} AND criterio_entrada = {p_GruCritDetEnt} """

# COMMAND ----------

sqlSafe(paso_query160)

# COMMAND ----------


paso_query170 = f"""
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

sqlSafe(paso_query170)

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
# MAGIC --select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo where rut_cliente = '12254846' and operacion = 'D36700088161'
# MAGIC 
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_sal_ope_crit where rut_cliente = '12254846' and operacion = 'D36700088161'
# MAGIC 
# MAGIC 
# MAGIC --select * from riesgobdu_golden_db.tbl_segmentacion_d00_segmentado where operacion like '%6081122103%' and fecha_cierre = '20220630'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --c4_33_venta_bajo_mora_tmp.txt   -- Individuales
# MAGIC --select rut_cli,num_interno_ident,dias_mora from tbl_c4_curses where nro_NN = 1 order by 2 desc
# MAGIC 
# MAGIC --c4_venta_bajo_excp.txt
# MAGIC --select * from tbl_c4_curses_02
# MAGIC /*
# MAGIC select 
# MAGIC   a.rut, 
# MAGIC   a.operacion_nova,
# MAGIC   b.operacion_bci 
# MAGIC from 
# MAGIC   tbl_homologa_nova a,
# MAGIC   (select * from tbl_homologa_nova  ) b
# MAGIC where 
# MAGIC   a.operacion_nova = b.operacion_nova
# MAGIC   and a.operacion_nova <> b.operacion_bci
# MAGIC */  
# MAGIC --select * from tbl_c4_ope_vta_con_crs_baj_mor
# MAGIC --select * from tbl_homologa_nova where rut = 7830311
# MAGIC 
# MAGIC --187--
# MAGIC --select count(1) from tbl_c4_curses where nro_NN = 1
# MAGIC --33684--
# MAGIC --select count(1) from tbl_homologa_nova
# MAGIC 
# MAGIC 
# MAGIC /*   c4_33_venta_bajo_mora_tmp_crz
# MAGIC select 
# MAGIC   a.rut_cli,
# MAGIC   b.operacion_bci,
# MAGIC   a.num_interno_ident 
# MAGIC from 
# MAGIC   tbl_c4_curses a,
# MAGIC   tbl_homologa_nova b
# MAGIC where 
# MAGIC   trim(a.num_interno_ident) = trim(b.operacion_nova)
# MAGIC   and a.nro_NN = 1
# MAGIC */
# MAGIC 
# MAGIC /*  c4_33_venta_bajo_mora_tmp_nocrz
# MAGIC select 
# MAGIC   a.rut_cli,
# MAGIC   a.num_interno_ident,
# MAGIC   a.dias_mora
# MAGIC from 
# MAGIC   tbl_c4_curses a
# MAGIC left join
# MAGIC   tbl_homologa_nova b
# MAGIC on 
# MAGIC   trim(a.num_interno_ident) = trim(b.operacion_nova)
# MAGIC where 
# MAGIC   b.operacion_nova is null
# MAGIC   and a.nro_NN = 1
# MAGIC */
# MAGIC 
# MAGIC -- union de las dos anteriores c4_33_venta_bajo_mora
# MAGIC 
# MAGIC /* c4_33_${arch_res_cardet}G.txt
# MAGIC select
# MAGIC   a.cod_sistema,
# MAGIC   a.operacion,
# MAGIC   a.rut_cliente,
# MAGIC   a.tipo_operacion,
# MAGIC   a.tipo_credito 
# MAGIC from 
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_d00_segmentado a
# MAGIC where 
# MAGIC   a.cod_segmento = 'G'
# MAGIC 
# MAGIC select 
# MAGIC   b.rut_cli,
# MAGIC   b.num_interno_ident,
# MAGIC   b.dias_mora
# MAGIC from 
# MAGIC   tbl_c4_curses b 
# MAGIC 
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC /*
# MAGIC select
# MAGIC   b.rut_cli,
# MAGIC   b.num_interno_ident,
# MAGIC   a.cod_sistema,
# MAGIC   b.dias_mora
# MAGIC from 
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_d00_segmentado a
# MAGIC join
# MAGIC   tbl_c4_curses b
# MAGIC on
# MAGIC   a.operacion = b.num_interno_ident
# MAGIC where 
# MAGIC   a.cod_segmento = 'G'
# MAGIC */
# MAGIC 
# MAGIC /*
# MAGIC --c4_33_venta_no_crz_sistema--
# MAGIC select
# MAGIC   b.rut_cli,
# MAGIC   b.num_interno_ident,
# MAGIC   b.dias_mora
# MAGIC from 
# MAGIC   tbl_c4_curses b
# MAGIC left join
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_d00_segmentado a
# MAGIC on
# MAGIC   a.operacion = b.num_interno_ident
# MAGIC where 
# MAGIC  a.operacion is null
# MAGIC */
# MAGIC 
# MAGIC /*
# MAGIC --c4_33_ope_fmt_leasing
# MAGIC select
# MAGIC   b.rut_cli,
# MAGIC   trim (b.num_interno_ident) as num_interno_ident  ,
# MAGIC   b.sistema,
# MAGIC   b.dias_mora,
# MAGIC   b.num_interno_ident
# MAGIC from 
# MAGIC   tbl_c4_curses b
# MAGIC left join
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_d00_segmentado a
# MAGIC on
# MAGIC   a.operacion = b.num_interno_ident
# MAGIC where
# MAGIC  b.rut_cli = 17798586
# MAGIC  and  a.operacion is null  
# MAGIC */
# MAGIC 
# MAGIC /*
# MAGIC --c4_33_res_cart_det_leasing
# MAGIC select
# MAGIC   trim(a.operacion),
# MAGIC   a.cod_sistema,
# MAGIC   a.operacion
# MAGIC from 
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_d00_segmentado a
# MAGIC where 
# MAGIC   a.cod_segmento = 'G'
# MAGIC  and a.cod_sistema = 21
# MAGIC  */
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC /* salidas ??? */ 
# MAGIC  --c4_33_ope_fmt_leasing
# MAGIC select
# MAGIC   b.rut_cli,
# MAGIC   trim (b.num_interno_ident) as num_interno_ident  ,
# MAGIC   b.sistema as sistema_venta,
# MAGIC   '11' as num_interno_ident_mor,
# MAGIC   '11' as sistema_mor,
# MAGIC   b.dias_mora
# MAGIC from 
# MAGIC   tbl_c4_curses b
# MAGIC left join
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_d00_segmentado a
# MAGIC on
# MAGIC   a.operacion = b.num_interno_ident
# MAGIC where
# MAGIC   a.operacion is null  
# MAGIC order by b.num_interno_ident desc
# MAGIC 
# MAGIC 
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tbl_c4_ope_vta_con_crs_baj_mor order by num_interno_ident desc

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW  tbl_c4_ope_vta_con_crs_baj_mor_NEW AS
# MAGIC select
# MAGIC   b.rut_cli,
# MAGIC   trim (b.num_interno_ident) as num_interno_ident  ,
# MAGIC   b.sistema as sistema_venta,
# MAGIC   '11' as num_interno_ident_mor,
# MAGIC   '11' as sistema_mor,
# MAGIC   b.dias_mora
# MAGIC from 
# MAGIC   tbl_c4_curses b
# MAGIC left join
# MAGIC   riesgobdu_golden_db.tbl_segmentacion_d00_segmentado a
# MAGIC on
# MAGIC   a.operacion = b.num_interno_ident
# MAGIC where
# MAGIC   a.operacion is null  
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   a.rut_cli
# MAGIC from
# MAGIC   tbl_c4_ope_vta_con_crs_baj_mor a
# MAGIC join
# MAGIC   tbl_c4_ope_vta_con_crs_baj_mor_NEW b
# MAGIC on
# MAGIC   trim(a.rut_cli) = trim(b.rut_cli)
# MAGIC   and trim(a.num_interno_ident) = trim(b.num_interno_ident)
# MAGIC where 
# MAGIC   trim(a.sistema_venta) = trim(b.sistema_venta)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_campo where fecha_cierre = 20220630
# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook carga insumos temporales desde ABFSS
# MAGIC #### Este notebook no se debe productivizar dado que las ingestas de insumos deben ser implementadas en paralelo
# MAGIC ***************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar ruta Insumos

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unidades montadas

# COMMAND ----------

# MAGIC %md
# MAGIC display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Listado Archivos

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils.fs.ls("/mnt/ingestion/dev/riesgobdu_fs/segm_cloud")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importar librerias

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, round, concat, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga temporal de insumos a Dataframe y vistas temporales

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de path

# COMMAND ----------

path = "dbfs:/mnt/ingestion/dev/riesgobdu_fs/segm_cloud/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIR

# COMMAND ----------

archivo = "FN062022.MOT"
path_temporal = str(path)+"/"+str(archivo)

lirDF = spark.read.csv(path_temporal, header=False)

lirDF = lirDF.withColumn("rut", lirDF._c0.substr(1,8))
lirDF = lirDF.withColumn("dv", lirDF._c0.substr(9,1))
lirDF = lirDF.withColumn("dat_cli", lirDF._c0.substr(10,5))

lirDF = lirDF.drop(lirDF._c0)

lirDF.createOrReplaceTempView("tbl_LIR")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Homologa NOVA

# COMMAND ----------

archivo = "OPERA_NOVA_BCI.DAT"
path_temporal = str(path)+"/"+str(archivo)

schema = StructType() \
    .add("rut",IntegerType(),True) \
    .add("dv",StringType(),True) \
    .add("operacion_nova",StringType(),True) \
    .add("operacion_bci",StringType(),True) 

homnovaDF = spark.read.csv(path_temporal, header=False, sep=';', schema=schema) 

homnovaDF.createOrReplaceTempView("tbl_homologa_nova")


# COMMAND ----------

# MAGIC %md
# MAGIC ### c4_hist_cart_det.txt

# COMMAND ----------

schema_cartdet = StructType() \
    .add("fec_proc",StringType(),True) \
    .add("sistema",StringType(),True) \
    .add("num_interno_ident",StringType(),True) \
    .add("ind_tipo_seg",StringType(),True) \
    .add("rut_cli",IntegerType(),True) \
    .add("dv_rut_cli",StringType(),True) \
    .add("tipo_operacion",StringType(),True) \
    .add("oficina",StringType(),True) \
    .add("fecha_otorgam",StringType(),True) \
    .add("moneda",StringType(),True) \
    .add("monto_original",DecimalType(18,0),True) \
    .add("saldo_contable",DecimalType(18,0),True) \
    .add("saldo_moroso1",DecimalType(18,0),True) \
    .add("saldo_moroso2",DecimalType(18,0),True) \
    .add("saldo_en_cart_venc",DecimalType(18,0),True)\
    .add("saldo_reprogramado",DecimalType(18,0),True) \
    .add("nro_ctas_amort_pend",IntegerType(),True) \
    .add("int_por_cobrar",DecimalType(18,0),True) \
    .add("reaj_deven",DecimalType(18,0),True) \
    .add("tipo_credito",StringType(),True) \
    .add("cuenta_contable",StringType(),True) \
    .add("fecha_de_cierre",StringType(),True) \
    .add("amd_fimo",StringType(),True) \
    .add("cod_oren",StringType(),True) \
    .add("fec_cven",StringType(),True) \
    .add("val_skif",DecimalType(18,0),True) \
    .add("val_inif",DecimalType(18,0),True) \
    .add("val_reif",DecimalType(18,0),True) \
    .add("cont_pag_cons",IntegerType(),True) \
    .add("ind_cartera",StringType(),True) \
    .add("ind_vig",StringType(),True)

histCartDetDF =  spark.read.csv("dbfs:/mnt/ingestion/dev/riesgobdu_fs/segm_cloud/Historia/c4_hist_cart_det.txt", header=False, sep="|", schema=schema_cartdet)

histCartDetDF.createOrReplaceTempView("tbl_c4_hist_cart_det")

# COMMAND ----------

# MAGIC %md
# MAGIC ### c4_hist_dat_cli.txt

# COMMAND ----------


schema_datcli = StructType() \
.add("fec_proc",StringType(),True) \
.add("rut_cli",IntegerType(),True) \
.add("dv_rut_cli",StringType(),True) \
.add("cic_cli",StringType(),True) \
.add("d00_sdo_contable",DecimalType(18,0),True) \
.add("d00_sdo_moroso1",DecimalType(18,0),True) \
.add("d00_sdo_moroso2",DecimalType(18,0),True) \
.add("d00_sdo_en_cart_venc",DecimalType(18,0),True) \
.add("d00_sdo_castigo",DecimalType(18,0),True) \
.add("d00_otr_saldos",DecimalType(18,0),True) \
.add("dsf_val_dvgn",DecimalType(18,0),True) \
.add("dsf_val_dvcn",DecimalType(18,0),True) \
.add("dsf_val_dvcx",DecimalType(18,0),True) \
.add("dsf_val_dvgx",DecimalType(18,0),True) \
.add("dsf_val_cdir",DecimalType(18,0),True) \
.add("dsf_val_dmor",DecimalType(18,0),True) \
.add("dsf_fec_proc",DecimalType(18,0),True) \
.add("cal_bci_cli",StringType(),True) \
.add("cal_sbi_cli",StringType(),True) \
.add("fec_cal_cli",StringType(),True) \
.add("fec_susp_dev",StringType(),True) \
.add("cal_bci_cli_pcie",StringType(),True) \
.add("fec_cal_cli_pcie",StringType(),True)

histDatCliDF =  spark.read.csv("dbfs:/mnt/ingestion/dev/riesgobdu_fs/segm_cloud/Historia/c4_hist_dat_cli.txt", header=False, sep="|", schema=schema_datcli)

histDatCliDF.createOrReplaceTempView("tbl_c4_hist_dat_cli")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### c4_hist_ope_crit.txt

# COMMAND ----------

schema_opecrit = StructType() \
    .add("nro_crit",IntegerType(),True) \
    .add("fec_proc",StringType(),True) \
    .add("sistema",StringType(),True) \
    .add("num_interno_ident",StringType(),True) \
    .add("ind_tipo_seg",StringType(),True) \
    .add("fec_ini_cd",StringType(),True) \
    .add("ori_det",IntegerType(),True) \
    .add("rut_cli",IntegerType(),True) \
    .add("fec_susp_dev",StringType(),True) 
	
histOpeCritDF =  spark.read.csv("dbfs:/mnt/ingestion/dev/riesgobdu_fs/segm_cloud/Historia/c4_hist_ope_crit.txt", header=False, sep="|", schema=schema_opecrit)	

histOpeCritDF.createOrReplaceTempView("tbl_c4_hist_ope_crit")

auxOpeDF = spark.read.csv("dbfs:/mnt/ingestion/dev/riesgobdu_fs/segm_cloud/Historia/c4_hist_ope_crit.txt", header=False, sep="|")


# COMMAND ----------

dbutils.fs.ls("/mnt/ingestion/dev/riesgobdu_fs/segm_cloud/202206")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### c4_Curses.txt

# COMMAND ----------

schema_c4_Curses = StructType() \
    .add("rut_cli",IntegerType(),True) \
    .add("dv",StringType(),True) \
    .add("num_interno_ident",StringType(),True) \
    .add("fecha_NN",StringType(),True) \
    .add("dias_mora",IntegerType(),True) \
    .add("sistema",IntegerType(),True) \
    .add("ind_tipo_seg",StringType(),True) \
    .add("tipo_credito",IntegerType(),True) \
    .add("tipo_operacion",StringType(),True) \
    .add("nro_NN",IntegerType(),True) \
    .add("fec_proc",IntegerType(),True) 
	
c4_CursesDF =  spark.read.csv("dbfs:/mnt/ingestion/dev/riesgobdu_fs/segm_cloud/202206/c4_Curses.txt", header=False, sep="|", schema=schema_c4_Curses)	

c4_CursesDF.createOrReplaceTempView("tbl_c4_curses")

auxOpeDF = spark.read.csv("dbfs:/mnt/ingestion/dev/riesgobdu_fs/segm_cloud/202206/c4_Curses.txt", header=False, sep="|")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### c4_Curses_02.txt

# COMMAND ----------

schema_c4_Curses_02 = StructType() \
    .add("rut_cli",IntegerType(),True) \
    .add("dv",StringType(),True) \
    .add("num_interno_ident",StringType(),True) \
    .add("fecha_NN",StringType(),True) \
    .add("dias_mora",IntegerType(),True) \
    .add("sistema",IntegerType(),True) \
    .add("ind_tipo_seg",StringType(),True) \
    .add("tipo_credito",IntegerType(),True) \
    .add("tipo_operacion",StringType(),True) \
    .add("nro_NN",IntegerType(),True) \
    .add("fec_proc",IntegerType(),True) 
	
c4_Curses_02DF =  spark.read.csv("dbfs:/mnt/ingestion/dev/riesgobdu_fs/segm_cloud/202206/c4_Curses_02.txt", header=False, sep="|", schema=schema_c4_Curses_02)	

c4_Curses_02DF.createOrReplaceTempView("tbl_c4_curses_02")

auxOpeDF = spark.read.csv("dbfs:/mnt/ingestion/dev/riesgobdu_fs/segm_cloud/202206/c4_Curses_02.txt", header=False, sep="|")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### c4_ope_vta_con_crs_baj_mor.txt

# COMMAND ----------

schema_c4_ope_vta = StructType() \
    .add("rut_cli",IntegerType(),True) \
    .add("num_interno_ident_venta",StringType(),True) \
    .add("sistema_venta",IntegerType(),True) \
    .add("num_interno_ident_mor",StringType(),True) \
    .add("sistema_mor",IntegerType(),True) \
    .add("dias_mora",IntegerType(),True) 
	
c4_ope_vtaDF =  spark.read.csv("dbfs:/mnt/ingestion/dev/riesgobdu_fs/segm_cloud/202206/c4_ope_vta_con_crs_baj_mor.txt", header=False, sep="|", schema=schema_c4_ope_vta)	

c4_ope_vtaDF.createOrReplaceTempView("tbl_c4_tmp_ope_vta_con_crs_baj_mor")

auxOpeDF = spark.read.csv("dbfs:/mnt/ingestion/dev/riesgobdu_fs/segm_cloud/202206/c4_ope_vta_con_crs_baj_mor.txt", header=False, sep="|")

# COMMAND ----------

 dbutils.fs.rm("abfss://gold@bcirg2dlssbx.dfs.core.windows.net/riesgobdu_db/tbl_segmentacion_ope_factor", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Jerarquias

# COMMAND ----------

schema_jerarquia = StructType() \
    .add("concepto",StringType(),True) \
    .add("criterio",IntegerType(),True) \
    .add("descripcion",StringType(),True) \
    .add("jerarquia",IntegerType(),True) \
	
jerarquiaDF =  spark.read.csv("/FileStore/tables/Jerarquia.txt", header=False, sep=";", schema=schema_jerarquia)	

jerarquiaDF.createOrReplaceTempView("tbl_jerarquia")

auxOpeDF = spark.read.csv("/FileStore/tables/Jerarquia.txt", header=False, sep=";")


# COMMAND ----------

# MAGIC %md
# MAGIC ### c4_pag_cons_ibm

# COMMAND ----------

schema_contpag = StructType() \
    .add("fec_proc",IntegerType(),True) \
    .add("sistema",IntegerType(),True) \
    .add("num_interno_ident",StringType(),True) \
    .add("rut_cli",IntegerType(),True) \
	.add("fec_inicio",IntegerType(),True) \
    .add("ind_tipo_seg",StringType(),True) \
    .add("cod_tioaux",StringType(),True) \
    .add("cod_criterio",IntegerType(),True) \
    .add("fec_ini_cd",IntegerType(),True) \
    .add("cont_pag_cons",IntegerType(),True) \

contpagDF =  spark.read.csv("/FileStore/tables/c4_pag_cons_ibm.txt", header=False, sep="|", schema=schema_contpag)	

contpagDF.createOrReplaceTempView("tbl_c4_tmp_ope_pag_cons_ibm")

auxOpeDF = spark.read.csv("/FileStore/tables/c4_pag_cons_ibm.txt", header=False, sep="|")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### c4_Mes_cartde

# COMMAND ----------

schema_mescart = StructType() \
    .add("fec_proc",IntegerType(),True) \
    .add("sistema",IntegerType(),True) \
    .add("num_interno_ident",StringType(),True) \
    .add("rut_cli",IntegerType(),True) \
	.add("fec_inicio",IntegerType(),True) \
    .add("ind_tipo_seg",StringType(),True) \
    .add("cod_tioaux",StringType(),True) \
    .add("cod_criterio",IntegerType(),True) \
    .add("fec_ini_cd",IntegerType(),True) \
    .add("num_meses",IntegerType(),True) \

mescartDF =  spark.read.csv("/FileStore/tables/c4_Mes_cartde.txt", header=False, sep="|", schema=schema_mescart)	

mescartDF.createOrReplaceTempView("tbl_c4_tmp_gru_ope_ent_meses")

auxOpeDF = spark.read.csv("/FileStore/tables/c4_Mes_cartde.txt", header=False, sep="|")



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### c4_tmp_ope_ctas_par

# COMMAND ----------

schema_pagparc = StructType() \
    .add("fec_proc",IntegerType(),True) \
    .add("rut_cli",IntegerType(),True) \
    .add("dv",StringType(),True) \
    .add("sistema",IntegerType(),True) \
    .add("num_interno_ident",StringType(),True) \
	.add("fec_otorg",IntegerType(),True) \
    .add("fec_ext",StringType(),True) \
    .add("nro_ctas",IntegerType(),True) \
    .add("periodo",IntegerType(),True) \

pagparcDF =  spark.read.csv("/FileStore/tables/c4_ope_pag_parc.txt", header=False, sep="|", schema=schema_pagparc)	

pagparcDF.createOrReplaceTempView("tbl_c4_tmp_ope_ctas_par")

auxOpeDF = spark.read.csv("/FileStore/tables/c4_ope_pag_parc.txt", header=False, sep="|")


# COMMAND ----------

# MAGIC %md
# MAGIC ### c4_arch_ope_dmor

# COMMAND ----------

schema_dmor = StructType() \
    .add("fec_proc",IntegerType(),True) \
    .add("rut_cli",IntegerType(),True) \
    .add("num_interno_ident",StringType(),True) \
    .add("sistema",IntegerType(),True) \
	.add("dia_mora_mes",IntegerType(),True) \
    .add("dia_mora_fin_mes",IntegerType(),True) \

dmorDF =  spark.read.csv("/FileStore/tables/c4_arch_ope_dmor.txt", header=False, sep="|", schema=schema_dmor)	

dmorDF.createOrReplaceTempView("tbl_c4_tmp_gru_ope_dia_mor")

auxOpeDF = spark.read.csv("/FileStore/tables/c4_arch_ope_dmor.txt", header=False, sep="|")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### c4_gru_Ren_Cardet_IFRS

# COMMAND ----------

schema_cren = StructType() \
    .add("fec_proc",IntegerType(),True) \
    .add("sistema",IntegerType(),True) \
    .add("num_interno_ident",StringType(),True) \
	.add("ind_tipo_seg",StringType(),True) \
    .add("rut_cli",IntegerType(),True) \
    .add("tipo_credito",IntegerType(),True) \
    .add("saldo_moroso2",IntegerType(),True) \
    .add("saldo_en_cart_venc",IntegerType(),True) \
    .add("nro_ctas_amort_pend",IntegerType(),True) \
    .add("cuenta_contable",IntegerType(),True) \
    .add("val_skif",IntegerType(),True) \
    .add("val_inif",IntegerType(),True) \
    .add("val_reif",IntegerType(),True) \
    .add("saldo_reprogramado",IntegerType(),True) \
	.add("cod_oren",StringType(),True) \
    .add("fecha_otorgam",IntegerType(),True) \
	.add("tipo_operacion",StringType(),True) \

crenDF =  spark.read.csv("/FileStore/tables/c4_gru_Ren_Cardet_IFRS.txt", header=False, sep="|", schema=schema_cren)	

crenDF.createOrReplaceTempView("tbl_c4_tmp_gru_ope_cond_ren")

auxOpeDF = spark.read.csv("/FileStore/tables/c4_gru_Ren_Cardet_IFRS.txt", header=False, sep="|")


# COMMAND ----------

# MAGIC %md
# MAGIC ### c4_dat_ope_ini_cd

# COMMAND ----------

schema_inicd = StructType() \
    .add("fec_proc",IntegerType(),True) \
    .add("sistema",IntegerType(),True) \
    .add("num_interno_ident",StringType(),True) \
    .add("ind_tipo_seg",StringType(),True) \
    .add("rut_cli",IntegerType(),True) \
    .add("fec_ini_cd",IntegerType(),True) \
    .add("nro_ctas_amort_pend",IntegerType(),True) \
	.add("val_skif",IntegerType(),True) \
    .add("ori_det",IntegerType(),True) \

inicdDF =  spark.read.csv("/FileStore/tables/c4_dat_ope_ini_cd.txt", header=False, sep="|", schema=schema_inicd)	

inicdDF.createOrReplaceTempView("tbl_c4_dat_ope_ini_cd")

auxOpeDF = spark.read.csv("/FileStore/tables/c4_dat_ope_ini_cd.txt", header=False, sep="|")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tbl_c4_dat_ope_ini_cd

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### tbl_condicion_salida

# COMMAND ----------

schema_consal = StructType() \
    .add("familia_ope",StringType(),True) \
    .add("evaluacion",StringType(),True) \
    .add("resultado",IntegerType(),True) \
    .add("criterio_salida",IntegerType(),True) \

consalDF =  spark.read.csv("/FileStore/tables/condicion_salida-2.txt", header=True, sep="|", schema=schema_consal)	

consalDF.createOrReplaceTempView("tbl_condicion_salida")

auxOpeDF = spark.read.csv("/FileStore/tables/condicion_salida-2.txt", header=True, sep="|")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### tbl_excp_mto_cero

# COMMAND ----------

schema_consal = StructType() \
    .add("sistema",IntegerType(),True) \
    .add("tipo_operacion",StringType(),True) \
    .add("concepto",StringType(),True) \
    .add("vigencia",StringType(),True) \

consalDF =  spark.read.csv("/FileStore/tables/excp_mto_cero_27.txt", header=False, sep="|", schema=schema_consal)	

consalDF.createOrReplaceTempView("tbl_excp_mto_cero")

auxOpeDF = spark.read.csv("/FileStore/tables/excp_mto_cero.txt", header=False, sep="|")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tbl_excp_mto_cero

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### jer_cri_ent

# COMMAND ----------

schema_consal = StructType() \
    .add("criterio",StringType(),True) \
    .add("jerarquia",IntegerType(),True) \
    .add("concepto",StringType(),True) \
    .add("vigencia",StringType(),True) \

consalDF =  spark.read.csv("/FileStore/tables/jer_cri_ent.txt", header=False, sep="|", schema=schema_consal)	

consalDF.createOrReplaceTempView("tbl_jer_cri_ent")

auxOpeDF = spark.read.csv("/FileStore/tables/jer_cri_ent.txt", header=False, sep="|") 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe  tbl_jer_cri_ent

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe tbl_jer_cri_ent

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from tbl_c4_hist_cart_det where rut_cli = 13883811 and fec_proc like '%Jun 30 2022%' 
# MAGIC 
# MAGIC select * from tbl_c4_hist_ope_crit where rut_cli = 12254846    -- and fec_proc like '%Jun 30 2022%' 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tbl_c4_tmp_gru_ope_dia_mor where rut_cli = 13883811
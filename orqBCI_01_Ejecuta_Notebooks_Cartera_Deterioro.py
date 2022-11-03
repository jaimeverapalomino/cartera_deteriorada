# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: orqBCI_01_Ejecuta_Notebooks_Cartera_Deterioro
# MAGIC **************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: orqBCI_01_Ejecuta_Notebooks_Cartera_Deterioro.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Jaime Vera (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 25/10/2022
# MAGIC * Descripcion: Orquestador Segmentación
# MAGIC ***************************************
# MAGIC * Documentacion: 
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
# MAGIC * 
# MAGIC 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * 
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios
# MAGIC ********************************************************************************************************
# MAGIC ##### Pendientes:
# MAGIC 
# MAGIC ##### Ayuda Link:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Dependencias

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Notebook con funciones genéricas

# COMMAND ----------

# MAGIC %run "../segmentacion/Funciones_Comunes"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tiempo de inicio del proceso

# COMMAND ----------

hora_ini = datetime.now()
diaEjecucionX = hora_ini.strftime('%Y%m%d')
horaIniX = hora_ini.strftime('%H%M%S')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga liberías

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parámetros

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setea Parámetros

# COMMAND ----------

#dbutils.widgets.removeAll()
dbutils.widgets.text("FechaW","","01-Fecha:")
dbutils.widgets.text("PeriodoW","","02-Periodo:")
dbutils.widgets.text("bd_silverW","","03-Nombre BD Silver:")
dbutils.widgets.text("bd_goldW","","04-Nombre BD Gold:")
dbutils.widgets.text("tipo_procesoW","C","05-Tipo de Proceso (C o PC):")
dbutils.widgets.text("guardar_logW","S","06-Guarda Log em BD (S o N):")

FechaX = dbutils.widgets.get("FechaW") 
PeriodoX = dbutils.widgets.get("PeriodoW")
base_silverX = dbutils.widgets.get("bd_silverW")
base_goldX = dbutils.widgets.get("bd_goldW")
tipo_procesoX = dbutils.widgets.get("tipo_procesoW")
guardar_logX = dbutils.widgets.get("guardar_logW")

spark.conf.set("bci.Fecha", FechaX)
spark.conf.set("bci.Periodo", PeriodoX)
spark.conf.set("bci.dbnamesilver", base_silverX)
spark.conf.set("bci.dbnameGold", base_goldX)
spark.conf.set("bci.tipo_proceso", tipo_procesoX)
spark.conf.set("bci.guarda_log", guardar_logX)

print(f"Fecha de Proceso: {FechaX}")
print(f"Periodo: {PeriodoX}")      
print(f"Nombre BD Silver: {base_silverX}")
print(f"Nombre BD Gold: {base_goldX}")
print(f"Tipo de Proceso: {tipo_procesoX}")
print(f"Registrar Log: {guardar_logX}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## INICIO Flujo ejecución de Segmentación

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_01_Valida_Inputs

# COMMAND ----------

EjecutaNotebook("BCI_00_Valida_Inputs", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_01_CarDet_Ind_Crit_Ent

# COMMAND ----------

EjecutaNotebook("BCI_01_CarDet_Ind_Crit_Ent", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX},  
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_02_CarDet_Ind_Crit_Ent_Crit

# COMMAND ----------

EjecutaNotebook("BCI_02_CarDet_Ind_Crit_Ent_Crit", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_03_CarDet_Ind_Crit_Ent_Prin

# COMMAND ----------

EjecutaNotebook("BCI_03_CarDet_Ind_Crit_Ent_Prin", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_04_CarDet_Ind_Crit_Sal

# COMMAND ----------

EjecutaNotebook("BCI_04_CarDet_Ind_Crit_Sal", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_05_CarDet_Ind_Crit_Sal_Crit

# COMMAND ----------

EjecutaNotebook("BCI_05_CarDet_Ind_Crit_Sal_Crit", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_06_CarDet_Ind_Crit_Sal_Prin

# COMMAND ----------

EjecutaNotebook("BCI_06_CarDet_Ind_Crit_Sal_Prin", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_07_CarDet_Gru_Crit_Ent_7

# COMMAND ----------

EjecutaNotebook("BCI_07_CarDet_Gru_Crit_Ent_7", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_08_CarDet_Gru_Crit_Ent_8

# COMMAND ----------

EjecutaNotebook("BCI_08_CarDet_Gru_Crit_Ent_8", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_09_CarDet_Gru_Crit_Ent_9

# COMMAND ----------

EjecutaNotebook("BCI_09_CarDet_Gru_Crit_Ent_9", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_10_CarDet_Gru_Crit_Ent_10

# COMMAND ----------

 EjecutaNotebook("BCI_10_CarDet_Gru_Crit_Ent_10", 0 , 
                 {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                 PeriodoX, 
                 FechaX, 
                 tipo_procesoX,
                 guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_11_CarDet_Gru_Crit_Ent_11

# COMMAND ----------

EjecutaNotebook("BCI_11_CarDet_Gru_Crit_Ent_11", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_12_CarDet_Gru_Crit_Ent_12

# COMMAND ----------

EjecutaNotebook("BCI_12_CarDet_Gru_Crit_Ent_12", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_13_CarDet_Gru_Crit_Ent_13

# COMMAND ----------

EjecutaNotebook("BCI_13_CarDet_Gru_Crit_Ent_13", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_14_CarDet_Gru_Crit_Ent_Crit

# COMMAND ----------

EjecutaNotebook("BCI_14_CarDet_Gru_Crit_Ent_Crit", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_15_CarDet_Gru_Crit_Ent_Prin

# COMMAND ----------

EjecutaNotebook("BCI_15_CarDet_Gru_Crit_Ent_Prin", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_17_CarDet_Gru_Crit_Sal_27

# COMMAND ----------

EjecutaNotebook("BCI_17_CarDet_Gru_Crit_Sal_27", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_18_CarDet_Gru_Crit_Sal_30

# COMMAND ----------

EjecutaNotebook("BCI_18_CarDet_Gru_Crit_Sal_30", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_24_CarDet_Gru_Crit_Sal_40

# COMMAND ----------

EjecutaNotebook("BCI_24_CarDet_Gru_Crit_Sal_40", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_25_CarDet_Gru_Crit_Sal_41

# COMMAND ----------

EjecutaNotebook("BCI_25_CarDet_Gru_Crit_Sal_41", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_26_CarDet_Gru_Crit_Sal_Mora_Cli

# COMMAND ----------

EjecutaNotebook("BCI_26_CarDet_Gru_Crit_Sal_Mora_Cli", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_27_CarDet_Gru_Crit_Sal_Pagos_Cons

# COMMAND ----------

EjecutaNotebook("BCI_27_CarDet_Gru_Crit_Sal_Pagos_Cons", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_28_CarDet_Gru_Crit_Sal_Mora_Sbif

# COMMAND ----------

EjecutaNotebook("BCI_28_CarDet_Gru_Crit_Sal_Mora_Sbif", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_29_CarDet_Gru_Crit_Sal_Pagos_Parc

# COMMAND ----------

EjecutaNotebook("BCI_29_CarDet_Gru_Crit_Sal_Pagos_Parc", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_30_CarDet_Gru_Crit_Sal_Sin_Ref

# COMMAND ----------

EjecutaNotebook("BCI_30_CarDet_Gru_Crit_Sal_Sin_Ref", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_31_CarDet_Gru_Crit_Sal_Pago_Cap

# COMMAND ----------

EjecutaNotebook("BCI_31_CarDet_Gru_Crit_Sal_Pago_Cap", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_32_CarDet_Gru_Crit_Sal_Min_Meses

# COMMAND ----------

EjecutaNotebook("BCI_32_CarDet_Gru_Crit_Sal_Min_Meses", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_33_CarDet_Gru_Crit_Sal

# COMMAND ----------

EjecutaNotebook("BCI_33_CarDet_Gru_Crit_Sal", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_34_CarDet_Gru_Crit_Sal_Fam

# COMMAND ----------

EjecutaNotebook("BCI_34_CarDet_Gru_Crit_Sal_Fam", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_35_CarDet_Gru_Crit_Sal_Crit

# COMMAND ----------

EjecutaNotebook("BCI_35_CarDet_Gru_Crit_Sal_Crit", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_36_CarDet_Gru_Crit_Sal_Prin

# COMMAND ----------

EjecutaNotebook("BCI_36_CarDet_Gru_Crit_Sal_Prin", 0 , 
                {"FechaW":FechaX,"PeriodoW": PeriodoX,"bd_silverW": base_silverX, "bd_goldW": base_goldX, "tipo_procesoW": tipo_procesoX}, 
                PeriodoX, 
                FechaX, 
                tipo_procesoX,
                guardar_logX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## FIN Flujo ejecución de Segmentación

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen Duración Proceso

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tiempo de fin del proceso

# COMMAND ----------

hora_fin = datetime.now()

delta = hora_fin - hora_ini
delta = str(delta).split(".")[0]

horaFinX = hora_fin.strftime('%H%M%S')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Estadísticas de ejecución

# COMMAND ----------

print("Inicio Proceso:", hora_ini.strftime('%d/%m/%Y %H:%M:%S'))
print("Fin Proceso   :", hora_fin.strftime('%d/%m/%Y %H:%M:%S'))
print("Duración      :", delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")

# COMMAND ----------


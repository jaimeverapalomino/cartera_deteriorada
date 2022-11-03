# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_00_Valida_Inputs
# MAGIC **************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_00_Valida_Inputs.ipynb
# MAGIC * Ruta: 
# MAGIC * Autor: Jaime Vera (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 30/08/2022
# MAGIC * Descripcion: Valida todos los parámetros de entrada ingresados en el orquestador. Estos parámetros de entrada son enviados después en los demás notebooks de procesamiento.
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
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cierreriesgo_log
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
# MAGIC ### Carga funciones comunes

# COMMAND ----------

# MAGIC %run "../segmentacion/Funciones_Comunes"

# COMMAND ----------

  diaIniX = datetime.now()
  dia_ejecucion = diaIniX.strftime('%Y%m%d')
  hora_inicio =  diaIniX.strftime('%H%M%S')
  print(hora_inicio)

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
# MAGIC ### Valida Parámetros

# COMMAND ----------

# MAGIC %md
# MAGIC #### FechaX

# COMMAND ----------

validaParametro(FechaX, 'Fecha')

# COMMAND ----------

# MAGIC %md
# MAGIC #### PeriodoX

# COMMAND ----------

validaParametro(PeriodoX, 'Período')

# COMMAND ----------

# MAGIC %md
# MAGIC #### base_silverX

# COMMAND ----------

validaParametro(base_silverX, 'Base de Datos Silver')

# COMMAND ----------

# MAGIC %md
# MAGIC #### base_goldX

# COMMAND ----------

validaParametro(base_goldX, 'Base de Datos Gold')

# COMMAND ----------

# MAGIC %md
# MAGIC #### tipo_procesoX

# COMMAND ----------

validaParametro(tipo_procesoX, 'Tipo de Proceso')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Valida Fecha Válida

# COMMAND ----------

validaFechaEntrada(FechaX)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Valida Período Válido

# COMMAND ----------

validaPeriodoEntrada(PeriodoX)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Valida "tipo_procesoX" Válido

# COMMAND ----------

if tipo_procesoX not in ["C","PC"]:
    dbutils.notebook.exit("{\"coderror\":28001, \"msgerror\":\"Tipo de proceso inválido: "+str(tipo_procesoX)+" .Valores permitidos: C o PC\"}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Valida Base de Datos

# COMMAND ----------

# MAGIC %md
# MAGIC ### base_silverX

# COMMAND ----------

validaBD(base_silverX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### base_goldX

# COMMAND ----------

validaBD(base_goldX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorX="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorX+"\"}")
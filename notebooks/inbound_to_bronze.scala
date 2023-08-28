// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls('/mnt/dados/inbound')

// COMMAND ----------

// MAGIC %md
// MAGIC #Lendo os dados na camada inbound

// COMMAND ----------

val path= "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados= spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// MAGIC %md
// MAGIC # Removendo colunas
// MAGIC

// COMMAND ----------

val dados_anuncio= dados.drop("imagens", "usuario")
display(dados_anuncio)

// COMMAND ----------

// MAGIC %md
// MAGIC # Criando uma coluna a partir do campo ID

// COMMAND ----------

import org.apache.spark.sql.functions.col


// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// MAGIC %md
// MAGIC # Salvando dados na camada bronze

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------

  

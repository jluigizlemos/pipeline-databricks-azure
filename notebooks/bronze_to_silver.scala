// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls('/mnt/dados/inbound')

// COMMAND ----------

val path= "dbfs:/mnt/dados/bronze/dataset_imoveis/"
val df= spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC # Transformando cada campo da coluna anuncio em um coluna

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")

// COMMAND ----------

display(dados_detalhados)

// COMMAND ----------

// MAGIC %md
// MAGIC # Removendo colunas

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
display(df_silver)

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------



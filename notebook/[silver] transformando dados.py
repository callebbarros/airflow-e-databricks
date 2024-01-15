# Databricks notebook source
from pyspark.sql.functions import to_date, first, avg, col, round

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-results/bronze/2023/"))

# COMMAND ----------

df_junto = spark.read.parquet("dbfs:/databricks-results/bronze/*/*/*")

# COMMAND ----------

df_junto.count()

# COMMAND ----------

df_junto.select("moeda").distinct().count()

# COMMAND ----------

moedas = ['USD', 'EUR', 'GBP']
df_moedas = df_junto.filter(df_junto.moeda.isin(moedas))

# COMMAND ----------

df_moedas.select("moeda").distinct().count()

# COMMAND ----------

df_moedas.printSchema()

# COMMAND ----------

#alterando tipo
df_moedas = df_moedas.withColumn("data", to_date("data"))

# COMMAND ----------

result_taxas_conversao = df_moedas.groupBy('data') \
    .pivot("moeda")\
    .agg(first('taxa'))\
    .orderBy("data", ascending = False)

# COMMAND ----------

result_conversao_real  = result_taxas_conversao.select("*")

# COMMAND ----------

for m in moedas:
    result_conversao_real = result_conversao_real.withColumn(m, round(1/col(m), 4))

# COMMAND ----------

result_conversao_real.show()

# COMMAND ----------

result_taxas_conversao = result_taxas_conversao.coalesce(1) 
result_conversao_real = result_conversao_real.coalesce(1) 

# COMMAND ----------

result_taxas_conversao.write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/databricks-results/prata/taxas_conversao")
    
result_conversao_real.write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/databricks-results/prata/valores_reais")


# COMMAND ----------

#dbutils.fs.ls("dbfs:/databricks-results/prata/valores_reais")

# COMMAND ----------

#dbutils.fs.rm("dbfs:/databricks-results/prata/valores_reais", True)

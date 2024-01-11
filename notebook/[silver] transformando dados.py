# Databricks notebook source
display(dbutils.fs.ls("dbfs:/databricks-results/bronze/2023/"))

# COMMAND ----------

spark.read.parquet("dbfs:/databricks-results/bronze/*/*/*").show(5)


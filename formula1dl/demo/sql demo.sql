-- Databricks notebook source
create database demo

-- COMMAND ----------

-- MAGIC %run "../includes/config"
-- MAGIC

-- COMMAND ----------

show databases

-- COMMAND ----------

use demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format("delta").saveAsTable("demo.race_results_py")

-- Databricks notebook source
drop database if exists f1_processed cascade

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dlsree/transforrmed"

-- COMMAND ----------

DESC DATABASE f1_processed;


-- COMMAND ----------

DESC DATABASE f1_presented;


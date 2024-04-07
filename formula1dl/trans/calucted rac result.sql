-- Databricks notebook source
use f1_pro CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
              (cessed

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS  f1_presented.calculated_race_results
AS
SELECT races.race_year,
       constructors.name AS team_name,
       drivers.name AS driver_name,
       results.position,
       results.points,
       11 - results.position AS calculated_points
FROM results
JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
                JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
                JOIN f1_processed.races ON (results.race_id = races.race_id)
WHERE results.position <= 10

-- COMMAND ----------

n

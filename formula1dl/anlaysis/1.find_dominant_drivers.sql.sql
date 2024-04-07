-- Databricks notebook source
SELECT driver_name,
       team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
FROM f1_presented.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name, team_name
HAVING COUNT(1) >= 40
ORDER BY avg_points DESC

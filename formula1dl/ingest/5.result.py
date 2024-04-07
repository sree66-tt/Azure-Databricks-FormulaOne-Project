# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

results_schema = StructType(fields =[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statustId", StringType(), True),

])


# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId","result_id") \
                                    .withColumnRenamed("raceId","race_id") \
                                    .withColumnRenamed("driverId","driver_id") \
                                    .withColumnRenamed("constructorId","constructor_id") \
                                    .withColumnRenamed("positionText","position_text") \
                                    .withColumnRenamed("positionOrder","position_order") \
                                    .withColumnRenamed("fastestLap","fastest_lap") \
                                    .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                                    .withColumn("ingestion_date",current_timestamp()).withColumn("data_source", lit(v_data_source)) 


# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statustId"))


# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
    deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlsree/transforrmed/results")
    deltaTable.alias("tgt").merge(
        results_final_df.alias("src"),
        "tgt.result_id = src.result_id") \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()
else:
    results_final_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,COUNT(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

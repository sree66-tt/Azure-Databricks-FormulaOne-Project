# Databricks notebook source

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lapt_time_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("lap",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)
])

# COMMAND ----------

lap_time_df = spark.read\
.schema(lapt_time_schema)\
.csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

lap_times_with_ingestion_date_df = add_ingestion_date(lap_time_df)

# COMMAND ----------

final_df = lap_time_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id")\
    .withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)) 

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.laptime")

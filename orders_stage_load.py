# Databricks notebook source
source_dir = "/Volumes/incremental_data_load_project_01/default/orders_data/source/" #this is our source volume mount point
target_dir = "/Volumes/incremental_data_load_project_01/default/orders_data/archive/"  # this is our target volume mount point
stage_table = "incremental_data_load_project_01.default.orders_stage"

# COMMAND ----------

#reading data from source directory
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(source_dir)

# COMMAND ----------

# create Delta table named stage_zn if not exists and overwrite the data in stage table 
df.write.format("delta").mode("overwrite").saveAsTable(stage_table)

# COMMAND ----------

import os
import shutil

# List all files in the source directory
files = dbutils.fs.ls(source_dir)

# Iterate on the list one by one and print each file path separately
for file in files:
    file_path = file.path
    print(file_path)
    
    # Construct the target path
    target_path = os.path.join(target_dir, os.path.basename(file_path))
    
    # Move the files from source to target folder
    dbutils.fs.mv(file_path, target_path)

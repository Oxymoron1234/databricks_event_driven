# Databricks notebook source
from delta.tabless improt *
stage_table_name = "incremental_data_load_project_01.default.orders_stage"
target_table_name = "incremental_data_load_project_01.default.orders_stage_target"

# COMMAND ----------

#Read data from stage table
stage_df = spark.read.table(stage_table_name)

# COMMAND ----------

#Create eqivalent table in target schema if not exists 
if not spark._jsparkSession.catalog().tableExists(target_table_name):
    stage_df.write.format("delta").saveAsTable(target_table_name)
else:
    print("Target table already exists")
    #perform delta table merge quesry from upsert based on tracking_id column
    target_table = DeltaTable.forName(spark, target_table_name)

    #Define the merge condition based on the target.target_id column
    merge_condition = "target.tracking_num = stage.tracking_num"

    #Execute the merge query
    target_table.alias("target").merge(stage_df.alias("stage"), merge_condition).whenMatchedDelete().whenNotMatchedInsertAll().execute()
    
    stage_df.write.format("delta").mode("append").saveAsTable(target_table_name)

# Databricks notebook source
# MAGIC %md
# MAGIC ##Dynamic data reading

# COMMAND ----------

# MAGIC %md
# MAGIC Parameter

# COMMAND ----------

dbutils.widgets.text("src_container","")
dbutils.widgets.text("sink_container","")
dbutils.widgets.text("folder","")

# COMMAND ----------

src_container = dbutils.widgets.get("src_container")
sink_container = dbutils.widgets.get("sink_container")
folder = dbutils.widgets.get("folder")


# COMMAND ----------

df=spark.read.format("parquet").\
    load(f"abfss://{src_container}@paris2024olympic.dfs.core.windows.net/{folder}")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('delta').\
    mode('append').\
    option('path',f'abfss://{sink_container}@paris2024olympic.dfs.core.windows.net/{folder}').\
    saveAsTable(f'olympic.{sink_container}.{folder}')
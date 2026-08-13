# Databricks notebook source
# MAGIC %md
# MAGIC ##Read NOCS

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_nocs=spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema','true')\
    .load('abfss://bronze@paris2024olympic.dfs.core.windows.net/nocs')


# COMMAND ----------

df_nocs.display()

# COMMAND ----------

df_nocs=df_nocs.drop('country')
df_nocs.display()

# COMMAND ----------

df_nocs = df_nocs.withColumn('tag', split(col('tag'), '-')[0])
df_nocs.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write NOCS

# COMMAND ----------

df_nocs.write.format('delta')\
    .mode('append')\
    .option('path','abfss://silver@paris2024olympic.dfs.core.windows.net/nocs')\
    .saveAsTable("olympic.silver.nocs")
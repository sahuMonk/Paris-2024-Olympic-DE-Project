# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df=spark.read.format('parquet').\
    load('abfss://bronze@paris2024olympic.dfs.core.windows.net/athletes')

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.fillna({'birth_place':'xyz','birth_country':'abc','residence_place':'unknown','residence_country':'aaa'})

# COMMAND ----------

df_filt = df.filter((col("current")==True) & (col("name").isin('GALSTYAN Slavik','HARUTYUNYAN Arsen','SEHEN Sajjad'))).display()

# COMMAND ----------

df= df.withColumn('height',col("height").cast(FloatType()))\
    .withColumn('weight',col("weight").cast(FloatType()))\
    .withColumn('birth_date',col("birth_date").cast(DateType()))\
    .withColumn('code',col("code").cast(IntegerType()))
df.display()

# COMMAND ----------

df_sorted= df.sort('height','weight',ascending=[0,1]).filter(col("weight")>0)

# COMMAND ----------

df_sorted.display()

# COMMAND ----------

df_sorted = df_sorted.withColumn('nationality',regexp_replace('nationality','United States','US'))

# COMMAND ----------

df_sorted.display()

# COMMAND ----------

df_sorted.groupBy("country_code").agg(count('code').alias('total_player')).sort('total_player',ascending=[0]).display()

# COMMAND ----------

df_sorted=df_sorted.withColumnRenamed('code','athlete_id')


# COMMAND ----------

df_sorted=df_sorted.withColumn('occupation',split(col('occupation'),','))
df_sorted.display()

# COMMAND ----------

df_sorted.columns

# COMMAND ----------

df_final=df_sorted.select('athlete_id','name','gender','country_code','nationality','height','weight','birth_date','birth_country','events','coach','education','occupation')

# COMMAND ----------

df_final.display()


# COMMAND ----------

df_final.withColumn('cum_weight',sum('weight').over(Window.partitionBy('nationality').orderBy('height').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

df_final.write.format("delta").\
    mode("append").\
    option('path','abfss://silver@paris2024olympic.dfs.core.windows.net/athletes').\
    saveAsTable('olympic.silver.athletes')
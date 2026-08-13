# Databricks notebook source
# MAGIC %md
# MAGIC ##delta live table- Gold layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### coaches DLT pipline

# COMMAND ----------

import dlt
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %md
# MAGIC ### Expectations for data quality

# COMMAND ----------

expec_coaches =
    {
        "rule1":"code is not null",
        "rule2": "current is True"
    }

# COMMAND ----------

expec_nocs =
    {
        "rule1":"code is not null"
    }

# COMMAND ----------

expec_events =
    {
        "rule1":"event is not null"
    }

# COMMAND ----------

@dlt.table

def source_coaches():
    df=spark.readStream.table('olympic.silver.coaches')
    return df

# COMMAND ----------

@dlt.view

def view_coaches():
    df=spark.readStream.table('LIVE.source_coaches')
    return df

# COMMAND ----------

@dlt.table

@dlt.expect_all(expec_coaches)
def coaches():
    df=spark.readStream.table('LIVE.view_coaches')
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### nocs dlt pipline

# COMMAND ----------

@dlt.view

def view_nocs():
    df=spark.readStream.table('olympic.silver.nocs')
    return df

# COMMAND ----------

@dlt.table

@dlt.expect_all_or_drop(expec_nocs)
def nocs():
    df=spark.readStream.table('LIVE.view_nocs')
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### events dlt pipline

# COMMAND ----------

@dlt.view

def view_events():
    df=spark.readStream.table('olympic.silver.events')
    return df

# COMMAND ----------

@dlt.table

@dlt.expect_all(expec_events)
def events():
    df=spark.readStream.table('LIVE.view_events')
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDC- Apply changes dlt

# COMMAND ----------

@dlt.view

def view_athlete():
    df=spark.readStream.table('olympic.silver.athletes')
    return df

# COMMAND ----------

dlt.create_streaming_table('athletes')

# COMMAND ----------

dlt.apply_changes(
    target = 'athletes',
    source = 'view_athlete',
    keys = ['athlete_id'],
    sequence_by = col("height"),
    stored_as_scd_type=1
)

# COMMAND ----------


# Databricks notebook source
my_arr=[
    {
        "src_container":'bronze',
        'sink_container':'silver',
        'folder':'events'
    },
    {
        "src_container":'bronze',
        'sink_container':'silver',
        'folder':'coaches'
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("my_output",my_arr)
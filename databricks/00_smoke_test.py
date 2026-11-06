# Databricks notebook source
print(spark.read.format("delta").load("abfss://gold@stuaebankdev417.dfs.core.windows.net/delta/dim_date").count())
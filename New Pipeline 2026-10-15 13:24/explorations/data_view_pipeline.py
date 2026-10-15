# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC select * from vp_vk_mkc20040528.bronze.tariffs

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vp_vk_mkc20040528.silver.tariffs
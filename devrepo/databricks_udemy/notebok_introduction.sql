-- Databricks notebook source


-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ps

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('/databricks-datasets/COVID/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for files in dbutils.fs.ls ('/databricks-datasets/COVID/'):
-- MAGIC     print(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for files in dbutils.fs.ls('/databricks-datasets/COVID'):
-- MAGIC     if files.name.endswith('/'):
-- MAGIC       print(files.name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.help('ls')
-- MAGIC

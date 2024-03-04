-- Databricks notebook source
use dbsql

-- COMMAND ----------

create table emp (id int, name string , sal double)

-- COMMAND ----------

insert into emp values(7,'ptg',10000000000000),(8,'pir',20),(9,'prg',3000),(10,'pri',400),(11,'pra',50),(12,'ira',6000)

-- COMMAND ----------

select * from emp

-- COMMAND ----------

describe detail emp

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls  'dbfs:/user/hive/warehouse/dbsql.db/emp'
-- MAGIC

-- COMMAND ----------

update emp set sal =900 where id in (11,12)

-- COMMAND ----------

describe history  emp

-- COMMAND ----------

select * from emp@v3

-- COMMAND ----------

optimize emp
zorder by id

-- COMMAND ----------

desc history emp

-- COMMAND ----------

describe detail emp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

vacuum emp RETAIN 168 HOURS

-- COMMAND ----------

set spark.databricks.delta.rentionDurationCheck.enabled=false

-- COMMAND ----------

select *, input_file_name() source_file from text.`dbfs:/FileStore/Address.json`

-- COMMAND ----------

select * from t_view

-- COMMAND ----------

create  or replace temporary view t_view as select * from json.`dbfs:/FileStore/Address.json`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json
-- MAGIC with  open(f"/dbfs/FileStore/Address.json","r") as f:
-- MAGIC     js = json.load(f)
-- MAGIC display(js)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=dbutils.fs.ls("dbfs:/FileStore/circuits.csv")
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------



-- COMMAND ----------



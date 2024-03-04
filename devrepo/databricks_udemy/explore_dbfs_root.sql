-- Databricks notebook source
-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/FileStore'))

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.csv('/FileStore/circuits.csv'))

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC c_df=spark.read.csv('/FileStore/circuits.csv')
-- MAGIC display(c_df)
-- MAGIC type(c_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC c_df=spark.read.option("header",True).option("inferschema",True).csv('/FileStore/circuits.csv')
-- MAGIC c_df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC  out_c_df=c_df.select("circuitid", "circuitref","name","location","country","lat","lng","alt")
-- MAGIC display(out_c_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark .sql.functions import col

-- COMMAND ----------

-- MAGIC %python
-- MAGIC c_df_select =out_c_df.select(col("circuitid"),col("lat"))
-- MAGIC display(c_df_select)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cir_ren_df=out_c_df.withColumnRenamed("circuitid","circuit_id")
-- MAGIC display(cir_ren_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import current_timestamp,lit

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dt_df=cir_ren_df.withColumn("ingestiondate", current_timestamp())\
-- MAGIC .withColumn("env",lit("prod"))
-- MAGIC display(dt_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dt_df.write.parquet("/FileStore/circuits_par")

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /FileStore/circuits_par/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_par= spark.read.parquet("/FileStore/circuits_par/")
-- MAGIC display(df_par)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_fil=df_par.filter("circuit_id in (1,2) and alt in (10,18)")\
-- MAGIC     .withColumnRenamed("circuit_id","fil_circuit_id")
-- MAGIC display(df_fil)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_where=df_par.where("circuit_id in (1,2) and alt in (10,18)")\
-- MAGIC     .withColumnRenamed("circuit_id","where_circuit_id")
-- MAGIC display(df_where)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC inn_df=df_where.join(df_fil,df_fil.fil_circuit_id==df_where.where_circuit_id,"inner")\
-- MAGIC     .select(df_fil.fil_circuit_id,df_where.alt)
-- MAGIC display(inn_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import count,countDistinct,sum

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_par.select (sum("circuit_id"),count("*").alias("counttt")).show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_par\
-- MAGIC     .groupBy("alt")\
-- MAGIC     .agg(sum("lat"),countDistinct("env").alias("evvn")) \
-- MAGIC     .show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.window import Window

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_par.createTempView("v_temp_view")

-- COMMAND ----------

select count(*) from v_temp_view

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ckt = 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dis_par=spark.sql(f"select * from v_temp_view where circuit_id={ckt}")
-- MAGIC display(dis_par)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_par.createOrReplaceGlobalTempView("gv_race_temp")

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

select * from global_temp.gv_race_temp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql ("select * \
-- MAGIC     FROM global_temp.gv_race_temp ").show()

-- COMMAND ----------

CREATE OR REPLACE TABLE people10m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
)

-- COMMAND ----------

describe extended colors_json

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DeltaTable.createIfNotExists(spark) \
-- MAGIC   .tableName("default.people10m") \
-- MAGIC   .addColumn("id", "INT") \
-- MAGIC   .addColumn("firstName", "STRING") \
-- MAGIC   .addColumn("middleName", "STRING") \
-- MAGIC   .addColumn("lastName", "STRING", comment = "surname") \
-- MAGIC   .addColumn("gender", "STRING") \
-- MAGIC   .addColumn("ssn", "STRING") \
-- MAGIC   .addColumn("salary", "INT") \
-- MAGIC   .execute()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

describe extended default.people10m


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW people_updates (
  id, firstName, middleName, lastName, gender, ssn, salary
) AS VALUES
  (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', '953-38-9452', 55250),
  (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M',  '906-51-2137', 48500),
  (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '988-61-6247', 90000),
  (20000001, 'John', '', 'Doe', 'M', '345-67-8901', 55500),
  (20000002, 'Mary', '', 'Smith', 'F',  '456-78-9012', 98250),
  (20000003, 'Jane', '', 'Doe', 'F', '567-89-0123', 89900);

-- COMMAND ----------


drop table people10m 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable

-- COMMAND ----------

insert INTO default.people_10m   select * from people_updates 

-- COMMAND ----------



-- COMMAND ----------

create table xyz1(
  id INT,
  MYNAME STRING
) 

-- COMMAND ----------

insert into xyz1 values(5,'pry'),(6,'irp'),(7,'apr'),(8,'rpa')

-- COMMAND ----------

-- DBTITLE 1,se
select * from xyz1 

-- COMMAND ----------

delete from xyz where 1d=1

-- COMMAND ----------

desc history xyz1

-- COMMAND ----------

delete from  xyz1  where id =6

-- COMMAND ----------

vacuum xyz1 retain 0 hours

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

select current_schema()

-- COMMAND ----------

show tables

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



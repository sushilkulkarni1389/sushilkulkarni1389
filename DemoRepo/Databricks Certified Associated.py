# Databricks notebook source
# MAGIC %sql
# MAGIC create table employees
# MAGIC (id INT, name STRING, salary DOUBLE);

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employees (id,name,salary) values (1, 'Alice',2350.5),
# MAGIC (2, 'Bob',3422.5 ),
# MAGIC (3,'Charlie',2453.5),
# MAGIC (4, 'David',3566.9),
# MAGIC (5, 'Eve',3589.5),
# MAGIC (6, 'Frank',4476.0);

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe employees

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail employees;

# COMMAND ----------

# MAGIC %fs ls abfss://unity-catalog-storage@dbstorage4df5wkccpqxsi.dfs.core.windows.net/1995567721324587/__unitystorage/catalogs/a210cd7d-34cf-4d6d-a261-dc44614bc777/tables/592b8363-631d-44bb-875b-693ced5410a9

# COMMAND ----------

dbutils.fs.ls('abfss://unity-catalog-storage@dbstorage4df5wkccpqxsi.dfs.core.windows.net/1995567721324587/__unitystorage/catalogs/a210cd7d-34cf-4d6d-a261-dc44614bc777/tables/592b8363-631d-44bb-875b-693ced5410a9')

# COMMAND ----------



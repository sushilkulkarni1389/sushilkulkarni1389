# Databricks notebook source
# MAGIC %md
# MAGIC ##This notebook shows how to delete files and folders

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables"))

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/*json")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables"))

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/countries_multi_line.json")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables"))

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/countries_multi_line.json")
dbutils.fs.rm("dbfs:/FileStore/tables/countries_single_line.json")


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables"))

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/countriesout")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/countriesout", recursive=True)

# COMMAND ----------

dbutils.fs.rm.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/countriesout",recurse=True)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/countriesout_1",recurse=True)
dbutils.fs.rm("dbfs:/FileStore/tables/countriesout_region",recurse=True)
dbutils.fs.rm("dbfs:/FileStore/tables/countries.txt")
dbutils.fs.rm("dbfs:/FileStore/tables/parquet_files",recurse=True)
              

# COMMAND ----------

display(%fs ls)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/Tables

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/FileStore/tables"

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC #this notebook is from Spark SQL course

# COMMAND ----------

# MAGIC %md
# MAGIC #Read data from csv

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/tables/countries.csv')

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/tables/countries.csv', header=True)

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/tables/countries.csv', header=True, inferSchema=True).display()

# COMMAND ----------

df_countries = spark.read.csv('dbfs:/FileStore/tables/countries.csv')

# COMMAND ----------

type(df_countries)

# COMMAND ----------

df_countries.show()

# COMMAND ----------

display(df_countries)

# COMMAND ----------

df_countries.describe()

# COMMAND ----------

df_countries.schema

# COMMAND ----------

df_countries.count

# COMMAND ----------

df_countries.count()

# COMMAND ----------

df_countries.show()

# COMMAND ----------

df_countries.printSchema()

# COMMAND ----------

df_countries = spark.read.options(header='true', inferSchema='true').csv('/FileStore/tables/countries.csv')

# COMMAND ----------

from pyspark import IntegerType, StringType, DoubleType, StructField, StructType

# COMMAND ----------

df_countries_sl_json = spark.read.json('dbfs:/FileStore/tables/countries_single_line.json')

# COMMAND ----------

display(df_countries_sl_json)

# COMMAND ----------

df_countries_ml_json = spark.read.json('dbfs:/FileStore/tables/countries_multi_line.json')

# COMMAND ----------

display(df_countries_ml_json)

# COMMAND ----------

df_countries_ml_json = spark.read.json('dbfs:/FileStore/tables/countries_multi_line.json', multiLine=True)

# COMMAND ----------

df_countries_ml_json = spark.read.option("ignoreCorruptFiles", "true").json("path_to_your_json_file")
display(df_countries_ml_json)

# COMMAND ----------

df_countries_ml_json = spark.read.option("ignoreCorruptFiles", "true").json("dbfs:/FileStore/tables/countries_multi_line.json")
display(df_countries_ml_json)

# COMMAND ----------

df_countries_ml_json =spark.read.csv("dbfs:/FileStore/tables/countries_multi_line.csv", multiLine=True)

# COMMAND ----------

df_countries_ml_json = spark.read.json('dbfs:/FileStore/tables/countries_multi_line.json', multiLine=True)

# COMMAND ----------

display(df_countries_ml_json)

# COMMAND ----------

df_countries_text = spark.read.csv("dbfs:/FileStore/tables/countries.txt")

# COMMAND ----------

display(df_countries_text)

# COMMAND ----------

df_countries_text = spark.read.options(header=True,sep="\t").csv("dbfs:/FileStore/tables/countries.txt")

# COMMAND ----------

display(df_countries_text)

# COMMAND ----------

display(df_countries_text.select("COUNTRY_ID").distinct)

# COMMAND ----------

display(df_countries_text.select("COUNTRY_ID").distinct().count)

# COMMAND ----------

df_countries_text.select("COUNTRY_ID").distinct().count

# COMMAND ----------

display(df_countries_text.distinct().select("REGION_ID"))

# COMMAND ----------

display(df_countries_text.select("REGION_ID").count)

# COMMAND ----------

display(df_countries)

# COMMAND ----------

# MAGIC %md
# MAGIC #Write dataframe to csv

# COMMAND ----------

df_countries.write.csv("dbfs:/FileStore/tables/countriesout",header=True)

# COMMAND ----------

spark.read.csv("dbfs:/FileStore/tables/countriesout/part-00000-tid-4683216683984288584-de0001f0-5d61-40bd-a667-f0bcfd76a35c-31-1-c000.csv")

# COMMAND ----------

display(spark.read.csv("dbfs:/FileStore/tables/countriesout/part-00000-tid-4683216683984288584-de0001f0-5d61-40bd-a667-f0bcfd76a35c-31-1-c000.csv",header=True))

# COMMAND ----------

df_countries.write.options(header="true", mode="overwrite").csv("dbfs:/FileStore/tables/countriesout_1")

# COMMAND ----------

df_countries.write.options(header="true", mode="overwrite").partitionBy("REGION_ID").csv("dbfs:/FileStore/tables/countriesout_region")

# COMMAND ----------

spark.read.csv("dbfs:/FileStore/tables/countriesout_region")

# COMMAND ----------

display(spark.read.csv("dbfs:/FileStore/tables/countriesout_region"))

# COMMAND ----------

display(spark.read.csv("dbfs:/FileStore/tables/countriesout_region/REGION_ID=30"))

# COMMAND ----------

display(df_countries)

# COMMAND ----------

df_countries.write.parquet("dbfs:/FileStore/tables/parquet_files")

# COMMAND ----------

display(spark.read.parquet("dbfs:/FileStore/tables/parquet_files"))

# COMMAND ----------



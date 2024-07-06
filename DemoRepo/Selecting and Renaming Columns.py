# Databricks notebook source
# MAGIC %md
# MAGIC ## This notebooks shows how to select columns from a dataframe and rename them

# COMMAND ----------

df_countries = spark.read.options(header=True,inferSchema=True).csv("dbfs:/FileStore/tables/countries.csv")

# COMMAND ----------

display(df_countries)

# COMMAND ----------

df_countries.select('name','capital','population')

# COMMAND ----------

display(df_countries.select('name','capital','population'))

# COMMAND ----------

display(df_countries.select(df_countries['name']))

# COMMAND ----------

display(df_countries.select(df_countries['name'],df_countries['Capital'],df_countries['POPULATION']))

# COMMAND ----------

display(df_countries.select(df_countries.NAME,df_countries.CAPITAL,df_countries.POPULATION))

# COMMAND ----------

from pyspark.sql.functions import col

df_countries.select(col('name'),col('Capital'),col('POPULATION')).display()

# COMMAND ----------

regions_path ='dbfs:/FileStore/tables/country_regions.csv'

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

regions_schema = StructType([
    StructField('ID',StringType(),False),
    StructField('Name',StringType(),False),
])

# COMMAND ----------

df_countries.schema

# COMMAND ----------

regions = spark.read.csv(path = regions_path, schema = regions_schema, header =True)

# COMMAND ----------

display(regions)

# COMMAND ----------

regions.select('id','name')

# COMMAND ----------

display(regions.select('id','name'))

# COMMAND ----------

display(regions.select(regions['id'].alias('region_id'),regions['name'].alias('continent_name')))

# COMMAND ----------

from pyspark.sql.withColumns import withRenamedColumns

# COMMAND ----------

regions.select('id','name').withRenamedColumns(['id','name'],'region_id','continent_name')

# COMMAND ----------

display(regions.select('id','name'))

# COMMAND ----------

display(regions.select('id','name').withColumnRenamed('id','region_id'))

# COMMAND ----------



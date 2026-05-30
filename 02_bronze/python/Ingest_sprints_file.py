# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest results.json file
# MAGIC 1. Read all files from folder using dataframe reader API
# MAGIC 2. Define schema
# MAGIC 3. Add metadata columns: source file, ingestion timestamp
# MAGIC 4. Write to bronze delta table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

# MAGIC %run ../Workspace/common/bronze_helpers

# COMMAND ----------

# Define source file and table name
source_file = f"{landing_folder_path}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

source_file

# COMMAND ----------

table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read JSON file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType, DataType

sprints_schema = StructType([
  StructField("constructorId", StringType()),
#  StructField("date", DataType()), data type trhows errors
  StructField("date", StringType()), 
  StructField("driverId", StringType()),
  StructField("grid", IntegerType()),
  StructField("laps", IntegerType()),
  StructField("number", IntegerType()),
  StructField("points", FloatType()),
  StructField("position", IntegerType()),
  StructField("positionText", StringType()),
  StructField("raceName", StringType()),
  StructField("round", IntegerType()),
  StructField("season", IntegerType()),
  StructField("status", StringType()),
  StructField("url", StringType())
])

# COMMAND ----------

sprints_df = (
    spark.read
        .format('json')
        .schema(sprints_schema)
        .option('mode', 'FAILFAST')
        .option('multiLine', True)
        .load(source_file)
)

# COMMAND ----------

# Check what files exist in the sprints folder
files = dbutils.fs.ls("/Volumes/formula1/landing/files/sprints")
for f in files:
    print(f.name, f.size)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Add metadata columns

# COMMAND ----------

sprints_final = add_ingestion_metadata(sprints_df)
display(sprints_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Write to bronze delta table

# COMMAND ----------

(
    sprints_final
        .write
        .mode('overwrite')
        .format('delta')
#        .saveAsTable('table_name') # doesn't work with overwrite mode
        .saveAsTable('formula1.bronze.sprints')
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT season, COUNT(*) FROM formula1.bronze.sprints GROUP BY season ORDER BY season;
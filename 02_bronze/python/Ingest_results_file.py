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
source_file = f"{landing_folder_path}/results"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

source_file

# COMMAND ----------

table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read JSON file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType, DataType
results_schema = StructType([
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

results_df = (
    spark.read
        .format('json')
        .schema(results_schema)
        .option('mode', 'FAILFAST')
#        .option('inferSchema', True)
        .load(source_file)
)

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Add metadata columns

# COMMAND ----------

results_final = add_ingestion_metadata(results_df)
display(results_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Write to bronze delta table

# COMMAND ----------

(
    results_final
        .write
        .mode('overwrite')
        .format('delta')
#        .saveAsTable('table_name') # doesn't work with overwrite mode
        .saveAsTable('formula1.bronze.results')
)

# COMMAND ----------

display(spark.table(table_name))
# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file
# MAGIC 1. Read the file using dataframe reader API
# MAGIC 2. Add metadata columns: source file, ingestion timestamp
# MAGIC 3. Write to bronze delta table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

# MAGIC %run ../Workspace/common/bronze_helpers

# COMMAND ----------

# Define source file and table name
source_file = f"{landing_folder_path}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read JSON file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DataType

name_schema = StructType([
    StructField("givenName", StringType()),
    StructField("familyName", StringType())
])

drivers_schema = StructType(fields=[ 
    StructField("_corrupt_record", StringType()),
    StructField("constructorId", StringType()),
#    StructField("dateOfBirth", DataType()), # It throws errors here
    StructField("dateOfBirth", StringType()),
    StructField("driverId", StringType()),
    StructField("name", name_schema),
    StructField("nationality", StringType()),
    StructField("url", StringType()),
])

# COMMAND ----------

drivers_df = (
    spark.read
        .format('json')
        .schema(drivers_schema)
        .option('mode', 'FAILFAST')
#        .option('inferSchema', True)
        .load(source_file)
)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Add metadata columns: source file, ingestion timestamp

# COMMAND ----------

drivers_final = add_ingestion_metadata(drivers_df)

# COMMAND ----------

display(drivers_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Write to bronze delta table

# COMMAND ----------

(
    drivers_final
        .write
        .mode('overwrite')
        .format('delta')
#        .saveAsTable('table_name') # doesn't work with overwrite mode
        .saveAsTable('formula1.bronze.drivers')
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT season, COUNT(*) FROM formula1.bronze.results GROUP BY season ORDER BY season;
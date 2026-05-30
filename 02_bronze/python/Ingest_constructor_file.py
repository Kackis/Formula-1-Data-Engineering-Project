# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructor.json file
# MAGIC 1. Read the file using dataframe reader API
# MAGIC 2. Add metadata columns: source file, ingestion timestamp
# MAGIC 3. Write to bronze delta table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

# MAGIC %run ../Workspace/common/bronze_helpers

# COMMAND ----------

# Define source file and table name
source_file = f"{landing_folder_path}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read JSON file using dataframe reader API

# COMMAND ----------

# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DataType

# constructor_schema = StructType(fields=[ 
#    StructField("_corrupt_record", StringType()),
#    StructField("constructorId", StringType()),
#    StructField("dateOfBirth", DataType()),
#    StructField("driverId", StringType()),
#    StructField("name", StringType()),
#    StructField("nationality", StringType()),
#    StructField("url", StringType()),
#])

# COMMAND ----------

constructor_schema = """ _corrupt_record STRING, constructorId STRING, dateOfBirth STRING, driverId STRING, name STRING, nationality STRING, url STRING"""

# COMMAND ----------

constructor_df = (
    spark.read
        .format('json')
        .option('header', True)
#        .option('inferSchema', True)
        .schema(constructor_schema)
        .load(source_file)
)

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Add metadata columns: source file, ingestion timestamp

# COMMAND ----------

constructor_final = add_ingestion_metadata(constructor_df)

# COMMAND ----------

display(constructor_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Write to bronze delta table

# COMMAND ----------

(
    constructor_final
        .write
        .mode('overwrite')
        .format('delta')
#        .saveAsTable('table_name') // doesn't work with overwrite mode
        .saveAsTable('formula1.bronze.constructors')
)

# COMMAND ----------

display(spark.table(table_name))
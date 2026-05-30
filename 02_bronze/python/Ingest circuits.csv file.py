# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuits.csv file
# MAGIC 1. Read the file using dataframe reader API
# MAGIC 2. Add metadata columns: source file, ingestion timestamp
# MAGIC 3. Write to bronze delta table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

# MAGIC %run ../Workspace/common/bronze_helpers

# COMMAND ----------

catalog_name

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuit_schema = StructType(fields=[
    StructField("circuitId",        StringType(), True),
    StructField("url",              StringType(), True),
    StructField("circuitName",      StringType(), True),
    StructField("lat",              DoubleType(), True),
    StructField("long",             DoubleType(), True),
    StructField("locality",         StringType(), True),
    StructField("country",          StringType(), True),
])

# COMMAND ----------

circuit_df = (
    spark.read
        .format('csv')
        .option('header', True)
#        .option('inferSchema', True)
        .schema(circuit_schema)
        .load('/Volumes/formula1/landing/files/circuits.csv')
)


# COMMAND ----------

circuit_df.show()

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Add metadata columns: source file, ingestion timestamp

# COMMAND ----------

from pyspark.sql import functions as F
circuit_final = (
    circuit_df
        .withColumn('ingestion_timestamp', F.current_timestamp())
        .withColumn('source_file', F.col('_metadata.file_path'))
)

# COMMAND ----------

# using helper function
circuit_final = add_ingestion_metadata(circuit_df)

# COMMAND ----------

display(circuit_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Write to bronze delta table

# COMMAND ----------

(
    circuit_final
        .write
        .mode('overwrite')
        .format('delta')
        .saveAsTable('formula1.bronze.circuit')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.bronze.circuit
# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest races.csv file
# MAGIC 1. Read the file using dataframe reader API
# MAGIC 2. Add metadata columns: source file, ingestion timestamp
# MAGIC 3. Write to bronze delta table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

circuit_schema = StructType(fields=[
    StructField("season",        StringType()),
    StructField("round",         IntegerType()),
    StructField("url",           StringType()),
    StructField("raceName",      StringType()),
    StructField("date",          DateType()),
    StructField("circuitId",     StringType()),
])

# COMMAND ----------

races_df = (
    spark.read
        .format('csv')
        .option('header', True)
#        .option('inferSchema', True)
        .schema(circuit_schema)
        .load('/Volumes/formula1/landing/files/races.csv')
)


# COMMAND ----------

races_df.show()

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Add metadata columns: source file, ingestion timestamp

# COMMAND ----------

from pyspark.sql import functions as F
races_final = (
    races_df
        .withColumn('ingestion_timestamp', F.current_timestamp())
        .withColumn('source_file', F.col('_metadata.file_path'))
)

# COMMAND ----------

display(races_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Write to bronze delta table

# COMMAND ----------

(
    races_final
        .write
        .mode('overwrite')
        .format('delta')
        .saveAsTable('formula1.bronze.races')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.bronze.races
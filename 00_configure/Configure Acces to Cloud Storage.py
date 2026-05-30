# Databricks notebook source
# MAGIC %md
# MAGIC #### Configure Acces to Cloud Storage via Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC   
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `databricks_kackis`
# MAGIC URL 'abfss://demo@kackistorage.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `databricks-credential`)
# MAGIC COMMENT 'External location for demo container';

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'abfss://demo@kackistorage.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %sql
# MAGIC   
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `databricks_kackis_formula1`
# MAGIC URL 'abfss://formula1@kackistorage.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `databricks-credential`)
# MAGIC COMMENT 'External location for formula1 container';

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'abfss://formula1@kackistorage.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS formula1
# MAGIC   MANAGED LOCATION 'abfss://formula1@kackistorage.dfs.core.windows.net/'
# MAGIC   COMMENT 'Main catalog for formula1 project';   

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.landing;
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.bronze
# MAGIC   MANAGED LOCATION 'abfss://formula1@kackistorage.dfs.core.windows.net/bronze';
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.silver
# MAGIC   MANAGED LOCATION 'abfss://formula1@kackistorage.dfs.core.windows.net/silver';
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.gold
# MAGIC   MANAGED LOCATION 'abfss://formula1@kackistorage.dfs.core.windows.net/gold';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG formula1;  
# MAGIC SHOW SCHEMAS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create volume files

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME formula1.landing.files
# MAGIC LOCATION 'abfss://formula1@kackistorage.dfs.core.windows.net/landing';

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/formula1/landing/files
# Databricks notebook source
# MAGIC %md 
# MAGIC ### Constructor standings
# MAGIC ##### Sources
# MAGIC 1. fact_results
# MAGIC 2. dim_constructors
# MAGIC
# MAGIC ##### Output 
# MAGIC 1. season
# MAGIC 2. constructor_id
# MAGIC 3. constructor_name
# MAGIC 4. nationality
# MAGIC 5. race starts
# MAGIC 6. total points
# MAGIC 7. number of wins
# MAGIC 8. number of podiums
# MAGIC 9. standing position

# COMMAND ----------

from pyspark.sql import functions as F


# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

fact_results_df = spark.table(f"{catalog_name}.{gold_schema}.fact_results")
dim_constructors_df = spark.table(f"{catalog_name}.{gold_schema}.dim_constructors")

# COMMAND ----------

display(fact_results_df)
display(dim_constructors_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW formula1.gold.v_constructor_standings
# MAGIC AS
# MAGIC WITH constructor_summary
# MAGIC AS
# MAGIC     (SELECT RESULTS.season,
# MAGIC                 CONSTRUCTORS.constructor_id,
# MAGIC                 CONSTRUCTORS.constructor_name,
# MAGIC                 CONSTRUCTORS.nationality,
# MAGIC                 COUNT(*) AS race_starts,
# MAGIC                 SUM(RESULTS.points) AS total_points,
# MAGIC                 COUNT_IF(RESULTS.is_win) AS number_of_wins,
# MAGIC                 COUNT_IF(RESULTS.is_podium) AS number_of_podiums
# MAGIC         FROM formula1.gold.fact_results RESULTS
# MAGIC         JOIN formula1.gold.dim_constructors CONSTRUCTORS
# MAGIC             ON RESULTS.constructor_id = CONSTRUCTORS.constructor_id
# MAGIC     GROUP BY RESULTS.season,
# MAGIC          CONSTRUCTORS.constructor_id,
# MAGIC          CONSTRUCTORS.constructor_name,
# MAGIC          CONSTRUCTORS.nationality)
# MAGIC SELECT season,
# MAGIC         constructor_id,
# MAGIC         constructor_name,
# MAGIC         nationality,
# MAGIC         RANK () OVER (PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing,
# MAGIC         race_starts,
# MAGIC         total_points,
# MAGIC         number_of_wins,
# MAGIC         number_of_podiums
# MAGIC     FROM constructor_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.gold.v_constructor_standings WHERE season = 2023
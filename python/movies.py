# Databricks notebook source
# MAGIC %md
# MAGIC ### Create movie silver table

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC  %run "./includes/utilities"

# COMMAND ----------

dbutils.fs.rm(bronze_folder_path, recurse=True)

# COMMAND ----------

dbutils.fs.rm(silver_folder_path, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 1 ingest raw data

# COMMAND ----------

raw_df = read_batch_raw(raw_folder_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 2 ingest metadata (transform the raw data)

# COMMAND ----------

transformed_raw_df = transform_raw(raw_df)

# COMMAND ----------

display(transformed_raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 3 write to a bronze table

# COMMAND ----------

raw_to_bronze = batch_writer( dataframe=transformed_raw_df )

raw_to_bronze.save(f"{bronze_folder_path}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### display bronze table

# COMMAND ----------

spark.sql("""
drop table if exists movies_bronze
""")

spark.sql(f"""
create table movies_bronze
using delta 
location "{bronze_folder_path}/"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from movies_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 load new records in needed columns from bronze table

# COMMAND ----------

display(dbutils.fs.ls(bronze_folder_path))

# COMMAND ----------


silver_movies = spark.read.load(f"{bronze_folder_path}/").filter("status = 'new' ")
#silver_df = spark.read.table(f"{bronze_folder_path}/").filter("status = 'new' ")

# COMMAND ----------

silver_movies = silver_movies.select("Movies.Id", "Movies.Title", "Movies.Overview", "Movies.Budget", "Movies.RunTime")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 5 transform data (rename, cast datatype)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import DataFrame

silver_movies = silver_movies.select(
    col("Id").cast("integer").alias("movie_id"),
    col("Title").alias("title"),
    col("Overview").alias("overview"),
    col("Budget").alias("budget"),
    col("RunTime").cast("integer").alias("runtime")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### we assume all the movies should have a minimum budget of 1 million, so if a movie has a budget of less than 1 million, we should replace it with 1 million

# COMMAND ----------

display(silver_movies)

# COMMAND ----------

silver_movies.createOrReplaceTempView("process")

silver_movies = spark.sql("update process set budget = 1000000 where budget<1000000")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### verify schema with an Assertion

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

assert silver_df.schema == _parse_datatype_string(
    """
  movie_id INTEGER,
  title STRING,
  overview STRING,
  budget INTEGER,
  runtime INTEGER
"""
), "Schemas do not match"
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Quarantine the Bad Data

# COMMAND ----------

silver_df.count()

# COMMAND ----------

silver_df.filter("runtime < 0").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Display the Quarantined Records

# COMMAND ----------

silver_movies_clean = silver_movies.filter("runtime >= 0")
silver_movies_quarantine = silver_movies.filter("runtime < 0")

# COMMAND ----------

display(silver_movies_quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 6 write clean batch to silver table 

# COMMAND ----------

silver_movies_clean.write.format("delta").mode("append").save(f"{silver_folder_path}/movies")

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movies_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movies_silver
USING DELTA
LOCATION f"{silver_folder_path}/movies"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movies_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 7 update bronze table to reflect the loads

# COMMAND ----------

# MAGIC %md
# MAGIC update claean records (update the status to "loaded")

# COMMAND ----------

from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, f"{bronze_folder_path}/bronze")
silverAugmented = (
    silver_movies_clean.withColumn("status", lit("loaded"))
)

update_match = "bronze.value = clean.value"
update = {"status": "clean.status"}

(
  bronzeTable.alias("bronze")
  .merge(silverAugmented.alias("clean"), update_match)
  .whenMatchedUpdate(set=update)
  .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC update quarantined records

# COMMAND ----------

silverAugmented = (
  silver_moives_quarantine
  .withColumn("status", lit("quarantined"))
)

update_match = "bronze.value = quarantine.value"
update = {"status" : "quarantine.status"}

(
  bronzeTable.alias("bronze")
  .merge(silverAugmented.alias("quarantine"), update_match)
  .whenMatchedUpdate(set=update)
  .execute()
)


# COMMAND ----------



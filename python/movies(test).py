# Databricks notebook source
# MAGIC %md 
# MAGIC ### Create movies's silver table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 1 ingest and create and write bronze table

# COMMAND ----------

# MAGIC %run ./raw_to_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 2  load records from the bronze records and cleansing

# COMMAND ----------

dbutils.fs.rm(silver_folder_path, recurse=True)

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

silver_df = bronze_df.filter("status = 'new' ").select("Id", "Title", "Overview", "Tagline", "OriginalLanguage", "ReleaseDate", "RunTime")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 3 transform - rename

# COMMAND ----------

silver_df = silver_df \
.withColumnRenamed("Id","movie_id") \
.withColumnRenamed("Title","title") \
.withColumnRenamed("Overview","overview") \
.withColumnRenamed("Tagline","tagline") \
.withColumnRenamed("OriginalLanguage","original_language") \
.withColumnRenamed("ReleaseDate","release_date") \
.withColumnRenamed("RunTime","runTime")


# COMMAND ----------

display(silver_df)

# COMMAND ----------



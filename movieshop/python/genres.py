# Databricks notebook source
# MAGIC %md
# MAGIC ### create genres silver table

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC  %run "./includes/utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC #### raw to bronze

# COMMAND ----------

raw_df = read_batch_raw(raw_folder_path)

transformed_raw_df = transform_raw(raw_df)

raw_to_bronze = batch_writer( dataframe=transformed_raw_df )
raw_to_bronze.save(f"{bronze_folder_path}/")


# COMMAND ----------

spark.sql("""
drop table if exists genres_bronze
""")

spark.sql(f"""
create table genres_bronze
using delta 
location "{bronze_folder_path}/"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from genres_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC #### bronze to silver

# COMMAND ----------

silver_genres = spark.read.table("OriginalLanguages_bronze").filter("status = 'new' ")

# COMMAND ----------

silver_genres = silver_genres.select("Movies.genres.id","Movies.genres.name","Movies")

# COMMAND ----------

silver_genres = silver_genres.select(
    col("id").cast("integer")，
    "name",
    col("Movies")
)

# COMMAND ----------

silver_genres.count()

# COMMAND ----------

silver_genres = silver_genres.drop_duplicates().dropna()

# COMMAND ----------

silver_genres.count()

# COMMAND ----------



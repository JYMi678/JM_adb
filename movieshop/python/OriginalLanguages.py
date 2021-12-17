# Databricks notebook source
# MAGIC %md
# MAGIC ### Create OriginalLanguages silver table

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC  %run "./includes/utilities"

# COMMAND ----------

#dbutils.fs.rm(bronze_folder_path, recurse=True)
#dbutils.fs.rm(silver_folder_path, recurse=True)

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
drop table if exists OriginalLanguages_bronze
""")

spark.sql(f"""
create table OriginalLanguages_bronze
using delta 
location "{bronze_folder_path}/"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from OriginalLanguages_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC #### bronze to silver

# COMMAND ----------

silver_originallanguages = spark.read.table("OriginalLanguages_bronze").filter("status = 'new' ")

# COMMAND ----------

silver_originallanguages = silver_originallanguages.select("Movies.Id","Movies.Title", "Movies.OriginalLanguages","Movies")

# COMMAND ----------

silver_originallanguages = silver_originallanguages.select(
    col("Id").cast("integer").alias("movie_id"),
    col("Title").alias("title"),
    col("OriginalLanguages").alias("original_languages"),
    col("Movies")
)

# COMMAND ----------

silver_originallanguages.count()

# COMMAND ----------

silver_originallanguages = silver_originallanguages.drop_duplicates()

# COMMAND ----------

silver_originallanguages.count()

# COMMAND ----------

silver_originallanguages.select("movie_id","title","original_languages").write.format("delta").mode("append").save(f"{silver_folder_path}/originallanguages")

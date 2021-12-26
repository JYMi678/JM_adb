# Databricks notebook source
# MAGIC %md
# MAGIC ### create genres silver table

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC  %run "./includes/common_functions"

# COMMAND ----------

dbutils.fs.rm(f"{bronze_folder_path}/genres", recurse=True)
dbutils.fs.rm(f"{silver_folder_path}/genres", recurse=True)

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

silver_genres = spark.read.table("genres_bronze").filter("status = 'new' ")
silver_genres = silver_genres.select(explode("Movies.genres").alias("genres"),"Movies")
silver_genres = silver_genres.select("genres.id","genres.name","movies")

# COMMAND ----------

display(silver_genres)

# COMMAND ----------

silver_genres = silver_genres.select(
        col("id").cast("integer").alias("genre_id"),
        col("name").alias("genre_name"),
        col("Movies")
    )

# COMMAND ----------

silver_genres.count()

# COMMAND ----------

silver_genres = silver_genres.dropDuplicates()

# COMMAND ----------

silver_genres.count()

# COMMAND ----------

silver_genres.na.drop().count() # TO SEE IF NEED after dropping all, id & name shouldn't be null

# COMMAND ----------

silver_genres_clean = silver_genres # assume no quarantine if all there is no null

# COMMAND ----------

bronzeToSilverWriter = batch_writer(dataframe=silver_genres_clean, exclude_columns=["Movies"])
bronzeToSilverWriter.save(f"{silver_folder_path}/genres")

# COMMAND ----------

# or maybe I can choose to update delta (why not work?)
delta_genres = read_batch_delta(f"{silver_folder_path}/genres")
delta_genres = delta_genres.dropDuplicates().na.drop()
display(delta_genres)


# COMMAND ----------

#
bronzeToSilverWriter = batch_writer(dataframe=delta_genres)
bronzeToSilverWriter.save(f"{silver_folder_path}/genres")

# COMMAND ----------

spark.sql("""
drop table if exists delta_genres
""")

spark.sql(f"""
create table delta_genres
using delta 
location "{silver_folder_path}/genres"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from delta_genres order by genre_id

# COMMAND ----------

# MAGIC %md
# MAGIC #no need
# MAGIC from delta.tables import DeltaTable
# MAGIC 
# MAGIC bronzeTable = DeltaTable.forPath(spark, f"{bronze_folder_path}/genres")
# MAGIC silverAugmented = (
# MAGIC     silver_genres_clean
# MAGIC     .withColumn("status", lit("loaded"))
# MAGIC )
# MAGIC 
# MAGIC update_match = "g_bronze.Movies = clean.Movies"
# MAGIC update = {"status": "clean.status"}
# MAGIC 
# MAGIC (
# MAGIC   bronzeTable.alias("g_bronze")
# MAGIC   .merge(silverAugmented.alias("clean"), update_match)
# MAGIC   .whenMatchedUpdate(set=update)
# MAGIC   .execute()
# MAGIC )

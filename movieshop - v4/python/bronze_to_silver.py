# Databricks notebook source
# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC  %run "./includes/common_functions"

# COMMAND ----------

dbutils.fs.rm(f"{bronze_folder_path}/movies", recurse=True)
dbutils.fs.rm(f"{bronze_folder_path}/genres", recurse=True)
dbutils.fs.rm(f"{bronze_folder_path}/movies_genres", recurse=True)
dbutils.fs.rm(f"{bronze_folder_path}/originallanguages", recurse=True)

dbutils.fs.rm(f"{silver_folder_path}/movies", recurse=True)
dbutils.fs.rm(f"{silver_folder_path}/genres", recurse=True)
dbutils.fs.rm(f"{silver_folder_path}/movies_genres", recurse=True)
dbutils.fs.rm(f"{silver_folder_path}/originallanguages", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Raw to Bronze Pipeline

# COMMAND ----------

# ingest raw data
raw_df = read_batch_raw(raw_folder_path)
# ingest metadata
transformed_raw_df = transform_raw(raw_df)
# write to movies bronze table
raw_to_bronze = batch_writer( dataframe=transformed_raw_df )
raw_to_bronze.save(f"{bronze_folder_path}/movies")

# COMMAND ----------

spark.sql("""
drop table if exists movies_bronze
""")

spark.sql(f"""
create table movies_bronze
using delta 
location "{bronze_folder_path}/movies"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from movies_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Bronze to Silver Pipeline (movies, genres, movies_genres, originallanguages)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### get movies silver table

# COMMAND ----------

# remember to write after all steps

bronze_movies = read_movies_bronze()
silver_movies = transform_movies_bronze(bronze_movies)

(silver_movies_clean, silver_movies_quarantine) = generate_clean_and_quarantine_dataframes(silver_movies)

# COMMAND ----------

# check genres part

# COMMAND ----------

# MAGIC %md
# MAGIC ##### get genres silver table

# COMMAND ----------

# get genres silver table. 
# here I use movies_clean, is that right?
silver_genres = silver_movies_clean.select(explode("Movies.genres").alias("genres"), "Movies")
silver_genres = silver_genres.select("genres.Genres_id","genres.Genres_name","Movies") 
#no need to rename or change type cause I have done when transform to silver_movies


# COMMAND ----------

silver_genres = silver_genres.dropDuplicates().na.drop()

# COMMAND ----------

# check duplication situation

# COMMAND ----------

# MAGIC %md
# MAGIC ##### get movies_genres silver table

# COMMAND ----------

movies_df = silver_movies_clean.select("Movie_id", "Movies")
silver_movies_genres = movies_df.join(silver_genres, silver_genres.Movies == movies_df.Movies).select(movies_df.Movie_id, silver_genres.Genre_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### get originallanguages silver table

# COMMAND ----------

silver_originallanguages = silver_movies_clean.select("Movies.Movies_id","Movies.Title", "Movies.OriginalLanguage","Movies")

# COMMAND ----------

silver_originallanguages = silver_originallanguages.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### write to silver table

# COMMAND ----------

# write clean movies data to silver table 
moviesToSilverWriter = batch_writer(dataframe=silver_movies_clean, exclude_columns=["Movies"])
moviesToSilverWriter.save(f"{silver_folder_path}/")

# write genres to silver table
genresToSilverWriter = batch_writer(dataframe=silver_genres, exclude_columns=["Movies"])
genresToSilverWriter.save(f"{silver_folder_path}/")

# write movies_genres to silver table
movies_genresToSilverWriter = batch_writer(dataframe=silver_movies_genres, exclude_columns=["Movies"])
movies_genresToSilverWriter.save(f"{silver_folder_path}/")

## write originallanguages to silver table
originallanguagesToSilverWriter = batch_writer(dataframe=silver_originallanguages, exclude_columns=["Movies"])
originallanguagesToSilverWriter.save(f"{silver_folder_path}/")

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
LOCATION "{silver_folder_path}/"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movies_silver order by movie_id

# COMMAND ----------

spark.sql("""
drop table if exists genres_silver
""")

spark.sql(f"""
create table genres_silver
using delta 
location "{silver_folder_path}/"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from genres_silver order by genre_id

# COMMAND ----------

spark.sql("""
drop table if exists movies_genres_silver 
""")

spark.sql(f"""
create table movies_genres_silver 
using delta 
location "{silver_folder_path}/"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movies_genres_silver 

# COMMAND ----------

spark.sql("""
drop table if exists originallanguages_silver 
""")

spark.sql(f"""
create table originallanguages_silver 
using delta 
location "{silver_folder_path}/"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from originallanguages_silver 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### update status in bronze table

# COMMAND ----------

# not sure 
update_bronze_movies_status(spark, f"{bronze_folder_path}/", silver_movies_clean, "loaded")
update_bronze_movies_status(spark, f"{bronze_folder_path}/", silver_movies_quarantine, "quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handle Quarantined Records

# COMMAND ----------

# Load Quarantined Records from the Bronze Table
bronze_Quarantined_DF = spark.read.table("movies_bronze").filter("status = 'quarantined'")

# COMMAND ----------

# Transform the Quarantined Records
bronzeQuarTransDF = transform_bronze(bronze_Quarantined_DF, quarantine=True).alias("quarantine")

# display(bronzeQuarTransDF)

# COMMAND ----------

# Write the Repaired (formerly Quarantined) Records to the movies Silver Table
bronzeToSilverWriter = batch_writer(dataframe=bronzeQuarTransDF, exclude_columns=["Movies"])
bronzeToSilverWriter.save(f"{silver_folder_path}/")

update_bronze_table_status(spark, f"{bronze_folder_path}/", bronzeQuarTransDF, "loaded")

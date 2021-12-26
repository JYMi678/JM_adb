# Databricks notebook source
# MAGIC %md
# MAGIC ### Create movie silver table

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC  %run "./includes/common_functions"

# COMMAND ----------

dbutils.fs.rm(f"{bronze_folder_path}/movies", recurse=True)

# COMMAND ----------

dbutils.fs.rm(f"{silver_folder_path}/movies", recurse=True)

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

raw_to_bronze.save(f"{bronze_folder_path}/movies")

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
location "{bronze_folder_path}/movies"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from movies_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze to silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 load new records in needed columns from bronze table

# COMMAND ----------

#silver_movies = spark.read.load(f"{bronze_folder_path}/").filter("status = 'new' ")
#silver_movies = spark.read.table("movies_bronze").filter("status = 'new' ")

bronze_moviesdf = read_movies_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.functions import from_json
# MAGIC from pyspark.sql import dataframe 
# MAGIC 
# MAGIC json_schema = """
# MAGIC     BackdropUrl string,
# MAGIC     Budget double,
# MAGIC     CreatedBy string,
# MAGIC     CreatedDate string,
# MAGIC     Id long,
# MAGIC     ImdbUrl string,
# MAGIC     OriginalLanguage string,
# MAGIC     Overview string,
# MAGIC     PosterUrl string,
# MAGIC     Price double,
# MAGIC     ReleaseDate string,
# MAGIC     Revenue double,
# MAGIC     RunTime long,
# MAGIC     Tagline string,
# MAGIC     Title string,
# MAGIC     TmdbUrl string,
# MAGIC     UpdatedBy string,
# MAGIC     UpdatedDate string,
# MAGIC     genres.id long,
# MAGIC     genres.name string
# MAGIC 
# MAGIC 
# MAGIC """
# MAGIC silver_movies.withColumn( "nested_json", from_json(col("Movies"),json_schema) )

# COMMAND ----------

#silver_movies = silver_movies.select("Movies.Id","Movies.Title", "Movies.Overview", "Movies.Budget", "Movies.RunTime","Movies")

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.types import _parse_datatype_string
# MAGIC 
# MAGIC assert silver_movies.schema == _parse_datatype_string(
# MAGIC     """
# MAGIC   Id LONG,
# MAGIC   Title STRING,
# MAGIC   Overview STRING,
# MAGIC   Budget DOUBLE,
# MAGIC   RunTime LONG,
# MAGIC   Movies.BackdropUrl STRING,
# MAGIC   Movies.Budget DOUBLE,
# MAGIC   Movies.CreatedBy STRING,
# MAGIC   Movies.CreatedDate STRING,
# MAGIC   Movies.Id LONG,
# MAGIC   Movies.ImdbUrl STRING,
# MAGIC   Movies.OriginalLanguage STRING,
# MAGIC   Movies.Overview STRING,
# MAGIC   Movies.PosterUrl STRING
# MAGIC   Movies.Price DOUBLE
# MAGIC   Movies.ReleaseDate STRING
# MAGIC   Movies.Revenue DOUBLE
# MAGIC   Movies.RunTime LONG
# MAGIC   Movies.Tagline STRING
# MAGIC   Movies.Title STRING
# MAGIC   Movies.TmdbUrl STRING
# MAGIC   Movies.UpdatedBy STRING
# MAGIC   Movies.UpdatedDate STRING
# MAGIC   Movies.genres.id LONG
# MAGIC   Movies.genres.name STRING
# MAGIC  
# MAGIC """
# MAGIC ), "Schemas do not match"
# MAGIC print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 5 transform data (rename, cast datatype)

# COMMAND ----------

bronze_moviesdf.count()

# COMMAND ----------

silver_movies = transform_movies_bronze(bronze_moviesdf)

# COMMAND ----------

silver_movies.count()

# COMMAND ----------

# MAGIC %md
# MAGIC silver_movies = silver_movies.select(
# MAGIC     col("Id").cast("integer").alias("movie_id"),
# MAGIC     col("Title").alias("title"),
# MAGIC     col("Overview").alias("overview"),
# MAGIC     col("Budget").alias("budget"),
# MAGIC     col("RunTime").cast("integer").alias("runtime"),
# MAGIC     col("Movies")
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### we assume all the movies should have a minimum budget of 1 million, so if a movie has a budget of less than 1 million, we should replace it with 1 million

# COMMAND ----------

display(silver_movies)

# COMMAND ----------

#silver_movies = silver_movies.withColumn("budget",when(col("budget")<=1000000 ,1000000).otherwise(silver_movies.budget))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### drop duplicates

# COMMAND ----------

#silver_movies.count()

# COMMAND ----------

#silver_movies = silver_movies.drop_duplicates()

# COMMAND ----------

#silver_movies.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #ignore
# MAGIC from pyspark.sql.types import _parse_datatype_string
# MAGIC 
# MAGIC assert silver_movies.schema == _parse_datatype_string(
# MAGIC     """
# MAGIC   movie_id INTEGER,
# MAGIC   title STRING,
# MAGIC   overview STRING,
# MAGIC   budget DOUBLE,
# MAGIC   runtime INTEGER
# MAGIC   Movies STRUCT
# MAGIC """
# MAGIC ), "Schemas do not match"
# MAGIC print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Quarantine the Bad Data

# COMMAND ----------

silver_movies.count()

# COMMAND ----------

silver_movies.filter("runtime < 0").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Display the Quarantined Records

# COMMAND ----------

#silver_movies_clean = silver_movies.filter("runtime >= 0")
#silver_movies_quarantine = silver_movies.filter("runtime < 0")

(silver_movies_clean, silver_movies_quarantine) = generate_clean_and_quarantine_dataframes(silver_movies)

# COMMAND ----------

display(silver_movies_quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 6 write clean batch to silver table 

# COMMAND ----------

#silver_movies_clean.select("movie_id","budget","title","overview","runtime").write.format("delta").mode("append").save(f"{silver_folder_path}/movies")

bronzeToSilverWriter = batch_writer(dataframe=silver_movies_clean, exclude_columns=["Movies"])
bronzeToSilverWriter.save(f"{silver_folder_path}/movies")

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
LOCATION "{silver_folder_path}/movies"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movies_silver order by movie_id

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 7 update bronze table to reflect the loads

# COMMAND ----------

update_bronze_movies_status(spark, f"{bronze_folder_path}/movies", silver_movies_clean, "loaded")
update_bronze_movies_status(spark, f"{bronze_folder_path}/movies", silver_movies_quarantine, "quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC update claean records (update the status to "loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC from delta.tables import DeltaTable
# MAGIC 
# MAGIC bronzeTable = DeltaTable.forPath(spark, f"{bronze_folder_path}/movies")
# MAGIC silverAugmented = (
# MAGIC     silver_movies_clean.withColumn("status", lit("loaded"))
# MAGIC )
# MAGIC 
# MAGIC update_match = "m_bronze.Movies = clean.Movies"
# MAGIC update = {"status": "clean.status"}
# MAGIC 
# MAGIC (
# MAGIC   bronzeTable.alias("m_bronze")
# MAGIC   .merge(silverAugmented.alias("clean"), update_match)
# MAGIC   .whenMatchedUpdate(set=update)
# MAGIC   .execute()
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC update quarantined records

# COMMAND ----------

display(silver_movies_quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC silverAugmented = (
# MAGIC    silver_movies_quarantine.withColumn("status", lit("quarantined"))
# MAGIC )
# MAGIC 
# MAGIC update_match = "m_bronze.Movies = quarantine.Movies"
# MAGIC update = {"status" : "quarantine.status"}
# MAGIC 
# MAGIC (
# MAGIC   bronzeTable.alias("m_bronze")
# MAGIC   .merge(silverAugmented.alias("quarantine"), update_match)
# MAGIC   .whenMatchedUpdate(set=update)
# MAGIC   .execute()
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Quarantined Records

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 8 Load Quarantined Records from the Bronze Table

# COMMAND ----------

bronze_Quarantined_DF = spark.read.table("movies_bronze").filter("status = 'quarantined'")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 9 Transform the Quarantined Records

# COMMAND ----------

bronzeQuarTransDF = transform_bronze(bronze_Quarantined_DF, quarantine=True).alias("quarantine")
display(bronzeQuarTransDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 10 Batch Write the Repaired (formerly Quarantined) Records to the Silver Table

# COMMAND ----------

bronzeToSilverWriter = batch_writer(dataframe=bronzeQuarTransDF, exclude_columns=["Movies"])
bronzeToSilverWriter.save(f"{silver_folder_path}/movies")

update_bronze_table_status(spark, f"{bronze_folder_path}/", bronzeQuarTransDF, "loaded")

# COMMAND ----------

display(silverCleanedDF )

# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest raw data and add meta data to create bronze table

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC  %run "./includes/utilities"

# COMMAND ----------

#notebook idepotent
dbutils.fs.rm(f"{bronze_folder_path}/bronze", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 Ingest 8 json files

# COMMAND ----------

# MAGIC %md
# MAGIC #ignore
# MAGIC genres_schema = StructType(fields=[StructField("id", LongType(), False),
# MAGIC                                    StructField("name", StringType(), True)
# MAGIC   
# MAGIC ])

# COMMAND ----------

# MAGIC %md
# MAGIC #ignore
# MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, LongType, TimestampType
# MAGIC movie_schema = StructType(fields=[StructField("BackdropUrl", StringType(), True),
# MAGIC                                   StructField("Budget", DoubleType(), True),
# MAGIC                                   StructField("CreatedBy", StringType(), True),
# MAGIC                                   StructField("CreatedDate", StringType(), True),
# MAGIC                                   StructField("Id", LongType(), False),
# MAGIC                                   StructField("ImdbUrl", StringType(), True),
# MAGIC                                   StructField("OriginalLanguage", StringType(), True),
# MAGIC                                   StructField("Overview", StringType(), True),
# MAGIC                                   StructField("PosterUrl", StringType(), True),
# MAGIC                                   StructField("Price", DoubleType(), True),
# MAGIC                                   StructField("ReleaseDate", StringType(), True),
# MAGIC                                   StructField("Revenue", DoubleType(), True),
# MAGIC                                   StructField("RunTime", LongType(), True),
# MAGIC                                   StructField("Tagline", StringType(), True),
# MAGIC                                   StructField("Title", StringType(), True),
# MAGIC                                   StructField("TmdbUrl", StringType(), True),
# MAGIC                                   StructField("UpdatedBy", StringType(), True),
# MAGIC                                   StructField("UpdatedDate", StringType(), True),
# MAGIC                                   StructField("genres", genres_schema)
# MAGIC ])

# COMMAND ----------

# MAGIC %md
# MAGIC #ignore
# MAGIC from pyspark.sql.functions import explode
# MAGIC raw_df = spark.read.schema(movie_schema).option("multiLine", True).json(raw_folder_path).select(explode("movie").alias("movies"))

# COMMAND ----------

from pyspark.sql.functions import explode
raw_df = spark.read.option("multiline", True).option("inferSchema", "true").json("/mnt/movieshopdl/raw/movieshop").select(explode("movie").alias("movies"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 Ingest metadata
# MAGIC 
# MAGIC data source (datasource), use "files.training.databricks.com"；
# MAGIC ingestion time (ingesttime)；
# MAGIC status (status), use "new"；
# MAGIC ingestion date (ingestdate)；

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

raw_df = raw_df \
.withColumn("data_source", lit("movieshop_final_project")) \
.withColumn("ingest_time", current_timestamp()) \
.withColumn("status", lit("new")) \
.withColumn("ingest_date", current_timestamp().cast("date"))


# COMMAND ----------

# display(raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 Write to a bronze table

# COMMAND ----------

from pyspark.sql.functions import col
bronze_df = (raw_df.select("Movies.Id", "Movies.Title", "Movies.Overview", "Movies.Tagline", "Movies.Budget", "Movies.Revenue", "Movies.ImdbUrl", "Movies.TmdbUrl", "Movies.PosterUrl", "Movies.BackdropUrl", "Movies.OriginalLanguage", "Movies.ReleaseDate", "Movies.RunTime", "Movies.Price", "Movies.CreatedDate", "Movies.UpdatedDate", "Movies.UpdatedBy", "Movies.CreatedBy", "Movies.genres", "data_source", "ingest_time", "status", "ingest_date")
)
raw_df.write.format("delta").mode("append").save(f"{bronze_folder_path}/bronze")


# COMMAND ----------

# display(bronze_df)

# COMMAND ----------

# display(dbutils.fs.ls(f"{bronze_folder_path}/bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 Register the Bronze Table in the Metastore

# COMMAND ----------

spark.sql("""
drop table if exists movies_bronze
""")

spark.sql(f"""
create table movies_bronze
using delta 
location "{bronze_folder_path}/bronze"
""")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC select * from movies_bronze

# COMMAND ----------

#dbutils.fs.rm(f"{raw_folder_path}", recurse=True)

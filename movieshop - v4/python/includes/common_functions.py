# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, LongType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
    explode,
    when,
    abs
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
    exclude_columns: List = [],
    mode: str = "append",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
    )


# COMMAND ----------

def read_batch_raw(raw_folder_path: str) -> DataFrame:
    return spark.read.option("multiline", True).option("inferSchema", "true").json("/mnt/movieshopdl/raw/movieshop").select(explode("movie").alias("movies"))


# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    return raw.withColumn("data_source", lit("movieshop_final_project")).withColumn("ingest_time", current_timestamp()).withColumn("status", lit("new")).withColumn("ingest_date", current_timestamp().cast("date"))
        

# COMMAND ----------

def read_movies_bronze() -> DataFrame:
    return spark.read.table("movies_bronze").filter("status = 'new' ")

# COMMAND ----------

def read_batch_delta(deltaPath: str) -> DataFrame:
    return spark.read.format("delta").load(deltaPath)

# COMMAND ----------

def transform_movies_bronze(bronze: DataFrame, quarantine: bool = False) -> DataFrame:
    
    silver_movies = bronze_movies.select("Movies.Id","Movies.Title", "Movies.Overview", "Movies.Tagline", "Movies.Budget","Movies.Revenue","Movies.ImdbUrl","Movies.TmdbUrl","Movies.PosterUrl","Movies.BackdropUrl","Movies.OriginalLanguage","Movies.ReleaseDate","Movies.RunTime", "Movies.Price", "Movies.CreatedDate", "Movies.UpdatedDate", "Movies.UpdatedBy", "Movies.CreatedBy", "Movies.genres", "Movies")
    
    silver_movies = silver_movies.withColumn("Budget",when(col("Budget")<=1000000 ,1000000).otherwise(silver_movies.Budget))
    
    silver_movies = silver_movies.dropDuplicates()
    
    
    if not quarantine:
        silver_movies = silver_movies.select(
            col("Id").cast("integer").alias("Movie_id"),
            "Title",
            "Overview",
            "Tagline",
            "Budget",
            "Revenue",
            "ImdbUrl",
            "TmdbUrl",
            "PosterUrl",
            "BackdropUrl",
            "OriginalLanguage",
            "ReleaseDate",
            col("RunTime").cast("integer").alias("Runtime"),
            "Price",
            "CreatedDate",
            "UpdatedDate",
            "UpdatedBy",
            "CreatedBy",
            "genres",
            "Movies"
        )
    else:
        silver_movies = silver_movies.select(
            col("Id").cast("integer").alias("Movie_id"),
            "Title",
            "Overview",
            "Budget",
            abs(col("RunTime")).cast("integer").alias("Runtime"),
            "Movies"
        )

    return silver_movies

# COMMAND ----------

def generate_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter("Runtime >= 0"),
        dataframe.filter("Runtime < 0"),
    )


# COMMAND ----------

def update_bronze_movies_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:

    bronzeTable = DeltaTable.forPath(spark, bronzeTablePath)
    dataframeAugmented = dataframe.withColumn("status", lit(status))

    update_match = "m_bronze.Movies = dataframe.Movies"
    update = {"status": "dataframe.status"}

    (
        bronzeTable.alias("m_bronze")
        .merge(dataframeAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True

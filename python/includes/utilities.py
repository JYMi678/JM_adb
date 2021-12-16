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

# MAGIC %md
# MAGIC def transform_raw(raw: DataFrame) -> DataFrame:
# MAGIC     return raw.select(
# MAGIC     
# MAGIC         lit("movieshop_final_project").alias("data_source"),
# MAGIC         current_timestamp().alias("ingest_time"),
# MAGIC         lit("new").alias("status"),
# MAGIC         current_timestamp().cast("date").alias("ingest_date")
# MAGIC     )

# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    return raw.withColumn("data_source", lit("movieshop_final_project")).withColumn("ingest_time", current_timestamp()).withColumn("status", lit("new")).withColumn("ingest_date", current_timestamp().cast("date"))
        

# COMMAND ----------

def read_batch_bronze() -> DataFrame:
    return spark.read.table(f"{bronze_folder_path}/bronze").select("Id", "Title", "Overview", "Buget", "RunTime").filter("status = 'new'")

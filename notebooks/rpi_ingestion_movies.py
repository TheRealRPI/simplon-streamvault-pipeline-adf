# Databricks notebook source
# MAGIC %md
# MAGIC # ETL ADLS Movies

# COMMAND ----------

# Imports

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# from azure.storage.blob import BlobServiceClient

# Variables Globales

AZURE_CONTAINER = "<AZURE-CONTAINER>"
AZURE_KEY = "<AZURE-KEY>"
AZ_RAW_PATH = f"abfss://raw@{AZURE_CONTAINER}.dfs.core.windows.net/"
AZ_CLEAN_PATH = f"abfss://clean@{AZURE_CONTAINER}.dfs.core.windows.net/"

# Session Spark

spark = SparkSession.builder.appName("StreamVault ETL").getOrCreate()
spark.conf.set(
    f"fs.azure.account.key.{AZURE_CONTAINER}.dfs.core.windows.net", AZURE_KEY
)

print(spark.version)

# COMMAND ----------

# Creation du DataFrame des Films

schema_films = StructType(
    [
        StructField("title", StringType()),
        StructField("genres", ArrayType(StringType())),
        StructField("cast", ArrayType(StringType())),
        StructField("directors", ArrayType(StringType())),
        # year : {"$numberInt":"1893"} dans le JSON → struct
        StructField("year", StructType([StructField("$numberInt", StringType())])),
        # imdb.rating : {"$numberDouble":"7.3"} OU {"$numberInt":"6"}
        StructField(
            "imdb",
            StructType(
                [
                    StructField(
                        "rating",
                        StructType(
                            [
                                StructField("$numberDouble", StringType()),
                                StructField("$numberInt", StringType()),
                            ]
                        ),
                    ),
                ]
            ),
        ),
    ]
)

df_movies = spark.read.schema(schema_films).json(f"{AZ_RAW_PATH}movies/movies.json")
display(df_movies.printSchema())
df_movies.head(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

annee = F.col("year.`$numberInt`").cast("int")

df_clean = df_movies.select(
    F.col("title").alias("_id"),
    F.col("title"),
    # year : décapsule {"$numberInt":"1893"} → int, NULL si aberrant
    F.when(annee.between(1850, 2100), annee).alias("year"),
    F.element_at("genres", 1).alias("genre"),
    F.col("cast"),
    F.col("directors"),
    F.coalesce(
        F.col("imdb.rating.`$numberDouble`"),
        F.col("imdb.rating.`$numberInt`"),
    )
    .cast("double")
    .alias("note_imdb"),
    F.lit("film").alias("type"),
).filter(F.col("year").isNotNull())

display(df_clean.printSchema())
print(f"Nombre de lignes : {df_clean.count()}")
print(df_clean.head(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ecriture ADLS Clean

# COMMAND ----------

# Ecriture en clean

df_clean.coalesce(1).write.mode("overwrite").format("json").save(
    f"{AZ_CLEAN_PATH}movies/movies.json"
)

# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Books
# MAGIC

# COMMAND ----------

# Imports

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# from azure.storage.blob import BlobServiceClient

# Variables Globales

AZURE_CONTAINER = "<AZURE-CONTAINER>"
AZURE_KEY = "<AZURE-KEY>"
AZ_RAW_PATH = f"abfss://raw@{AZURE_CONTAINER}.dfs.core.windows.net/"
AZ_CLEAN_PATH = f"abfss://clean@{AZURE_CONTAINER}.dfs.core.windows.net/"

# COMMAND ----------

# Session Spark

spark = SparkSession.builder.appName("StreamVault ETL").getOrCreate()
spark.conf.set(
    f"fs.azure.account.key.{AZURE_CONTAINER}.dfs.core.windows.net", AZURE_KEY
)

print(spark.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion des Livres

# COMMAND ----------

# Creation du DataFrame des Livres

df_books = spark.read.csv(
    f"{AZ_RAW_PATH}books/books.csv", header=True, inferSchema=True
)
display(df_books.printSchema())
display(df_books.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

df_clean = df_books.select(
    F.col("Name").cast("string").alias("_id"),
    F.col("Name").cast("string").alias("title"),
    # year : filtre les années entre 1800 et 2100 (en string) + suppression des valeurs non numériques
    F.when(
        (F.col("PublishYear").rlike("^[0-9]+$"))  # Vérifie que c'est un nombre
        & (F.col("PublishYear").cast("int") >= 1800)
        & (F.col("PublishYear").cast("int") <= 2100),
        F.col("PublishYear").cast("string"),  # Garde le format string
    )
    .otherwise(None)
    .alias("year"),
    F.lit("Non renseigné").alias("genre"),
    F.col("Authors").cast("string").alias("directors"),
    F.col("pagesNumber").cast("int").alias("pagesNumber"),
    F.col("Rating").cast("double").alias("note"),
    F.lit("livre").alias("type"),
).filter(
    F.col("year").isNotNull()
)  # Supprime les lignes où year est NULL (dates aberrantes)

df_clean.printSchema()
print(f"Nombre de lignes : {df_clean.count()}")
display(df_clean.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ecriture en ADLS - Clean

# COMMAND ----------

# Ecriture en clean
try:
    df_clean.coalesce(1).write.mode("overwrite").format("json").save(
        f"{AZ_CLEAN_PATH}books/books.json"
    )
except Exception as e:
    print(e)

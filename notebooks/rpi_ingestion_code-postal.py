# Databricks notebook source
# Imports

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

# Creation du DataFrame des CP

df_client = spark.read.csv(
    f"{AZ_RAW_PATH}clients/clients.csv", header=True, inferSchema=True
)
display(df_client.printSchema())
display(df_client.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

# Creation du DataFrame des code postaux unique

df_cp = df_client.select(F.col("code_postal").alias("code_postal")).distinct()
print(f"Nombre de cp different : {df_cp.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy to ADLS

# COMMAND ----------

# Sauvegarde sous la forme d'un fichier unique

local_csv = "/tmp/code_postaux.csv"

df_cp.toPandas().to_csv(local_csv, index=False)

dbutils.fs.cp(f"file:{local_csv}", f"{AZ_CLEAN_PATH}clients/code_postaux.csv")

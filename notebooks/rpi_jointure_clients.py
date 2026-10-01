# Databricks notebook source
# Imports

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import json

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

# MAGIC %md
# MAGIC ## Recup du couple Code_postal : Nom_de_commune

# COMMAND ----------

# 1. Récupérer le paramètre (chaîne JSON envoyée par ADF)
cp_villes_raw = dbutils.widgets.get("cp_villes")

# 2. Parser en liste de dictionnaires
cp_villes = json.loads(cp_villes_raw)

# 3. Créer un DataFrame à partir de la liste de dictionnaires
schema = StructType(
    [StructField("code_postal", StringType()), StructField("commune", StringType())]
)

df_cp_villes = spark.createDataFrame(cp_villes, schema)

# display(df_cp_villes)

# COMMAND ----------

# Creation du DataFrame Client final

df_client_source = spark.read.csv(
    f"{AZ_RAW_PATH}clients/clients.csv", header=True, inferSchema=True
).withColumn("code_postal", F.lpad(F.col("code_postal").cast("string"), 5, "0"))

display(df_client_source.printSchema())
display(df_client_source.head(5))


# COMMAND ----------

# JOINTURE

df_client = df_client_source.join(df_cp_villes, on="code_postal", how="inner")

display(df_client.head(5))


# COMMAND ----------

# FILTRE

df_client = df_client.select(
    F.concat(F.col("nom"), F.lit("_"), F.col("prenom")).alias("_id"),
    F.col("code_postal"),
    F.col("nom"),
    F.col("prenom"),
    F.col("commune"),
    F.lit("cient").alias("type"),
)

display(df_client.head(5))

# COMMAND ----------

# Ecriture en clean

df_client.coalesce(1).write.mode("overwrite").format("json").option(
    "path", f"{AZ_CLEAN_PATH}clients/clients.json"
).option("singleFile", "true").save()

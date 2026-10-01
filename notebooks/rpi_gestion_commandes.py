# Databricks notebook source
# MAGIC %md
# MAGIC # Gestion des Commandes
# MAGIC
# MAGIC - Recup des commandes depuis Azure Event Hub
# MAGIC - Copie des commandes dans ADLS
# MAGIC - Copie des commandes dans MongoDB

# COMMAND ----------

# Imports

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# from azure.storage.blob import BlobServiceClient

# Variables Globales

AZURE_CONTAINER = "<AZURE-CONTAINER>"
AZURE_KEY = "<AZURE-KEY>"
AZ_CLEAN_PATH = f"abfss://clean@{AZURE_CONTAINER}.dfs.core.windows.net/"

# Session Spark

spark = SparkSession.builder.appName("StreamVault ETL").getOrCreate()
spark.conf.set(
    f"fs.azure.account.key.{AZURE_CONTAINER}.dfs.core.windows.net", AZURE_KEY
)

print(spark.version)

# COMMAND ----------

# Cellule 1 — Configuration

# Chaîne de connexion Event Hub (claim Listen requis — EntityPath obligatoire pour le connecteur)
EVENT_HUB_CONN_STR = "<EVENT-HUB-CONN-STR>"

# Destination des commandes (JSON, partitionné par jour)
OUTPUT_PATH = f"abfss://clean@{AZURE_CONTAINER}.dfs.core.windows.net/commands/"

# Checkpoint : dossier séparé, sinon les offsets seraient confondus avec les données
CHECKPOINT_PATH = (
    f"abfss://clean@{AZURE_CONTAINER}.dfs.core.windows.net/checkpoints/commands/"
)

# Basic tier = 1 seul consumer group
CONSUMER_GROUP = "$Default"

# Un micro-lot toutes les 30 s (~10 commandes par lot avec le producteur à 3 s)
TRIGGER_INTERVAL = "30 seconds"

# COMMAND ----------

# 1. Diagnostic → doit afficher "Connecteur OK"
try:
    enc = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt("test")
    print("Connecteur OK — encrypt('test') =", enc)
except Exception as e:
    print("Connecteur absent :", e)


# COMMAND ----------

# Cellule 2 — Lecture du flux Event Hub (Structured Streaming)

# Le connecteur exige la chaîne de connexion ENCODÉE (pas la chaîne en clair)
encoded_conn_str = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
    EVENT_HUB_CONN_STR
)

eh_options = {
    "eventhubs.connectionString": encoded_conn_str,
    "eventhubs.consumerGroup": CONSUMER_GROUP,
}

raw_stream = spark.readStream.format("eventhubs").options(**eh_options).load()

# Vérification : le connecteur livre le body en binaire + colonnes techniques
raw_stream.printSchema()

# COMMAND ----------

# Cellule 3 — Parsing : body binaire → commandes structurées

# Schéma explicite, aligné sur commandProducer.py
# (date gardée en string ISO 8601 : fidèle au producteur, sans risque de perte
# lors du cast du fuseau Europe/Paris)
command_schema = StructType(
    [
        StructField("_id", StringType()),
        StructField("client", StringType()),
        StructField("media", StringType()),
        StructField("prix", DoubleType()),
        StructField("date", StringType()),
    ]
)

commands = (
    raw_stream.selectExpr("CAST(body AS STRING) AS json_body")
    .select(F.from_json("json_body", command_schema).alias("commande"))
    .select("commande.*")
    .filter(F.col("_id").isNotNull())  # jette un éventuel événement mal formé
    .withColumn("jour", F.to_date(F.substring("date", 1, 10)))  # clé de partition
)

display(commands)  # aperçu live, non bloquant

# COMMAND ----------

# Cellule 4 — Écriture : chaque micro-lot devient des fichiers JSON dans ADLS

query = (
    commands.writeStream.format("json")
    .outputMode("append")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("jour")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

print("Requête streaming démarrée — id :", query.id)

# Blocage : le flux tourne en continu jusqu'à interruption

query.awaitTermination()

# COMMAND ----------

# Cellule 5 — Arrêt propre du consommateur
query.stop()

"""Simulateur de commandes StreamVault : lit MongoDB, envoie vers Azure Event Hub."""

import json
import logging
import os
from datetime import datetime
from time import sleep
from typing import TypedDict
from uuid import uuid4
from zlib import crc32
from zoneinfo import ZoneInfo

from azure.eventhub import EventData, EventHubProducerClient, TransportType
from pymongo import MongoClient
from pymongo.collection import Collection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "streamvault"
EVENT_HUB_NAME = "eventhubcruster"


class Commande(TypedDict):
    _id: str
    client: str
    media: str
    prix: float
    date: str


def createCommand(cl: Collection, md: Collection) -> Commande:
    """Crée une commande fictive : client et média réels tirés au hasard,
    prix stable par titre, date ISO 8601 (Europe/Paris)."""
    try:
        commande_id = str(uuid4())
        # Tirage aléatoire côté serveur MongoDB (remplace find_one(), qui
        # renvoie toujours le premier document dans l'ordre naturel, donc
        # toujours le même client/média) :
        # - aggregate([étapes]) : exécute un pipeline d'agrégation sur le
        #   serveur Mongo ; une seule étape suffit ici
        # - { "$sample": { "size": 1 } } : sélectionne 1 document au hasard
        #   dans toute la collection (tirage aléatoire natif MongoDB)
        # - le pipeline renvoie un curseur ; .next() avance sur le document
        #   tiré, ["_id"] en extrait l'identifiant
        # - str() : un ObjectId n'est pas sérialisable en JSON, on le
        #   convertit en chaîne
        client = str(cl.aggregate([{"$sample": {"size": 1}}]).next()["_id"])
        media = str(md.aggregate([{"$sample": {"size": 1}}]).next()["_id"])
        # Prix déterministe par titre (Q17 : un titre donné garde toujours le
        # même prix). Dérivation sans stockage, de l'intérieur vers l'extérieur :
        # - crc32(media) : somme de contrôle CRC-32 des octets du titre, stable
        #   d'une exécution/machine à l'autre (contrairement au hash() natif
        #   Python, randomisé à chaque lancement de processus)
        # - % 900 : replie la valeur dans [0, 899], répartition quasi uniforme
        # - / 100 puis 1 + ... : décale le résultat dans [1.00, 9.99]
        # - round(..., 2) : arrondi en centimes
        # Limite : des titres différents peuvent partager le même prix
        # (collision sur 900 valeurs) — sans importance, seule la stabilité
        # par titre est exigée.
        prix = round(1 + (crc32(media.encode()) % 900) / 100, 2)
        date = datetime.now(ZoneInfo("Europe/Paris")).isoformat()

        commande = {
            "_id": commande_id,
            "client": client,
            "media": media,
            "prix": prix,
            "date": date,
        }

        return commande
    except Exception as e:
        logger.error("Erreur lors de la création de commande : %s", e)
        raise RuntimeError("Erreur lors de la création de commande") from e


def sendCommand(producer: EventHubProducerClient, commande: Commande) -> None:
    """Sérialise la commande en JSON et l'envoie à l'Event Hub."""
    try:
        batch = producer.create_batch()
        batch.add(EventData(json.dumps(commande)))
        producer.send_batch(batch)
        logger.info("Commande envoyée : %s", commande)
    except Exception as e:
        logger.error("Erreur lors de l'envoi vers Event Hub : %s", e)
        raise RuntimeError("Erreur lors de l'envoi vers Event Hub") from e


def getEventHubName(connection_str: str) -> str:
    """Renvoie l'EntityPath de la chaîne de connexion, sinon EVENT_HUB_NAME."""
    for part in connection_str.split(";"):
        if part.strip().startswith("EntityPath="):
            return part.strip().split("=", 1)[1]
    return EVENT_HUB_NAME


def getMongoCollections(mongoClient: MongoClient) -> tuple[Collection, Collection]:
    """Renvoie les collections clients et media de la base StreamVault."""
    try:
        database = mongoClient[MONGO_DB]
        clients = database["clients"]
        medias = database["media"]

        logger.info(
            "Collections MongoDB prêtes : %s.clients, %s.media", MONGO_DB, MONGO_DB
        )

        return clients, medias

    except Exception as e:
        logger.error("Erreur lors de la connexion a MongoDB : %s", e)
        raise RuntimeError("Erreur lors de la connexion a MongoDB") from e


def simulateCommands(nbCommands: int) -> None:
    """Génère et envoie une commande toutes les 3 secondes."""
    connection_str = os.environ.get("EVENT_HUB_CONN_STR")
    if not connection_str:
        logger.error("Variable d'environnement EVENT_HUB_CONN_STR manquante")
        raise RuntimeError(
            "Définissez EVENT_HUB_CONN_STR avec la chaîne de connexion de l'Event Hub"
        )

    mongo: MongoClient | None = None
    producer: EventHubProducerClient | None = None
    total = nbCommands
    try:
        logger.info("Connexion a MongoDB...")
        mongo = MongoClient(MONGO_URI)
        cl, md = getMongoCollections(mongo)

        logger.info("Connexion a l'Event Hub '%s'...", EVENT_HUB_NAME)
        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_str,
            eventhub_name=getEventHubName(connection_str),
            transport_type=TransportType.AmqpOverWebsocket,
        )

        while nbCommands > 0:
            commande = createCommand(cl, md)
            sendCommand(producer, commande)
            nbCommands -= 1
            sleep(3)

        logger.info("Simulation terminée : %d commandes envoyées", total)
    except Exception as e:
        logger.error("Erreur lors de la simulation des commandes : %s", e)
        raise RuntimeError("Erreur lors de la simulation des commandes") from e
    finally:
        if producer:
            producer.close()
        if mongo:
            mongo.close()


if __name__ == "__main__":
    simulateCommands(100)

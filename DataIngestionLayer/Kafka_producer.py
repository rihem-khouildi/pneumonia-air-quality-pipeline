import csv
import logging
import random
from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration du logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='kafka-iot:9092',
    value_serializer=lambda v: v.encode('utf-8')  # Assurer que les messages sont encodés en UTF-8
)

# Nom du topic Kafka
topic = 'kafka_topic'

def send_message(message):
    """
    Fonction pour envoyer un message à Kafka et gérer les erreurs
    """
    try:
        future = producer.send(topic, value=message)
        result = future.get(timeout=60)  # Attente de la confirmation d'envoi
        logger.info(f"Sent: {message} to {topic}, Result: {result}")
    except KafkaError as e:
        logger.error(f"Error sending message: {message} | Error: {e}")

def produce_from_csv(file_path):
    """
    Fonction pour lire un fichier CSV, mélanger les lignes et envoyer chaque ligne à Kafka
    """
    with open(file_path, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)  # Lire toutes les lignes du fichier CSV dans une liste
        random.shuffle(rows)  # Mélanger les lignes de manière aléatoire
        
        for row in rows:
            # Joindre les éléments de la ligne CSV en un message
            message = ",".join(row)
            send_message(message)
            sleep(1)  # Simuler un envoi en temps réel
        
        logger.info(f"Finished sending all messages from {file_path}")

if __name__ == "__main__":
    csv_file = 'chest_xray_multimodal_60k_menghir_nul.csv'  # Chemin vers ton fichier CSV
    produce_from_csv(csv_file)
    producer.flush()  # S'assurer que tous les messages sont envoyés avant de fermer le programme
    producer.close()  # Fermer proprement le producteur Kafka

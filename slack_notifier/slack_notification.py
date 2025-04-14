from confluent_kafka import Consumer
import json
import time
import requests
import os
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('slack-notifier')

# Définition des variables ou récupération de variables d'environnement
KAFKA_SERVERS = "redpanda:9092"
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sport.sport_advantages.sport_activities")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
KAFKA_GROUP_ID = 'slack-notifier-group'

logger.info(f"Configuration: KAFKA_SERVERS={KAFKA_SERVERS}, KAFKA_TOPIC={KAFKA_TOPIC}")
logger.info(f"Webhook URL configuré: {'Oui' if SLACK_WEBHOOK_URL else 'Non'}")

# Configuration du consommateur
consumer = Consumer({
    'bootstrap.servers': KAFKA_SERVERS,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True 
})

# S'abonner au topic
consumer.subscribe([KAFKA_TOPIC])
logger.info(f"Abonnement au topic: {KAFKA_TOPIC}")

# Fonction pour envoyer une notification à Slack
def send_to_slack(message):
    """Envoie de message sur Slack à l'aide de Webhook Slack
    
    Args:
        message (dict): Message dans topic Redpanda
    Returns:
        Boolean: True ou False si le message a bien envoyé sur Slack ou pas
    """
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL n'est pas configuré, notification ignorée")
        return False
    
    try:
        # Vérifier la structure du message
        if 'payload' not in message or 'after' not in message['payload']:
            logger.warning(f"Structure de message invalide: {message}")
            return False
            
        activity = message["payload"]["after"]
        
        # Vérifier que les champs nécessaires existent
        if 'sport_type' not in activity or 'comment' not in activity or 'first_name' not in activity or 'last_name' not in activity:
            logger.warning(f"Champs manquants dans l'activité: {activity}")
            return False
        
        
        slack_message = {
            "text": f"Auteur: {activity.get('first_name')} {activity.get('last_name')}",
            "attachments": [  # Notez le pluriel "attachments"
                {
                    "pretext": activity.get("comment"),
                    "text": f"Type d'activité: {activity.get('sport_type')}",
                    "color": "#36a64f"
                }
            ]
        }
        
        logger.info(f"Envoi du message Slack: {slack_message}")
        
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json=slack_message,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            logger.info("Message Slack envoyé avec succès")
            return True
        else:
            logger.error(f"Échec de l'envoi Slack. Code: {response.status_code}, Réponse: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Erreur lors de l'envoi Slack: {e}")
        return False

# Boucle de lecture continue
logger.info(f"Démarrage de la consommation du topic {KAFKA_TOPIC} ...")

try:
    while True:
        # Poll pour obtenir des messages avec timeout de 1 seconde
        msg = consumer.poll(1.0)
        
        # Si aucun message n'est disponible, continuer
        if msg is None:
            continue
        
        # Gestion des erreurs
        if msg.error():
            logger.error(f"Erreur consommateur: {msg.error()}")
            continue
        
        # Traitement du message
        try:
            # Décoder le message JSON
            value_str = msg.value().decode('utf-8')
            logger.debug(f"Message reçu: {value_str[:200]}...")  # Limiter à 200 caractères pour le log
            
            value = json.loads(value_str)
            
            # Envoyer à Slack
            if 'payload' in value and 'after' in value['payload']:
                activity = value['payload']['after']
                comment = activity.get('comment')
                if comment:
                    logger.info(f"Auteur: {activity.get('first_name', 'inconnu')} {activity.get('last_name', 'inconnu')}: Activité détectée: {activity.get('sport_type', 'inconnu')} - {activity.get('comment', 'pas de commentaire')}")
                    send_to_slack(value)
            else:
                logger.warning(f"Structure de message non reconnue: {value}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Erreur de décodage JSON: {e}")
            logger.error(f"Message brut: {msg.value()[:200]}...")  # Montrer le début du message
        except Exception as e:
            logger.error(f"Erreur de traitement: {e}")
            
except KeyboardInterrupt:
    logger.warning("Arrêt de la consommation")
finally:
    # Fermeture propre du consommateur
    consumer.close()
    logger.info("Consommateur fermé")
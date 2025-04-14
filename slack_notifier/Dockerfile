FROM python:3.11-slim

WORKDIR /app

# Installation des dépendances
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copie des scripts
COPY . .

# Définition du point d'entrée
CMD ["python", "slack_notification.py"]
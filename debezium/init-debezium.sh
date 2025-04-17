#!/bin/bash

# Démarrer le service Debezium
/docker-entrypoint.sh start &

# Attendre que le service soit prêt
for i in {1..10}; do
  if curl -s -f http://localhost:8083/; then
    # Créer le connecteur en utilisant les variables d'environnement
    curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d '{
      "name": "sport-advantages-postgres-connector",
      "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "'"$SPORT_POSTGRES_HOST"'",
        "database.port": "'"$SPORT_POSTGRES_INTERNAL_PORT"'",
        "database.user": "'"$SPORT_POSTGRES_USER"'",
        "database.password": "'"$SPORT_POSTGRES_PASSWORD"'",
        "database.dbname": "'"$SPORT_POSTGRES_DB"'",
        "database.server.name": "sport-advantages",
        "table.include.list": "sport_advantages.sport_activities",
        "plugin.name": "pgoutput",
        "topic.prefix": "sport",
        "snapshot.mode": "initial"
      }
    }'
    break
  fi
  sleep $((2 * i))
done

# Garder le conteneur en vie
tail -f /dev/null
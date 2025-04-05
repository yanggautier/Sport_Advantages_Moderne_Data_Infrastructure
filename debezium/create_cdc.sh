#! /bin/bash

docker compose exec debezium `curl -H 'Content-Type: application/json' debezium:8083/connectors --data '
{
  "name": "postgres-connector",  
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector", 
    "plugin.name": "pgoutput",
    "database.hostname": "'"${SPORT_POSTGRES_HOST}"'", 
    "database.port": "'"${SPORT_POSTGRES_PORT}"'", 
    "database.user": "'"${SPORT_POSTGRES_USER}"'", 
    "database.password": "'"${SPORT_POSTGRES_PASSWORD}"'", 
    "database.dbname" : "'"${SPORT_POSTGRES_DB}"'", 
    "database.server.name": "sport-advantages", 
    "table.include.list": "sport_advantages.sport_activities",
    "snapshot.mode": "initial"
  }
}'
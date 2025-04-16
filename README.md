


Connection à la base de données
`psql -h localhost -p 5432 -U [Username] -d [DBname]`

- PostgreSQL pour les données métier
- Debezium pour le CDC (Change Data Capture)
- Redpanda comme alternative à Kafka
- Spark pour le traitement des données
- Delta Lake pour le stockage en couche delta
- MinIO comme stockage compatible S3
- Airflow pour l'orchestration

Télécharger ces fichiers sur site de Maven, puis mettre dans le répertoire ./trino/plugin
- `trino-delta-lake-413.jar`(https://mvnrepository.com/artifact/io.trino/trino-delta-lake/413) 


docker exec -it sport-advantages-trino trino

-- Show schemas in delta catalog
SHOW SCHEMAS FROM delta;

-- Create a schema if needed
CREATE SCHEMA IF NOT EXISTS delta.default;

-- Register an existing Delta table
CALL delta.system.register_table('default', 'sport_activities', 's3://delta-tables/tables/sport_activities');

-- Query your Delta table
SELECT * FROM delta.default.sport_activities LIMIT 10;


# Ajout de connection de database dans Superset 
trino://trino@trino:8080/delta


## ID de Dashboard dans Grafana
Dashboard ID 9628: PostgreSQL Database
Dashboard ID 12485: PostgreSQL Exporter Dashboard



pour connecter à la base de données métier: docker exec -it business-postgres psql -h localhost -p 5432 -U sportadvantagehr -d sportadvantages



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


Requête pour Trino

-- Show all available catalogs
SHOW CATALOGS;

-- Show schemas in hive catalog
SHOW SCHEMAS FROM hive;

-- Show schemas in delta catalog
SHOW SCHEMAS FROM delta;

-- Create a schema if needed
CREATE SCHEMA IF NOT EXISTS delta.default;

-- Register an existing Delta table
CALL delta.system.register_table('default', 'sport_activities', 's3://delta-tables/your_table_path');

-- Query your Delta table
SELECT * FROM delta.default.your_table LIMIT 10;


- PostgreSQL pour les données métier
- Debezium pour le CDC (Change Data Capture)
- Redpanda comme alternative à Kafka
- Spark pour le traitement des données
- Delta Lake pour le stockage en couche delta
- MinIO comme stockage compatible S3
- Airflow pour l'orchestration


docker exec -it sport-advantages-trino trino

-- Show schemas in delta catalog
SHOW SCHEMAS FROM delta;

-- Create a schema if needed
CREATE SCHEMA IF NOT EXISTS delta.default;

-- Register an existing Delta table
CALL delta.system.register_table('default', 'sport_activities', 's3://delta-tables/tables/sport_activities');

-- Query your Delta table
SELECT * FROM delta.default.sport_activities LIMIT 10;


-- Pour la table finale
CALL delta.system.register_table('default', 'final', 's3://final-tables/joined_data');

-- Query your Fainal table
SELECT * FROM delta.default.final LIMIT 10;



## ID de Dashboard dans Grafana
Dashboard ID 9628: PostgreSQL Database
Dashboard ID 12485: PostgreSQL Exporter Dashboard



## Commande pour connecter à la base de données
docker exec -it business-postgres psql -h localhost -p 5432 -U [Username] -d [DBname]

## Vérification de connecteur Debezium pour Redpanda
curl -X GET http://localhost:8083/connectors

## Création mannulle de connecteur Debezium Redpanda
```
docker exec -it sport-advantages-debezium curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d '{
  "name": "sport-advantages-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "business-postgres",
    "database.port": "5432",
    "database.user": "'${SPORT_POSTGRES_USER}'",
    "database.password": "'${SPORT_POSTGRES_PASSWORD}'",
    "database.dbname": "'${SPORT_POSTGRES_DB}'",
    "database.server.name": "business-postgres",
    "table.include.list": "sport_advantages.sport_activities",
    "plugin.name": "pgoutput",
    "topic.prefix": "sport",
    "snapshot.mode": "initial"
  }
}'
```

## Instruction pour Superset 
1. Ajout de connection de database dans Superset 
  - Nom de database au choix
  - Type de base de données: Trino avec SQLALCHEMY URL `trino://trino@trino:8080/delta`

2. Création de dataset 
  - Choisir database qu'on a créé
  - Schema: `default`
  - Table: `final`

3. Création de dataset personnalisé
```
WITH prime AS (
  SELECT 
    id_employee,  
    CASE 
      WHEN is_valid = true THEN ROUND(gross_salary * 0.05, 2)
      ELSE 0
    END AS commute_prime
  FROM final
)
SELECT 
  f.id_employee,
  f.gross_salary,
  f.business_unity,
  f.constract_type, 
  f.calculed_distance,
  f.is_valid AS commute_valid,
  CASE 
    WHEN f.count_activity >= 15 THEN true
    ELSE false
  END AS is_valid_activities,
  f.mean_duration,
  p.commute_prime,
  (f.gross_salary + p.commute_prime) AS total_salary 
FROM final f
JOIN prime p ON f.id_employee = p.id_employee;  
```

4. Création de visuel et dashboard


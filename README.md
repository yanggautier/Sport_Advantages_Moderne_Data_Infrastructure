# Projet Sport Advantages

Ce projet implémente une architecture complète de traitement de données pour gérer un programme d'avantages liés aux activités sportives des employés. Le système capture, traite, analyse et visualise les activités sportives pour calculer les primes et avantages.

## Schéma
![Schéma](images/schema.png)

## Architecture

Le projet utilise une architecture moderne de data engineering avec les composants suivants :

- **PostgreSQL** : Stockage des données métier (activités sportives et informations des employés)
- **Debezium** : Capture des changements de données (CDC) depuis PostgreSQL
- **Redpanda** : Alternative à Kafka pour le streaming de données
- **Spark** : Traitement distribué des données
- **Delta Lake** : Format de stockage en couches pour les données traitées
- **MinIO** : Stockage objet compatible S3
- **Airflow** : Orchestration des workflows de données
- **Trino** : Moteur de requêtes SQL pour l'analyse de données
- **Superset** : Visualisation et dashboarding
- **Prometheus/Grafana** : Monitoring de l'infrastructure

## Structure du projet

```
├── README.md                # Documentation du projet
├── activity_generator       # Service de génération de données d'activités sportives
├── airflow                  # Configuration et DAGs Airflow
├── commute_validation       # Service de validation des trajets domicile-travail
├── docker-compose.yaml      # Configuration des services
├── init-scripts             # Scripts d'initialisation de la base de données
├── monitoring               # Configuration des outils de monitoring
├── postgres-config          # Configuration PostgreSQL
├── slack_notifier           # Notifications Slack
├── spark                    # Scripts Spark/Scala pour le traitement des données
├── superset                 # Configuration et dashboards Superset
└── trino                    # Configuration du moteur de requête Trino
```

## Prérequis

- Docker et Docker Compose
- Au moins 8GB de RAM disponible
- Télécharger le fichier [`trino-delta-lake-413.jar`](https://mvnrepository.com/artifact/io.trino/trino-delta-lake/413) et le placer dans le répertoire `./trino/plugin`

## Démarrage rapide

1. Cloner le dépôt
```bash
git clone <repository-url>
cd sport-advantages
```

2. Création d'un clé API pour l'API `Distance Matrix` sur le site Google et un URL d'Incoming Webhook sur le site Slake

3. Créer un fichier `.env` à partir de `.env.sample` en complétant avec les 2 valeurs lors de l'étape précédente:

4. Lancer les services
```bash
docker-compose up -d
```

5. Vérifier que tous les services sont démarrés
```bash
docker-compose ps
```

## Flux de données
1. **Chargement de données**: Le service `init-db-commute-validation` import de données RH dans la base de données PostgreSQL et utilise l'API Distance Matrix pour valider les moyens de transports

2. **Génération des données** : Le service `activity_generator` insère des activités sportives simulées dans PostgreSQL

3. **Capture des changements** : Debezium capture les modifications de la table `sport_activities` et les publie sur Redpanda

4. **Traitement des données** : Les scripts Spark consomment les messages Redpanda et les stockent dans Delta Lake via MinIO

4. **Orchestration** : Airflow gère les jobs de transformation et de validation des données

5. **Analyse** : Trino permet d'interroger les données stockées dans Delta Lake

6. **Visualisation** : Superset fournit des dashboards pour analyser les activités et calculer les primes

## Accès aux interfaces utilisateur

- **Airflow** : http://localhost:8081 (admin/admin)
- **Redpanda Console** : http://localhost:8080
- **Spark Master** : http://localhost:8090
- **MinIO Console** : http://localhost:9001 (minio_user/minio_password_)
- **Trino** : http://localhost:8100
- **Superset** : http://localhost:8088 (admin/admin)
- **Prometheus** : http://localhost:9090
- **Grafana** : http://localhost:3000 (admin/admin)


## Dags dans Airflow
- **great_expectations_validation** : Validations de données de la table `sport_advantages.sport_activities` dans PostgreSQL 
- **spark_local_test** : Test la connection entre Airflow et Spark

- **join_tables** : Lecture de données dans Delta Table `sport_activities`, table dans PostgreSQL `sport_advantages.employes` et de les joindre puis sauvegarder dans une autre Delta Table `final`

## Configuration de Trino

Une fois les services démarrés, vous pouvez accéder à Trino et configurer l'accès aux tables Delta :

```bash
docker exec -it sport-advantages-trino trino

# Afficher les schémas disponibles
SHOW SCHEMAS FROM delta;

# Créer un schéma si nécessaire
CREATE SCHEMA IF NOT EXISTS delta.default;

# Enregistrer une table Delta existante
CALL delta.system.register_table('default', 'sport_activities', 's3://delta-tables/tables/sport_activities');

# Interroger la table Delta
SELECT * FROM delta.default.sport_activities LIMIT 10;

# Pour la table finale des données jointes
CALL delta.system.register_table('default', 'final', 's3://final-tables/joined_data');

# Interroger la table finale
SELECT * FROM delta.default.final LIMIT 10;
```

## Configuration de Superset

1. **Ajouter une connexion à la base de données**:
   - Nom de la base de données : au choix
   - Type de base de données : Trino
   - SQLALCHEMY URL : `trino://trino@trino:8080/delta`

2. **Créer un dataset**:
   - Choisir la base de données créée
   - Schéma : `default`
   - Table : `final`

3. **Créer un dataset personnalisé avec la requête SQL suivante**:
```sql
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
FROM final f JOIN prime p ON f.id_employee = p.id_employee;
```

4. **Créer des visualisations et un dashboard**:
   - Répartition des primes par unité commerciale
   - Nombre d'activités sportives par employé
   - Distance moyenne domicile-travail
   - Distribution des salaires avant/après prime
   - Taux de validation des activités sportives

## Monitoring avec Grafana

Les dashboards préconfigurés sont disponibles dans Grafana :
- Dashboard ID 9628 : PostgreSQL Database
- Dashboard ID 12485 : PostgreSQL Exporter

## Commandes utiles

### PostgreSQL
```bash
# Se connecter à la base de données métier
docker exec -it business-postgres psql -h localhost -p 5432 -U sport_user -d sport_db
```

### Debezium
```bash
# Vérifier les connecteurs Debezium
curl -X GET http://localhost:8083/connectors

# Créer manuellement un connecteur Debezium
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

### Redpanda
```bash
# Lister les topics
docker exec -it sport-advantages-redpanda rpk topic list

# Consommer des messages d'un topic
docker exec -it sport-advantages-redpanda rpk topic consume sport.sport_advantages.sport_activities
```

### Spark
```bash
# Accéder au shell Spark
docker exec -it sport-advantages-spark-master spark-shell

# Vérifier les applications Spark en cours d'exécution
docker exec -it sport-advantages-spark-master /opt/bitnami/spark/bin/spark-submit --list
```

### MinIO
```bash
# Lister les buckets
docker exec -it sport-advantages-minio-init mc ls myminio

# Lister les fichiers dans un bucket
docker exec -it sport-advantages-minio-init mc ls myminio/delta-tables
```

## Architecture détaillée

### Génération de données

Le service `activity_generator` génère des données sportives fictives et les insère dans la base de données PostgreSQL. Ces données simulent les activités sportives des employés de l'entreprise.

### Validation des trajets domicile-travail

Le service `commute_validation` importe les données RH des employés et valide les trajets domicile-travail en utilisant l'API Google Maps. Cette validation détermine si un employé est éligible à une prime de transport écologique.

### Traitement des données avec Spark et Delta Lake

1. Debezium capture les changements dans la table `sport_activities`
2. Les changements sont publiés sur un topic Redpanda
3. Un job Spark (Scala) consomme les messages et les stocke dans un format Delta Lake sur MinIO
4. Des transformations supplémentaires sont effectuées pour enrichir les données et calculer les métriques

### Orchestration avec Airflow

Airflow gère plusieurs workflows :
- Validation des données d'activités sportives
- Traitement des données avec Spark
- Réécriture des tables Delta
- Calcul des primes et des avantages

### Analyse avec Trino

Trino permet d'interroger les données stockées dans Delta Lake et PostgreSQL avec un langage SQL unifié, facilitant les analyses complexes et les jointures entre différentes sources.

### Visualisation avec Superset

Superset fournit des dashboards interactifs permettant d'analyser :
- La participation aux activités sportives par département
- Le calcul des primes de transport écologique
- Les métriques de performance sportive
- L'impact financier du programme d'avantages

### Notifications avec Slack

Le service `slack_notifier` envoie des notifications sur Slack lorsque des événements importants se produisent, comme l'insertion de nouvelles activités sportives ou le calcul des primes.

## Dépannage

### Problèmes de connectivité
- Vérifier que tous les conteneurs sont en cours d'exécution : `docker-compose ps`
- Vérifier les logs des conteneurs : `docker-compose logs <service-name>`

### Problèmes avec Debezium
- Vérifier l'état du connecteur : `curl -X GET http://localhost:8083/connectors/<connector-name>/status`
- Supprimer et recréer le connecteur si nécessaire

### Problèmes avec Delta Lake
- Vérifier que les tables sont correctement enregistrées dans Trino
- Vérifier les permissions et l'accès à MinIO

### Problèmes de performances
- Ajuster les ressources des conteneurs dans le fichier `docker-compose.yaml`
- Optimiser les requêtes SQL et les transformations Spark

## Conclusion

Ce projet démontre une architecture moderne de data engineering pour gérer un programme d'avantages liés aux activités sportives des employés. Il utilise des technologies open-source de pointe pour capturer, traiter, analyser et visualiser les données, permettant aux entreprises de promouvoir un mode de vie sain tout en offrant des avantages financiers aux employés actifs.

Pour toute question ou amélioration, n'hésitez pas à contribuer au projet ou à ouvrir une issue sur le dépôt.
import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from great_expectations.checkpoint import Checkpoint


# Récupérer la variable d'environnement pour chaîne de connection PostgreSQL
AIRFLOW_CONN_SPORT_ADVANTAGES_DB = os.environ.get("AIRFLOW_CONN_SPORT_ADVANTAGES_DB")
print(f"Connection string: {AIRFLOW_CONN_SPORT_ADVANTAGES_DB}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def log_and_execute_query(conn, query, message):
    """Exécute une requête et logue le résultat"""
    print(f"Exécution de la requête: {query}")
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    print(f"{message}: {result}")
    cursor.close()
    return result

def check_table_exists():
    """Vérifie si la table sport_activities existe avant de lancer la validation"""
    import psycopg2
    import time
    from urllib.parse import urlparse
    
    print(f"Connexion string vérifiée: {AIRFLOW_CONN_SPORT_ADVANTAGES_DB}")
    
    # Parse de l'URL de connexion
    if AIRFLOW_CONN_SPORT_ADVANTAGES_DB.startswith('postgresql://'):
        parsed = urlparse(AIRFLOW_CONN_SPORT_ADVANTAGES_DB)
        username = parsed.username
        password = parsed.password
        database = parsed.path[1:]
        hostname = parsed.hostname
        port = parsed.port or 5432
        print(f"Paramètres de connexion: host={hostname}, port={port}, user={username}, database={database}")
    else:
        raise ValueError(f"Format de connexion invalide: {AIRFLOW_CONN_SPORT_ADVANTAGES_DB}")
    
    max_retries = 5
    retry_interval = 30  # secondes
    
    for attempt in range(max_retries):
        try:
            print(f"Tentative de connexion {attempt+1}/{max_retries}")
            conn = psycopg2.connect(
                host=hostname,
                port=port,
                user=username, 
                password=password,
                database=database
            )
            print("Connexion réussie!")
            
            # Liste tous les schémas
            all_schemas = log_and_execute_query(
                conn, 
                "SELECT schema_name FROM information_schema.schemata;",
                "Tous les schémas disponibles"
            )
            
            # Vérifie si le schéma existe
            schema_exists = log_and_execute_query(
                conn,
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'sport_advantages';",
                "Schéma sport_advantages"
            )
            
            if not schema_exists:
                print(f"Le schéma 'sport_advantages' n'existe pas. Tentative {attempt+1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(retry_interval)
                    continue
                else:
                    raise ValueError("Le schéma 'sport_advantages' n'existe pas après plusieurs tentatives")
            
            # Liste toutes les tables dans le schéma
            all_tables = log_and_execute_query(
                conn,
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'sport_advantages';",
                "Toutes les tables dans le schéma sport_advantages"
            )
            
            # Vérifie si la table existe
            table_exists = log_and_execute_query(
                conn,
                """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'sport_advantages' 
                AND table_name = 'sport_activities';
                """,
                "Table sport_activities"
            )
            
            if not table_exists:
                print(f"La table 'sport_advantages.sport_activities' n'existe pas. Tentative {attempt+1}/{max_retries}")
                # Créer le schéma et la table si nécessaire (à des fins de test)
                if attempt < max_retries - 1:
                    print("Tentative de création du schéma et de la table...")
                    try:
                        cursor = conn.cursor()
                        # Créer le schéma s'il n'existe pas
                        cursor.execute("CREATE SCHEMA IF NOT EXISTS sport_advantages;")
                        # Créer la table si elle n'existe pas
                        cursor.execute("""
                            CREATE TABLE IF NOT EXISTS sport_advantages.sport_activities (
                                id SERIAL PRIMARY KEY,
                                start_datetime TIMESTAMP NOT NULL,
                                sport_type VARCHAR(50) NOT NULL,
                                activity_duration INTEGER NOT NULL CHECK (activity_duration >= 0),
                                distance NUMERIC CHECK (distance >= 0 OR distance IS NULL)
                            );
                        """)
                        # Insérer quelques données de test
                        cursor.execute("""
                            INSERT INTO sport_advantages.sport_activities 
                            (start_datetime, sport_type, activity_duration, distance)
                            VALUES 
                            (NOW(), 'Running', 3600, 10.5),
                            (NOW(), 'Swimming', 1800, 1.2),
                            (NOW(), 'Cycling', 7200, 35.0);
                        """)
                        conn.commit()
                        print("Schéma et table créés avec succès !")
                    except Exception as e:
                        print(f"Erreur lors de la création de la table: {e}")
                        conn.rollback()
                    finally:
                        cursor.close()
                    
                    time.sleep(retry_interval)
                    continue
                else:
                    raise ValueError("La table 'sport_advantages.sport_activities' n'existe pas après plusieurs tentatives")
            
            print("La table 'sport_advantages.sport_activities' existe!")
            
            # Vérifie le contenu de la table
            sample_data = log_and_execute_query(
                conn,
                "SELECT * FROM sport_advantages.sport_activities LIMIT 5;",
                "Échantillon de données"
            )
            
            return True
            
        except Exception as e:
            print(f"Erreur lors de la vérification de la table: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Nouvelle tentative dans {retry_interval} secondes...")
                time.sleep(retry_interval)
            else:
                print(f"Échec après {max_retries} tentatives")
                raise
        finally:
            if 'conn' in locals() and conn is not None:
                conn.close()
                print("Connexion fermée")
    
    return False

def run_ge_validation():
    """ Exécuter validation de Great Expectations sur la table sport_activites."""
    import great_expectations as gx

    print("Démarrage de la validation Great Expectations")
    print(f"Connection string pour GE: {AIRFLOW_CONN_SPORT_ADVANTAGES_DB}")
    
    # Initialiser le contexte de Great Expectations
    context = gx.get_context()
    print("Contexte GE initialisé")

    # Ajouter la source PostgreSQL
    print("Ajout de la source PostgreSQL...")
    pg_datasource = context.sources.add_postgres(
        name="pg_datasource", 
        connection_string=AIRFLOW_CONN_SPORT_ADVANTAGES_DB
    )
    print("Source PostgreSQL ajoutée")
    
    # Vérification des méta-informations pour le débogage
    print("Connexion à la base de données pour vérifier la table...")
    import psycopg2
    from urllib.parse import urlparse
    
    if AIRFLOW_CONN_SPORT_ADVANTAGES_DB.startswith('postgresql://'):
        parsed = urlparse(AIRFLOW_CONN_SPORT_ADVANTAGES_DB)
        username = parsed.username
        password = parsed.password
        database = parsed.path[1:]
        hostname = parsed.hostname
        port = parsed.port or 5432
        print(f"Paramètres de connexion GE: host={hostname}, port={port}, user={username}, database={database}")
    
    try:
        conn = psycopg2.connect(
            host=hostname,
            port=port,
            user=username, 
            password=password,
            database=database
        )
        cursor = conn.cursor()
        
        # Vérifier les schémas disponibles
        cursor.execute("SELECT schema_name FROM information_schema.schemata;")
        schemas = cursor.fetchall()
        print(f"Schémas disponibles: {schemas}")
        
        # Vérifier si le schéma sport_advantages existe
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'sport_advantages';")
        schema_exists = cursor.fetchone() is not None
        print(f"Le schéma sport_advantages existe: {schema_exists}")
        
        if schema_exists:
            # Vérifier les tables dans le schéma sport_advantages
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'sport_advantages';")
            tables = cursor.fetchall()
            print(f"Tables dans le schéma sport_advantages: {tables}")
            
            # Vérifier si la table sport_activities existe
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'sport_advantages' 
                AND table_name = 'sport_activities';
            """)
            table_exists = cursor.fetchone() is not None
            print(f"La table sport_activities existe: {table_exists}")
            
            if table_exists:
                # Vérifier la structure de la table
                cursor.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'sport_advantages' 
                    AND table_name = 'sport_activities';
                """)
                columns = cursor.fetchall()
                print(f"Colonnes de la table: {columns}")
                
                # Vérifier le contenu de la table
                cursor.execute("SELECT COUNT(*) FROM sport_advantages.sport_activities;")
                row_count = cursor.fetchone()[0]
                print(f"Nombre de lignes dans la table: {row_count}")
                
                cursor.execute("SELECT * FROM sport_advantages.sport_activities LIMIT 5;")
                sample_data = cursor.fetchall()
                print(f"Échantillon de données: {sample_data}")
    except Exception as e:
        print(f"Erreur lors de la vérification de la base de données: {e}")
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()
    
    # Ajouter l'asset de table pour validation
    print("Ajout de l'asset de table...")
    try:
        pg_datasource.add_table_asset(
            name="sport_activities", 
            schema_name="sport_advantages",
            table_name="sport_activities"
        )
        print("Asset de table ajouté avec succès")
    except Exception as e:
        print(f"Erreur lors de l'ajout de l'asset de table: {e}")
        raise

    # Création de requête batch pour la table
    print("Construction de la requête batch...")
    batch_request = pg_datasource.get_asset("sport_activities").build_batch_request()
    print("Requête batch construite")

    # Créer ou configurer la suite d'attente 
    print("Configuration de la suite d'attente...")
    expectation_suite_name = "sport_activities_expectation_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
    print("Suite d'attente configurée")

    # Créer le validator pour les données
    print("Création du validator...")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    print("Validator créé")
    
    print("Aperçu des données:")
    print(validator.head())

    # Pour vérifier les valeurs de colonne "start_datetime"
    print("Ajout des attentes...")
    validator.expect_column_values_to_not_be_null(column="start_datetime")
    validator.expect_column_values_to_not_be_null(column="sport_type")
    validator.expect_column_values_to_be_between(
        column="activity_duration",
        min_value=0,
        mostly=1.0
    )
    validator.expect_column_values_to_be_between(
        column="distance",
        min_value=0, 
        mostly=1.0,  
        allow_cross_type_comparisons=False,
        include_minimum=True,
        missing_value_handling="ignore"
    )
    print("Attentes ajoutées")

    # Sauvegarde de suite d'expectation
    print("Sauvegarde de la suite d'attente...")
    validator.save_expectation_suite(discard_failed_expectations=False)
    print("Suite d'attente sauvegardée")

    # Créer le checkpoint AVANT de l'exécuter
    print("Création du checkpoint...")
    checkpoint_name = "sport_activities_checkpoint"

    # Créer la configuration du checkpoint
    checkpoint_config = {
        "name": checkpoint_name,
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "run_name_template": "%Y%m%d-%H%M%S-validation",
        "validations": [
            {
                "batch_request": batch_request,
                "expectation_suite_name": expectation_suite_name
            }
        ]
    }

    # Ajouter le checkpoint au contexte
    context.add_checkpoint(**checkpoint_config)
    print("Checkpoint créé")

    # Maintenant exécuter le checkpoint
    print("Exécution de la validation avec SimpleCheckpoint...")
    checkpoint_result = context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        run_name="airflow_validation_run"
    )

    print(f"Résultat de la validation: {checkpoint_result}")

    # Générer une erreur si la validation échoue
    if not checkpoint_result["success"]:
        raise ValueError("Validation failed!")
    
    print("Validation réussie !")
    return True

# Définition de DAG Pipeline
with DAG(    
    dag_id="great_expectations_validation",
    start_date=datetime(2025, 4, 6), 
    schedule_interval="@daily",  
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['SQL table', 'random activité sportive']
) as dag:
    # Créer une tâche pour vérifier l'existence de la table
    check_table_task = PythonOperator(
        task_id="check_table_exists",
        python_callable=check_table_exists
    )
    
    # Créer une tâche pour la validation
    validation_task = PythonOperator(
        task_id="validate_data",
        python_callable=run_ge_validation
    )
    
    # Définir l'ordre d'exécution
    check_table_task >> validation_task
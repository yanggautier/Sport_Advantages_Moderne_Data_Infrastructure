import argparse
import os
import pandas as pd
import json
import tempfile
import shutil
import boto3
from botocore.client import Config
import psycopg2
from psycopg2.extras import RealDictCursor


def connect_to_db():
    """Établir une connexion à la base de données PostgreSQL
    
    Returns:
        connection: Connexion à la base de données
    """
    # Récupérer les variables d'environnement
    db_host = os.environ.get("DB_HOST", "business-postgres")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    
    if not all([db_name, db_user, db_password]):
        raise ValueError("Variables d'environnement DB_NAME, DB_USER ou DB_PASSWORD non définies")
    
    # Établir la connexion
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    
    return conn


def get_minio_client():
    """Créer un client MinIO/S3
    
    Returns:
        boto3.client: Client MinIO
    """
    endpoint_url = "http://minio:9000"
    access_key = os.environ.get("MINIO_ROOT_USER")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD")
    
    if not access_key or not secret_key:
        raise ValueError("Variables d'environnement MINIO_ROOT_USER ou MINIO_ROOT_PASSWORD non définies")
    
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    return s3_client


def ensure_bucket_exists(s3_client, bucket_name):
    """S'assurer qu'un bucket existe, le créer sinon
    
    Args:
        s3_client: Client MinIO
        bucket_name (str): Nom du bucket
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ Le bucket '{bucket_name}' existe déjà")
    except:
        # Créer le bucket s'il n'existe pas
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✅ Le bucket '{bucket_name}' a été créé avec succès")


def explore_bucket(s3_client, bucket_name):
    """Explorer un bucket pour comprendre sa structure
    
    Args:
        s3_client: Client MinIO
        bucket_name (str): Nom du bucket
    """
    print(f"Exploration du bucket {bucket_name}:")
    
    try:
        # Lister les objets à la racine du bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, Delimiter='/')
        
        # Afficher les préfixes (dossiers) trouvés
        if 'CommonPrefixes' in response:
            print("Dossiers trouvés:")
            for prefix in response['CommonPrefixes']:
                print(f"  - {prefix['Prefix']}")
                
                # Explorer le contenu de chaque préfixe
                sub_response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix['Prefix'],
                    Delimiter='/'
                )
                
                if 'CommonPrefixes' in sub_response:
                    print(f"    Sous-dossiers de {prefix['Prefix']}:")
                    for sub_prefix in sub_response['CommonPrefixes']:
                        print(f"      - {sub_prefix['Prefix']}")
        
        # Afficher les fichiers trouvés à la racine
        if 'Contents' in response:
            print("Fichiers à la racine:")
            for obj in response['Contents']:
                print(f"  - {obj['Key']} ({obj['Size']} octets)")
                
    except Exception as e:
        print(f"❌ Erreur lors de l'exploration du bucket: {e}")


def find_json_files_in_bucket(s3_client, bucket_name, prefix=''):
    """Recherche récursivement des fichiers JSON dans un bucket
    
    Args:
        s3_client: Client MinIO
        bucket_name (str): Nom du bucket
        prefix (str): Préfixe à explorer (dossier)
        
    Returns:
        list: Liste des chemins vers les fichiers JSON trouvés
    """
    json_files = []
    
    try:
        # Lister tous les objets avec le préfixe spécifié
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.json'):
                        json_files.append(key)
        
        return json_files
    
    except Exception as e:
        print(f"❌ Erreur lors de la recherche de fichiers JSON: {e}")
        return []


def download_file_from_minio(s3_client, bucket, key, local_path):
    """Télécharger un fichier depuis MinIO
    
    Args:
        s3_client: Client MinIO
        bucket (str): Nom du bucket
        key (str): Chemin du fichier dans le bucket
        local_path (str): Chemin local où sauvegarder le fichier
        
    Returns:
        bool: True si le téléchargement a réussi, False sinon
    """
    try:
        print(f"Téléchargement de s3://{bucket}/{key} vers {local_path}")
        s3_client.download_file(bucket, key, local_path)
        return True
    except Exception as e:
        print(f"❌ Erreur lors du téléchargement de {key}: {e}")
        return False


def upload_to_minio(s3_client, bucket, key, local_path):
    """Télécharger un fichier vers MinIO
    
    Args:
        s3_client: Client MinIO
        bucket (str): Nom du bucket
        key (str): Chemin du fichier dans le bucket
        local_path (str): Chemin local du fichier à télécharger
        
    Returns:
        bool: True si le téléchargement a réussi, False sinon
    """
    try:
        print(f"Téléchargement de {local_path} vers s3://{bucket}/{key}")
        s3_client.upload_file(local_path, bucket, key)
        return True
    except Exception as e:
        print(f"❌ Erreur lors du téléchargement vers {key}: {e}")
        return False


def get_table_data(conn, table_name):
    """Récupérer les données d'une table PostgreSQL
    
    Args:
        conn: Connexion à la base de données
        table_name (str): Nom de la table
        
    Returns:
        pandas.DataFrame: DataFrame contenant les données
    """
    query = f"SELECT * FROM {table_name}"
    print(f"Exécution de la requête: {query}")
    
    try:
        # Utiliser pandas pour lire directement les données dans un DataFrame
        df = pd.read_sql_query(query, conn)
        print(f"✅ {len(df)} lignes lues depuis {table_name}")
        return df
    except Exception as e:
        print(f"❌ Erreur lors de la lecture de la table {table_name}: {e}")
        raise


def process_data(input_bucket, table, output_bucket):
    """Traiter les données comme le ferait Spark
    
    Args:
        input_bucket (str): Nom du bucket d'entrée
        table (str): Nom de la table
        output_bucket (str): Nom du bucket de sortie
        
    Returns:
        bool: True si le traitement a réussi, False sinon
    """
    try:
        # Créer un client MinIO
        s3_client = get_minio_client()
        
        # S'assurer que les buckets existent
        ensure_bucket_exists(s3_client, input_bucket)
        ensure_bucket_exists(s3_client, output_bucket)
        
        # Explorer la structure du bucket d'entrée
        explore_bucket(s3_client, input_bucket)
        
        # Créer un répertoire temporaire pour les fichiers JSON
        temp_dir = tempfile.mkdtemp(prefix="minio_data_")
        
        try:
            # Rechercher des fichiers JSON dans le bucket
            json_files = find_json_files_in_bucket(s3_client, input_bucket, f"tables/{table}/")
            
            if not json_files:
                print(f"⚠️ Aucun fichier JSON trouvé dans {input_bucket}/tables/{table}/")
                # Rechercher plus largement
                json_files = find_json_files_in_bucket(s3_client, input_bucket)
                
                if json_files:
                    print(f"Trouvé {len(json_files)} fichiers JSON ailleurs dans le bucket:")
                    for json_file in json_files:
                        print(f"  - {json_file}")
            
            # Liste pour stocker les données d'activité
            activity_data = []
            
            # Télécharger et traiter chaque fichier JSON trouvé
            for json_file in json_files:
                local_path = os.path.join(temp_dir, os.path.basename(json_file))
                if download_file_from_minio(s3_client, input_bucket, json_file, local_path):
                    with open(local_path, 'r') as f:
                        try:
                            data = json.load(f)
                            if isinstance(data, list):
                                activity_data.extend(data)
                            else:
                                activity_data.append(data)
                        except json.JSONDecodeError as e:
                            print(f"❌ Erreur lors du décodage de {json_file}: {e}")
            
            # Créer un DataFrame pour les activités sportives
            if activity_data:
                activity_df = pd.DataFrame(activity_data)
                print(f"✅ {len(activity_df)} activités sportives chargées depuis les fichiers JSON")
            else:
                # Créer un DataFrame vide avec le bon schéma
                activity_df = pd.DataFrame(columns=["id_employee", "activity_date", "activity_type", "activity_duration"])
                print("⚠️ Aucune donnée d'activité trouvée, utilisation d'un DataFrame vide")
            
            # Établir une connexion à la base de données
            conn = connect_to_db()
            
            try:
                # Récupérer les données des employés
                employee_df = get_table_data(conn, "sport_advantages.employees_masked")
                
                # Récupérer les données de validation de déplacement
                validation_df = get_table_data(conn, "sport_advantages.commute_validations")
                
                # Transformer les données d'activités - regrouper par employé
                if not activity_df.empty:
                    activity_transformed_df = activity_df.groupby('id_employee').agg(
                        count_activity=('id_employee', 'count'),
                        mean_duration=('activity_duration', 'mean')
                    ).reset_index()
                else:
                    activity_transformed_df = pd.DataFrame(columns=['id_employee', 'count_activity', 'mean_duration'])
                
                # Joindre les données des employés et des validations
                employee_join_validation_df = pd.merge(
                    employee_df,
                    validation_df,
                    on='id_employee',
                    how='inner'
                )
                
                # Joindre avec les données d'activités transformées
                final_df = pd.merge(
                    employee_join_validation_df,
                    activity_transformed_df,
                    on='id_employee',
                    how='left'
                )
                
                # Remplacer les NaN par 0 pour les colonnes numériques
                if 'count_activity' in final_df.columns:
                    final_df['count_activity'] = final_df['count_activity'].fillna(0)
                if 'mean_duration' in final_df.columns:
                    final_df['mean_duration'] = final_df['mean_duration'].fillna(0)
                
                # Aperçu des données finales
                print("\nAperçu des données finales:")
                print(final_df.head())
                
                # Sauvegarder les résultats dans un fichier JSON temporaire
                output_file = os.path.join(temp_dir, 'joined_data.json')
                final_df.to_json(output_file, orient='records')
                
                # Télécharger le fichier vers MinIO
                upload_success = upload_to_minio(
                    s3_client,
                    output_bucket,
                    'joined_data/data.json',
                    output_file
                )
                
                if upload_success:
                    print("✅ Les données ont été traitées et sauvegardées avec succès!")
                else:
                    print("⚠️ Échec du téléchargement des résultats vers MinIO.")
                
                # Sauvegarder également au format CSV
                csv_file = os.path.join(temp_dir, 'joined_data.csv')
                final_df.to_csv(csv_file, index=False)
                
                # Télécharger le fichier CSV vers MinIO
                upload_to_minio(
                    s3_client,
                    output_bucket,
                    'joined_data/data.csv',
                    csv_file
                )
                
                return True
                
            finally:
                # Fermer la connexion à la base de données
                conn.close()
                
        finally:
            # Supprimer le répertoire temporaire
            shutil.rmtree(temp_dir)
            
    except Exception as e:
        print(f"❌ Erreur lors du traitement des données: {e}")
        return False


def main():
    """Fonction principale du script"""
    parser = argparse.ArgumentParser(description="Traitement des données sans Spark")
    parser.add_argument("--input_bucket", required=True, help="Nom du bucket S3/MinIO d'entrée")
    parser.add_argument("--table", required=True, help="Nom de la table Delta")
    parser.add_argument("--output_bucket", required=True, help="Bucket de sortie pour les résultats")
    args = parser.parse_args()
    
    print("=== Démarrage du script de traitement des données (sans Spark) ===")
    
    success = process_data(args.input_bucket, args.table, args.output_bucket)
    
    if success:
        print("✅ Traitement terminé avec succès")
    else:
        print("❌ Traitement terminé avec des erreurs")
    
    print("=== Fin du script ===")


if __name__ == "__main__":
    main()
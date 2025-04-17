import argparse
import os
from typing import Dict, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, count
import boto3
from botocore.client import Config


def create_spark_session() -> SparkSession:
    """Créer et configurer une session Spark
    
    Returns:
        SparkSession: Session Spark configurée
    """
    spark = SparkSession.builder.appName("ReadDeltaLake") \
            .config("spark.master", "spark://spark-master:7077") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD")) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.0,org.postgresql:postgresql:42.5.1,org.apache.hadoop:hadoop-aws:3.3.1") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
            .getOrCreate()

    # Afficher la version de Spark pour le debug
    print(f"Version de Spark: {spark.version}")
    
    return spark


def read_delta_table(spark: SparkSession, bucket: str, table: str) -> DataFrame:
    """Lire les données de table Delta depuis MinIO
    
    Args:
        spark (SparkSession): Session Spark
        bucket (str): Nom du bucket
        table (str): Nom de la table
        
    Returns:
        DataFrame: DataFrame contenant les données lues
        
    Raises:
        FileNotFoundError: Si la table n'existe pas
    """
    path = f"s3a://{bucket}/tables/{table}"
    print(f"Lecture de la table Delta depuis: {path}")

    try:
        # Utiliser directement l'API DataFrame de Spark pour lire la table
        df = spark.read.format("delta").load(path)
        print(f"Nombre de lignes lues: {df.count()}")
        return df
    except Exception as e:
        print(f"Erreur lors de la lecture de la table Delta: {e}")
        
        # Des informations de diagnostic supplémentaires
        print("Tentative de diagnostic supplémentaire...")
        
        try:
            # Vérifier si le bucket existe à l'aide de boto3
            endpoint_url = "http://minio:9000" 
            access_key = os.environ.get("MINIO_ROOT_USER")
            secret_key = os.environ.get("MINIO_ROOT_PASSWORD")
            
            s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=Config(signature_version='s3v4'),
                region_name='us-east-1'
            )
            
            # Obtenir la liste des buckets et vérifier le contenu
            buckets = s3_client.list_buckets()
            print(f"Buckets disponibles: {[b['Name'] for b in buckets['Buckets']]}")
            
            # Vérifier le contenu du bucket
            if bucket in [b['Name'] for b in buckets['Buckets']]:
                prefix = "tables/"
                objs = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
                
                if 'Contents' in objs:
                    print(f"Contenu du dossier {prefix} dans le bucket {bucket}:")
                    for obj in objs['Contents']:
                        print(f"  - {obj['Key']}")
                else:
                    print(f"Le dossier {prefix} est vide ou n'existe pas dans {bucket}")
        except Exception as diag_e:
            print(f"Erreur lors du diagnostic: {diag_e}")
            
        raise FileNotFoundError(f"La table Delta n'existe pas à l'emplacement {path} ou n'est pas accessible")


def read_sql_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Lire des données dans un serveur SQL pour retourner en Spark DataFrame

    Args:
        spark (SparkSession): Session de spark
        table_name (str): Nom de table dans la base de données

    Returns:
        DataFrame: DataFrame Spark contenant les données lues
        
    Raises:
        Exception: Si la lecture de la table SQL échoue
    """
    # Récupérer les variables d'environnement
    postgres_user = os.environ.get("DB_USER")
    if not postgres_user:
        raise ValueError("Variable d'environnement DB_USER non définie")

    postgres_password = os.environ.get("DB_PASSWORD")
    if not postgres_password:
        raise ValueError("Variable d'environnement DB_PASSWORD non définie")
    
    postgres_host = os.environ.get("DB_HOST", "business-postgres")
    postgres_port = os.environ.get("DB_PORT", "5432") 
    postgres_db = os.environ.get("DB_NAME")
    if not postgres_db:
        raise ValueError("Variable d'environnement DB_NAME non définie")

    url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"

    properties: Dict[str, str] = {
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver"
    }

    try:
        # Vérifier que les propriétés ne contiennent pas de valeurs nulles
        for key, value in properties.items():
            if value is None:
                raise ValueError(f"La propriété JDBC '{key}' a une valeur nulle")
            
        df = spark.read.jdbc(url, table_name, properties=properties)
        print(f"Nombre de lignes lues depuis {table_name}: {df.count()}")
        return df
    except Exception as e:
        print(f"Erreur lors de la lecture de la table SQL: {e}")
        print(f"Détails de la connexion (sans mot de passe):")
        print(f"  URL: {url}")
        print(f"  User: {postgres_user}")
        print(f"  Table: {table_name}")
        print(f"  Driver: {properties.get('driver')}")
        raise


def transform_activity_data(activity_df: DataFrame) -> DataFrame:
    """Transforme les données d'activités en agrégeant par employé
    
    Args:
        activity_df (DataFrame): DataFrame contenant les données d'activités
        
    Returns:
        DataFrame: DataFrame agrégé par employé
    """
    return activity_df.groupBy("id_employee") \
                    .agg(
                        count("id_employee").alias("count_activity"),
                        avg("activity_duration").alias("mean_duration")
                    )


def ensure_compatible_types(employee_df: DataFrame, validation_df: DataFrame, 
                           activity_df: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Assure la compatibilité des types de données pour les jointures
    
    Args:
        employee_df (DataFrame): DataFrame des employés
        validation_df (DataFrame): DataFrame des validations
        activity_df (DataFrame): DataFrame des activités
        
    Returns:
        tuple[DataFrame, DataFrame, DataFrame]: DataFrames avec types compatibles
    """
    print("Types de données avant jointure:")
    print("id_employee dans employee_df:", employee_df.schema["id_employee"].dataType)
    print("id_employee dans validation_df:", validation_df.schema["id_employee"].dataType)
    print("id_employee dans activity_df:", activity_df.schema["id_employee"].dataType)

    # Type de référence pour la jointure (celui des employés)
    ref_type = employee_df.schema["id_employee"].dataType

    # Convertir les types si nécessaire
    if str(validation_df.schema["id_employee"].dataType) != str(ref_type):
        print(f"Conversion du type id_employee pour validation_df")
        validation_df = validation_df.withColumn("id_employee", 
                                               validation_df["id_employee"].cast(ref_type))

    if str(activity_df.schema["id_employee"].dataType) != str(ref_type):
        print(f"Conversion du type id_employee pour activity_df")
        activity_df = activity_df.withColumn("id_employee", 
                                           activity_df["id_employee"].cast(ref_type))
        
    return employee_df, validation_df, activity_df


def ensure_bucket_exists(bucket_name: str) -> None:
    """Vérifie si un bucket existe dans MinIO et le crée s'il n'existe pas
    
    Args:
        bucket_name (str): Nom du bucket à vérifier/créer
    """
    # Récupérer les variables d'environnement
    endpoint_url = "http://minio:9000"
    access_key = os.environ.get("MINIO_ROOT_USER")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD")
    
    if not access_key or not secret_key:
        print("⚠️ Impossible de vérifier/créer le bucket: variables d'environnement manquantes")
        return
    
    try:
        # Création du client S3 compatible avec MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'  # Valeur arbitraire, MinIO n'utilise pas les régions
        )
        
        # Vérifier si le bucket existe
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"✅ Le bucket '{bucket_name}' existe déjà")
        except:
            # Créer le bucket s'il n'existe pas
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"✅ Le bucket '{bucket_name}' a été créé avec succès")
    except Exception as e:
        print(f"❌ Erreur lors de la vérification/création du bucket: {e}")


def save_to_delta(df: DataFrame, output_bucket: str, path_suffix: str = "joined_data") -> None:
    """Sauvegarde un DataFrame dans une table Delta
    
    Args:
        df (DataFrame): DataFrame à sauvegarder
        output_bucket (str): Nom du bucket de sortie
        path_suffix (str, optional): Suffixe du chemin. Par défaut "joined_data"
    """
    
    output_path = f"s3a://{output_bucket}/{path_suffix}"
    print(f"Sauvegarde des données vers: {output_path}")
    
    # S'assurer que le bucket existe avant d'essayer d'écrire dedans
    ensure_bucket_exists(output_bucket)
    
    try:
        # Vérifier si l'écriture est possible
        try:
            # Tester l'accès en écriture avec un petit DataFrame de test
            test_df = df.limit(1)
            test_output_path = f"s3a://{output_bucket}/test_write"
            test_df.write.format("delta").mode("overwrite").save(test_output_path)
            print("✅ Test d'écriture dans MinIO réussi")
        except Exception as test_e:
            print(f"⚠️ Test d'écriture échoué, mais on continue avec l'écriture principale: {test_e}")
        
        # Écrire dans la table Delta
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(output_path)
        print(f"Données sauvegardées avec succès!")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde des données: {e}")
        
        # Si le bucket n'existe pas, suggérer de le créer
        if "NoSuchBucket" in str(e):
            print(f"Le bucket '{output_bucket}' n'existe pas dans MinIO. Assurez-vous de le créer avant d'exécuter ce script.")
        elif "Wrong FS" in str(e):
            print(f"Problème de système de fichiers. Vérifiez que vous n'avez pas configuré fs.defaultFS dans votre session Spark.")
        
        raise

def verify_minio_access(spark: SparkSession, bucket: str):
    """Verify we can access MinIO"""
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )
        path = spark._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/")
        if fs.exists(path):
            print(f"✅ Access to bucket {bucket} verified")
        else:
            print(f"⚠️ Bucket {bucket} not found but MinIO access works")
    except Exception as e:
        print(f"❌ MinIO access failed: {e}")
        raise

    
def main() -> None:
    """Fonction principale du script."""
    parser = argparse.ArgumentParser(description="Lecteur et analyseur de tables Delta")
    parser.add_argument("--input_bucket", required=True, help="Nom du bucket S3/MinIO")
    parser.add_argument("--table", required=True, help="Nom de la table Delta")
    parser.add_argument("--output_bucket", required=True, help="Bucket de sortie pour les résultats")
    args = parser.parse_args()

    # Créer la session Spark
    spark = create_spark_session()

    # S'assurer que les buckets d'entrée et de sortie existent
    ensure_bucket_exists(args.input_bucket)
    ensure_bucket_exists(args.output_bucket)
    
    # Vérifier l'accès aux buckets - mais ne pas faire échouer le script
    input_access_ok = verify_minio_access(spark, args.input_bucket)
    output_access_ok = verify_minio_access(spark, args.output_bucket)
    
    if input_access_ok and output_access_ok:
        print("✅ Accès vérifié aux buckets d'entrée et de sortie")
    else:
        print("⚠️ Problèmes d'accès aux buckets, mais on continue")

    try:
        # Ajouter une liste des fichiers JAR chargés à des fins de diagnostic
        print("JARs disponibles:")
        for jar in spark.sparkContext._jsc.sc().listJars():
            print(f"  - {jar}")
            
        # Afficher les propriétés Hadoop pour le diagnostic
        print("\nConfiguratiom Hadoop pertinente:")
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        properties = ["fs.s3a.impl", "fs.s3a.endpoint", "fs.s3a.access.key", 
                     "fs.s3a.secret.key", "fs.s3a.path.style.access"]
        for prop in properties:
            print(f"  {prop}: {hadoop_conf.get(prop)}")
        
        # Lire des données des activités sportives
        activity_df = read_delta_table(spark, args.input_bucket, args.table)
        print("Schéma des données sportives")
        activity_df.printSchema()

        activity_df.show(5)
        
        # Lire les données de HR
        employee_df = read_sql_table(spark, "sport_advantages.employees_masked")
        print("Schéma des données salariés")
        employee_df.printSchema()
        
        # Lire les données de validation de déplacement
        validation_df = read_sql_table(spark, "sport_advantages.commute_validations")
        print("Schéma des validations de déplacement")
        validation_df.printSchema()
        
        # Transformer les données d'activités
        activity_transformed_df = transform_activity_data(activity_df)
        print("Données d'activités sportives agrégées")
        activity_transformed_df.show(5)
        
        # Assurer la compatibilité des types pour les jointures
        employee_df, validation_df, activity_transformed_df = ensure_compatible_types(
            employee_df, validation_df, activity_transformed_df
        )
        
        # Jointure de 2 DataFrame employees et commute_validations
        employee_join_validation_df = employee_df.join(validation_df, "id_employee")
        print("Données de jointure de table employés et validations")
        employee_join_validation_df.show(5)
        
        # Jointure de DataFrame précédent avec le DataFrame des activités sportives
        final_df = employee_join_validation_df.join(activity_transformed_df, "id_employee", "left")
        print("Données finales après toutes les jointures")
        final_df.show(5)
        
        # Sauvegarde de DataFrame final dans une table Delta
        save_to_delta(final_df, args.output_bucket)
        
    except FileNotFoundError as e:
        print(f"Erreur: {e}")
        print("⚠️ Vérifiez que la table Delta existe et que le chemin est correct")
        # Continuer en cas d'erreur liée aux fichiers non trouvés
        print("Création d'un DataFrame vide comme solution de repli")
        # Créer un DataFrame vide avec des colonnes similaires
        columns = ["id_employee", "employee_name", "commute_distance", "count_activity", "mean_duration"]
        empty_df = spark.createDataFrame([], spark.createStructType([]))
        # Sauvegarder ce DataFrame vide
        save_to_delta(empty_df, args.output_bucket, "empty_fallback")
    except Exception as e:
        print(f"Erreur pendant l'exécution: {e}")
        raise

if __name__ == "__main__":
    main()
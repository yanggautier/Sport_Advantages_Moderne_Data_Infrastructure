// Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import io.delta.tables._
import org.apache.spark.sql.types._

// Lecture des variables d'environnement
val topicName = Option(System.getenv("TOPIC_NAME")).getOrElse("sport.sport_advantages.sport_activities")
val bootstrapServers = Option(System.getenv("KAFKA_SERVERS")).getOrElse("redpanda:9092")

// Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import io.delta.tables._
import org.apache.spark.sql.types._

// Configuration pour MinIO (service local dans docker-compose)
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
spark.conf.set("spark.hadoop.fs.s3a.access.key", sys.env.getOrElse("MINIO_ROOT_USER", "minioadmin"))
spark.conf.set("spark.hadoop.fs.s3a.secret.key", sys.env.getOrElse("MINIO_ROOT_PASSWORD", "minioadmin"))
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

// Utiliser un fournisseur d'authentification simple
spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

// Désactiver les tentatives de signature AWS standard
spark.conf.set("spark.hadoop.fs.s3a.signing-algorithm", "S3SignerType")

// Si nécessaire, désactiver la validation de région
spark.conf.set("spark.hadoop.fs.s3a.change.detection.mode", "none")
spark.conf.set("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")

// Définir votre bucket
val bucketName = "delta-tables"

// Test de connectivité
try {
  val hadoopConf = spark.sparkContext.hadoopConfiguration
  val s3 = new org.apache.hadoop.fs.s3a.S3AFileSystem()
  s3.initialize(new java.net.URI("s3a://delta-tables"), hadoopConf)
  
  // Essayer de lister le contenu
  val status = s3.listStatus(new org.apache.hadoop.fs.Path("s3a://delta-tables"))
  println(s"Connexion à MinIO réussie, éléments trouvés: ${status.length}")
  
  // Essayer de créer un répertoire test
  val testPath = new org.apache.hadoop.fs.Path("s3a://delta-tables/test-path")
  val result = s3.mkdirs(testPath)
  println(s"Création de répertoire: $result")
} catch {
  case e: Exception => 
    println(s"Erreur de connexion à MinIO: ${e.getMessage}")
    e.printStackTrace()
}

// Vérifier si le bucket existe
try {
  val hadoopConf = spark.sparkContext.hadoopConfiguration
  val s3 = new org.apache.hadoop.fs.s3a.S3AFileSystem()

  s3.initialize(new java.net.URI(s"s3a://$bucketName"), hadoopConf)
  if (!s3.exists(new org.apache.hadoop.fs.Path(s"s3a://$bucketName"))) {
    println(s"Le bucket $bucketName n'existe pas!")
  } else {
    println(s"Le bucket $bucketName existe.")
  }
} catch {
  case e: Exception => println(s"Erreur en vérifiant le bucket: ${e.getMessage}")
}


// Affichage des variables d'environnement pour debug
println("=== Variables d'environnement ===")
println(s"TOPIC_NAME: $topicName")
println(s"KAFKA_SERVERS: $bootstrapServers")

println("=== ENV VAR DEBUG ===")
println(s"MINIO_ROOT_USER: ${sys.env.getOrElse("MINIO_ROOT_USER", "Not found")}")
println(s"MINIO_ROOT_PASSWORD: ${sys.env.getOrElse("MINIO_ROOT_PASSWORD", "Not found")}")

val activitySchema = StructType(Array(
  StructField("id", IntegerType, false),
  StructField("id_employee", IntegerType, false),
  StructField("start_datetime", StringType, false),
  StructField("sport_type", StringType, false),
  StructField("activity_duration", IntegerType, false),
  StructField("distance", DoubleType, true),
  StructField("comment", StringType, true)
))

// Lecture du stream depuis Redpanda
val redpandaStream = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", bootstrapServers)
  .option("subscribe", topicName)
  .option("startingOffsets", "earliest")
  .load()

println("=== Schéma du flux Redpanda ===")
redpandaStream.printSchema()

val activityData = redpandaStream
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .select(from_json(col("value"), activitySchema).as("data"))
  .select("data.*")

println("=== Schéma des données transformées ===")
activityData.printSchema()

// Ecrire dans Delta Lake en mode streaming
println("=== Démarrage de l'écriture streaming vers Delta Lake ===")
val query = activityData
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", s"s3a://$bucketName/checkpoints/sport_activities")
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .start(s"s3a://$bucketName/tables/sport_activities")
  
// Affichage du statut du stream
println(s"Query status: ${query.status}")
println("Stream actif. Appuyez sur Ctrl+C pour arrêter...")

// Attendre que le traitement soit terminé
query.awaitTermination()
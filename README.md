


Connection à la base de données
`psql -h localhost -p 5432 -U [Username] -d [DBname]`



- PostgreSQL pour les données métier
- Debezium pour le CDC (Change Data Capture)
- Redpanda comme alternative à Kafka
- Spark pour le traitement des données
- Delta Lake pour le stockage en couche delta
- MinIO comme stockage compatible S3
- Airflow pour l'orchestration



Dû à la taille du fichier `trino-delta-lake-413.zip` (283Mo), il faut télécharger sur `https://repo1-maven-org.maven-cache.lunarclientcdn.com/maven2/io/trino/trino-delta-lake/413/` et le mettre dans  .trino/plugins/
"""Lit le topic random_names en continu et écrit dans Cassandra."""
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, trim
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("random_names_stream")

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "random_names"
CHECKPOINT = "/tmp/checkpoints/random_names"

# Le contrat : mêmes champs que le producteur et la table Cassandra
SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("full_name", StringType()),
    StructField("gender", StringType()),
    StructField("address", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("postcode", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("email", StringType()),
    StructField("ingested_at", StringType()),
])


def main() -> None:
    # Pas de try/except muet : si ça casse, on veut voir la vraie erreur (#12)
    spark = (
        SparkSession.builder.appName("RandomNamesToCassandra")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")   # 1er lancement seulement ; ensuite le checkpoint décide
        .option("failOnDataLoss", "false")
        .load()
    )

    people = (
        raw.select(from_json(col("value").cast("string"), SCHEMA).alias("p"))
        .select("p.*")
        .filter(col("id").isNotNull())                       # écarte les messages illisibles
        .withColumn("full_name", trim(col("full_name")))
        .withColumn("ingested_at", to_timestamp(col("ingested_at")))
    )

    query = (
        people.writeStream.format("org.apache.spark.sql.cassandra")
        .option("keyspace", "spark_streaming")
        .option("table", "random_names")
        .option("checkpointLocation", CHECKPOINT)            # reprise sans tout relire (#11)
        .outputMode("append")
        .start()
    )
    log.info("Streaming démarré : %s -> spark_streaming.random_names", TOPIC)
    query.awaitTermination()


if __name__ == "__main__":
    main()
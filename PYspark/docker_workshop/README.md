# Annuaire Express — Pipeline streaming Kafka → Spark → Cassandra

Toutes les 5 minutes, Airflow récupère de faux profils sur l'API randomuser.me et les publie dans Kafka. Spark Structured Streaming les lit en continu et les écrit dans Cassandra.

```
API randomuser.me → Airflow (DAG /5 min) → Kafka (topic random_names) → Spark Streaming → Cassandra
```

## Stack

| Service | Image | Port local |
| --- | --- | --- |
| Kafka (KRaft, sans ZooKeeper) | `apache/kafka:4.1.0` | 9094 |
| Kafka UI | `ghcr.io/kafbat/kafka-ui:v1.3.0` | 8888 |
| Cassandra | `cassandra:5.0` | 9042 |
| Spark | `apache/spark:3.5.5-scala2.12-java17-python3-ubuntu` | — |
| Airflow | `apache/airflow:3.1.0` | 8080 |

## Prérequis

- Docker Desktop **démarré**, avec au moins 6 Go de RAM.
- Ports 8080, 8888, 9042 et 9094 libres.
- Python 3.10+, uniquement pour le test du producteur depuis le PC.

> **PowerShell** : les commandes ci-dessous tiennent sur une seule ligne. Pour couper une ligne, utilise l'accent grave `` ` `` (pas `'` ni `\`).

## Structure

```
annuaire-express/
├── docker-compose.yml        # les 7 services
├── cassandra/init.cql        # keyspace + table (exécuté au démarrage)
├── dags/
│   ├── stream_to_kafka.py    # producteur Kafka
│   └── random_people_dag.py  # DAG Airflow
└── spark/
    └── spark_streaming.py    # Kafka → Cassandra
```

## Étapes

### 0. Créer les dossiers

```powershell
mkdir annuaire-express\dags, annuaire-express\spark, annuaire-express\cassandra
cd annuaire-express
```

### 1. Écrire `cassandra/init.cql` AVANT de lancer Docker

Le fichier ne doit **pas** être vide : sans lui, Docker crée un dossier `init.cql` et l'initialisation échoue.

```sql
CREATE KEYSPACE IF NOT EXISTS spark_streaming
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS spark_streaming.random_names (
  id uuid PRIMARY KEY, full_name text, gender text, address text,
  city text, country text, postcode text, latitude double,
  longitude double, email text, ingested_at timestamp
);
```

### 2. Lancer l'infrastructure

```powershell
docker compose up -d
docker compose ps
```

✅ `kafka` et `cassandra` sont *healthy*, `kafka-init` et `cassandra-init` sont *exited (0)*. Le topic `random_names` est visible sur http://localhost:8888.

```powershell
docker compose exec cassandra cqlsh -e "DESCRIBE TABLE spark_streaming.random_names;"
```

### 3. Tester le producteur depuis le PC (optionnel)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install "kafka-python>=2.1" requests
python dags\stream_to_kafka.py
```

Si l'activation du venv est refusée : `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`.

✅ 6 lignes « Envoyé … » en 30 secondes. Pour les relire depuis Kafka :

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic random_names --from-beginning --max-messages 3
```

### 4. Activer le DAG Airflow

1. Ouvre http://localhost:8080 (pas de login en local).
2. Active `random_people_names`, puis clique sur **Trigger**.
3. Suis les logs de la tâche `kafka_data_stream`.

✅ La tâche passe au vert après environ 2 minutes. Chaque run envoie 12 profils.

### 5. Lancer le streaming Spark

```powershell
docker compose exec spark /opt/spark/bin/spark-submit --master "local[2]" --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 /opt/app/spark_streaming.py
```

✅ Le log affiche `Streaming démarré`. Le terminal reste occupé, c'est normal : ouvre un second terminal pour la suite.

### 6. Vérifier de bout en bout

```powershell
# Nombre de lignes dans Cassandra
docker compose exec cassandra cqlsh -e "SELECT count(*) FROM spark_streaming.random_names;"

# Aperçu des profils
docker compose exec cassandra cqlsh -e "SELECT full_name, city, country, ingested_at FROM spark_streaming.random_names LIMIT 5;"

# Nombre de messages dans Kafka (somme des 3 partitions)
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic random_names
```

✅ Le `count(*)` de Cassandra est égal à la somme des offsets Kafka, et il augmente de 12 toutes les 5 minutes.

## Arrêter / nettoyer

```powershell
docker compose down        # arrête tout, garde les données Cassandra
docker compose down -v     # arrête tout et supprime les données
deactivate                 # quitte le venv
```

## Dépannage express

| Symptôme | Remède |
| --- | --- |
| `failed to connect to the docker API … dockerDesktopLinuxEngine` | Démarre Docker Desktop et attends « Engine running » |
| `Argument manquant` / `Expression manquante après --` | Commande multiligne bash dans PowerShell : tout sur une ligne, ou accent grave `` ` `` |
| `python stream_to_kafka.py` ne fait rien | Fichier vide ou bloc `if __name__ == "__main__":` manquant |
| `NoBrokersAvailable` | Kafka pas *healthy*. Depuis le PC, utilise `localhost:9094` ; dans Docker, `kafka:9092` |
| DAG absent dans Airflow | Erreur d'import : bandeau rouge dans l'interface, ou `docker compose logs airflow` |
| `Failed to find data source: kafka` | `--packages` manquant dans `spark-submit` |
| `NoSuchMethodError` / `ClassNotFoundException` | JAR `_2.12` et `_2.13` mélangés : tout en `_2.12` |
| `basedir must be absolute: ?/.ivy2` | Ajoute `--conf spark.jars.ivy=/tmp/.ivy2` |
| `cassandra-init` en erreur | `init.cql` créé comme dossier : supprime-le, écris le fichier, puis `docker compose up -d cassandra-init` |
| Conteneurs qui redémarrent en boucle | Pas assez de RAM pour Docker (6 Go minimum) |

## Corrections clés par rapport à l'article d'origine

- **Un seul `docker-compose.yml`** : réseau commun, service Spark explicite, Airflow inclus.
- **Kafka 4 en KRaft** : ZooKeeper a été supprimé de Kafka 4.0.
- **Producteur** : un nouvel appel API à chaque message (avant, le même profil était envoyé 12 fois), `flush()` à la fin, `postcode` en texte.
- **Cassandra** : clé primaire `id` (UUID) au lieu de `full_name`, qui écrasait les homonymes.
- **Airflow 3** : `schedule=` au lieu de `schedule_interval`, import depuis `airflow.providers.standard`.
- **Spark** : `--packages` avec des versions alignées (Scala 2.12, Spark 3.5.5), et un `checkpointLocation`.

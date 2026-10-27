CREATE KEYSPACE IF NOT EXISTS spark_streaming
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS spark_streaming.random_names (
  id          uuid PRIMARY KEY,   -- unique par profil (avant : full_name)
  full_name   text,
  gender      text,
  address     text,               -- avant : location
  city        text,
  country     text,
  postcode    text,               -- avant : int, plantait sur "SW1A 1AA"
  latitude    double,
  longitude   double,
  email       text,
  

);
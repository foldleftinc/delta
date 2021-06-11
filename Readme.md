
# About
## Testing Bronze -> Silver streaming architecture with delta lake 
Postgres -> Debezium (KConnect) -> Kafka -> Spark Streaming -> Delta Table (Bronze) -> Spark Streaming -> Delta Silver


# Pre-requisite
```
java
docker
scala
sbt
spark
```

# Compile & Package
```bash
sbt universal:packageZipTarball
```

# Running
```bash
docker compose up
```

# Upload KConnect
```bash
cd conf
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @debezium-cdc-postgres-source.json
```

# Start spark
```bash
spark-submit --class au.com.aeonsoftware.App \
    --packages io.delta:delta-core_2.12:1.0.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    ./target/scala-2.12/au-com-aeonsoftware-delta-sample_2.12-0.1.jar
```
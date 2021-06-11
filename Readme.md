curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @debezium-cdc-postgres-source.json

spark-submit --class au.com.aeonsoftware.App ./target/scala-2.12/au-com-aeonsoftware-delta-sample_2.12-0.1.jar
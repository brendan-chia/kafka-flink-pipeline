#!/bin/bash
mkdir -p jars
cd jars

echo "Downloading Flink Kafka connector..."
curl -L -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/2.2.1/flink-sql-connector-kafka-2.2.1.jar

echo "Downloading Flink JDBC connector..."
curl -L -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar

echo "Downloading PostgreSQL driver..."
curl -L -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

echo "Done:"
ls -lh *.jar

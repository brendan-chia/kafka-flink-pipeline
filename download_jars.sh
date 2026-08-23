#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_DIR="$SCRIPT_DIR/jars"
PROMETHEUS_PLUGIN_DIR="$SCRIPT_DIR/plugins/prometheus"

mkdir -p "$JAR_DIR" "$PROMETHEUS_PLUGIN_DIR"

download() {
  local url="$1"
  local output="$2"
  echo "Downloading $(basename "$output")..."
  curl --fail --location --output "$output" "$url"
}

download \
  "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar" \
  "$JAR_DIR/flink-sql-connector-kafka-4.0.1-2.0.jar"
download \
  "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc-core/4.0.0-2.0/flink-connector-jdbc-core-4.0.0-2.0.jar" \
  "$JAR_DIR/flink-connector-jdbc-core-4.0.0-2.0.jar"
download \
  "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc-postgres/4.0.0-2.0/flink-connector-jdbc-postgres-4.0.0-2.0.jar" \
  "$JAR_DIR/flink-connector-jdbc-postgres-4.0.0-2.0.jar"
download \
  "https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar" \
  "$JAR_DIR/postgresql-42.6.0.jar"
download \
  "https://repo.maven.apache.org/maven2/org/apache/flink/flink-metrics-prometheus/2.2.1/flink-metrics-prometheus-2.2.1.jar" \
  "$PROMETHEUS_PLUGIN_DIR/flink-metrics-prometheus-2.2.1.jar"

echo "Connector JARs:"
ls -lh "$JAR_DIR"/*.jar
echo "Flink metric reporter plugin:"
ls -lh "$PROMETHEUS_PLUGIN_DIR"/*.jar

#!/usr/bin/env bash

set -euo pipefail

# Detect Spark version from Bitnami image
SPARK_IMAGE="docker.io/bitnami/spark:3"
echo "Detecting Spark version from $SPARK_IMAGE ..."
SPARK_VERSION=$(docker run --rm "$SPARK_IMAGE" spark-submit --version 2>&1 | awk '/version/ && /Spark/{print $5; exit}')

if [[ -z "${SPARK_VERSION:-}" ]]; then
  echo "Failed to detect Spark version. Set SPARK_VERSION manually, e.g.:"
  echo "  SPARK_VERSION=3.5.1 ./get_spark_kafka_jars.sh"
  exit 1
fi

echo "Using Spark version: $SPARK_VERSION"

JARS_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$JARS_DIR"

echo "Downloading Spark-Kafka connector JARs into $JARS_DIR"

curl -fLO "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"
curl -fLO "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"

# Kafka clients version aligned with Kafka 3.7 image
KAFKA_CLIENT_VERSION="3.7.0"
curl -fLO "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar"

echo "Done. Files in $JARS_DIR:"
ls -1




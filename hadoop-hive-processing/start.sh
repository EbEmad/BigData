#!/bin/bash
echo "Starting the Cloudera CDP Data Lake Simulation..."
docker-compose up -d
echo "Services are starting. Please wait 1-2 minutes for the Hive Metastore to become fully healthy."
echo "Access the UIs at:"
echo " - NiFi: http://localhost:8443/nifi"
echo " - HDFS: http://localhost:50070"
echo " - Spark (Jupyter): http://localhost:8888"
echo " - Spark Master UI: http://localhost:8081"
echo " - Trino: http://localhost:8085"

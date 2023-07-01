#!/bin/bash
set -e

KAFKA_TOPICS="raw-data-custom.json raw-data-custom2.json raw-data-basic.json raw-data-basic2.json custom-transaction.json custom-transaction2.json basic-transaction.json basic-transaction2.json indicator.json"

for TOPIC in $KAFKA_TOPICS; do
  kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic $TOPIC
done

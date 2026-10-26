#!/usr/bin/env bash
# One-command setup: dependencies + Kafka, then starts the producer.
set -euo pipefail
cd "$(dirname "$0")"

docker compose up -d --wait

echo "Kafka is up on localhost:9092. Producing to topic 'sensors' (Ctrl+C to stop)..."
exec ./run.sh produce_messages.py

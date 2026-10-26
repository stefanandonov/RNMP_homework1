#!/usr/bin/env bash
# Stops Kafka and removes its data.
cd "$(dirname "$0")"
docker compose down -v

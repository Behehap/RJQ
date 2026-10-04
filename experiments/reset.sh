#!/bin/bash
# reset.sh — Wipes jobs and Prometheus data. Grafana stays.

set -e

echo "Stopping app and Prometheus..."
docker compose stop rjq prometheus

echo "Truncating jobs table..."
docker compose exec postgres psql -U rjq -d rjq -c "TRUNCATE jobs;"

echo "Recreating Prometheus container (clears its data)..."
docker compose rm -f prometheus
docker compose up -d prometheus

echo "Restarting the app..."
docker compose up -d rjq

echo "Done. Grafana is untouched."
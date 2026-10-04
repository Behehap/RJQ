#!/bin/bash
# super-urgent-only.sh — Submits a large batch of super-urgent jobs
# to an idle system and measures how many complete immediately
# versus how many are delayed.
#
# Usage:
#   ./super-urgent-only.sh [count] [parallelism]
#
# Example:
#   ./super-urgent-only.sh 10000 50

set -e

COUNT=${1:-10000}
PARALLEL=${2:-50}
API="http://localhost:8080/jobs"

echo "=============================================="
echo "  RJQ Super-Urgent Only Test"
echo "  Count: $COUNT   Parallelism: $PARALLEL"
echo "=============================================="
echo ""

# Make sure the system is idle before we start.
echo "Waiting for the system to drain..."
while true; do
  STATS=$(curl -s http://localhost:8080/stats)
  PENDING=$(echo "$STATS" | grep -o '"pending":[0-9]*' | cut -d: -f2)
  PROCESSING=$(echo "$STATS" | grep -o '"processing":[0-9]*' | cut -d: -f2)
  if [ "$PENDING" = "0" ] && [ "$PROCESSING" = "0" ]; then
    echo "  System idle."
    break
  fi
  echo "  pending=$PENDING processing=$PROCESSING"
  sleep 2
done

echo ""

# Note the start time so we can compute the elapsed window later.
START_TIME=$(date +%s)
echo "Start time: $(date -d @$START_TIME '+%H:%M:%S')"
echo ""

# Submit COUNT super-urgent jobs in parallel.
echo "Submitting $COUNT super-urgent jobs..."
seq 1 $COUNT | xargs -P $PARALLEL -I {} curl -s -X POST "$API" \
  -H "Content-Type: application/json" \
  -d '{"job_type":"email","payload":{"to":"super-{}@test.com","subject":"SUPER URGENT {}","body":"Body {}"},"queue":"priority","priority":3}' \
  > /dev/null

END_SUBMIT=$(date +%s)
echo "  Submitted in $((END_SUBMIT - START_TIME))s"
echo ""

# Poll every 5 seconds and record the state.
echo "Monitoring queue drain (polling every 5s)..."
echo ""

LAST_PENDING=0
DRAIN_START=""
while true; do
  STATS=$(curl -s http://localhost:8080/stats)
  PENDING=$(echo "$STATS" | grep -o '"pending":[0-9]*' | cut -d: -f2)
  PROCESSING=$(echo "$STATS" | grep -o '"processing":[0-9]*' | cut -d: -f2)
  NOW=$(date +%s)
  ELAPSED=$((NOW - START_TIME))

  echo "  t+${ELAPSED}s  pending=$PENDING processing=$PROCESSING"

  if [ "$PENDING" = "0" ] && [ "$PROCESSING" = "0" ]; then
    echo ""
    echo "  Queue drained at t+${ELAPSED}s"
    break
  fi

  sleep 5
done

TOTAL_TIME=$(( $(date +%s) - START_TIME ))

echo ""
echo "=============================================="
echo "  Test complete"
echo "=============================================="
echo "  Total jobs:     $COUNT"
echo "  Total time:     ${TOTAL_TIME}s"
echo "  Submit window:  $((END_SUBMIT - START_TIME))s"
echo "  Drain window:   $((TOTAL_TIME - (END_SUBMIT - START_TIME)))s"
echo ""

# Capture the metrics snapshot for later analysis.
mkdir -p results
SNAPSHOT="results/super-urgent-${COUNT}-$(date +%s).txt"
curl -s http://localhost:8080/metrics > "$SNAPSHOT"
echo "  Metrics saved to: $SNAPSHOT"
echo ""

# Pull the DB state so we can compare completed vs pending.
echo "  Database state:"
docker compose exec -T postgres psql -U rjq -d rjq -c \
  "SELECT status, COUNT(*) FROM jobs WHERE queue_type = 'priority' GROUP BY status;" \
  | tee "results/super-urgent-${COUNT}-db-$(date +%s).txt"

echo ""
echo "Next steps:"
echo "  1. Look at Grafana: filter by queue=priority."
echo "  2. Compare submitted, completed, and pending counts."
echo "  3. Check the time between submission and completion for the last jobs."
#!/bin/bash
# heavy-load.sh — Heavy-duty load test for RJQ
#
# Submits a large mixed workload across all queues and priority levels.
# Uses parallel submission so the queue builds up faster than workers can drain.
#
# Usage:
#   ./heavy-load.sh [fifo_count] [priority_count] [rate_count] [parallelism]
#
# Example:
#   ./heavy-load.sh 5000 1000 200 50

set -e

FIFO_COUNT=${1:-1000}
PRIORITY_COUNT=${2:-1000}
RATE_COUNT=${3:-10}
PARALLEL=${4:-50}

API="http://localhost:8080/jobs"

echo "=============================================="
echo "  RJQ Heavy-Duty Load Test"
echo "  FIFO: $FIFO_COUNT   Priority: $PRIORITY_COUNT   Rate-Limited: $RATE_COUNT"
echo "  Parallelism: $PARALLEL"
echo "=============================================="

submit_fifo() {
  local i=$1
  curl -s -X POST "$API" \
    -H "Content-Type: application/json" \
    -d "{\"job_type\":\"email\",\"payload\":{\"to\":\"fifo-$i@test.com\",\"subject\":\"Newsletter $i\",\"body\":\"Bulk content $i\"},\"queue\":\"fifo\",\"priority\":1}" > /dev/null
}

submit_priority() {
  local i=$1
  local mod=$((i % 10))
  local prio=1
  local subj="Normal $i"
  local to="normal-$i@test.com"

  if [ $mod -eq 0 ]; then
    prio=2
    subj="URGENT $i"
    to="urgent-$i@test.com"
  elif [ $mod -eq 5 ]; then
    prio=3
    subj="SUPER URGENT $i"
    to="super-$i@test.com"
  fi

  curl -s -X POST "$API" \
    -H "Content-Type: application/json" \
    -d "{\"job_type\":\"email\",\"payload\":{\"to\":\"$to\",\"subject\":\"$subj\",\"body\":\"Content $i\"},\"queue\":\"priority\",\"priority\":$prio}" > /dev/null
}

submit_rate() {
  local i=$1
  curl -s -X POST "$API" \
    -H "Content-Type: application/json" \
    -d "{\"job_type\":\"email\",\"payload\":{\"to\":\"rate-$i@test.com\",\"subject\":\"Marketing $i\",\"body\":\"Rate content $i\"},\"queue\":\"rate-limited\",\"priority\":1}" > /dev/null
}

export -f submit_fifo submit_priority submit_rate
export API

# Phase 1: FIFO flood.
echo ""
echo "=== Phase 1: FIFO flood ($FIFO_COUNT jobs) ==="
START=$(date +%s)
seq 1 $FIFO_COUNT | xargs -P $PARALLEL -I {} bash -c 'submit_fifo "$@"' _ {}
END=$(date +%s)
echo "  Done in $((END - START))s"

echo "  Queue is building. Waiting 5 seconds before next phase..."
sleep 5

# Phase 2: Priority mix.
echo ""
echo "=== Phase 2: Priority mix ($PRIORITY_COUNT jobs) ==="
START=$(date +%s)
seq 1 $PRIORITY_COUNT | xargs -P $PARALLEL -I {} bash -c 'submit_priority "$@"' _ {}
END=$(date +%s)
echo "  Done in $((END - START))s"

echo "  Waiting 5 seconds before next phase..."
sleep 5

# Phase 3: Rate-limited.
echo ""
echo "=== Phase 3: Rate-Limited ($RATE_COUNT jobs) ==="
START=$(date +%s)
seq 1 $RATE_COUNT | xargs -P $PARALLEL -I {} bash -c 'submit_rate "$@"' _ {}
END=$(date +%s)
echo "  Done in $((END - START))s"

echo ""
echo "=============================================="
echo "  All jobs submitted"
echo "=============================================="
echo ""
echo "Watching until the queue drains. This can take a while."

IDLE_STREAK=0
while true; do
  STATS=$(curl -s http://localhost:8080/stats)
  PENDING=$(echo "$STATS" | grep -o '"pending":[0-9]*' | cut -d: -f2)
  PROCESSING=$(echo "$STATS" | grep -o '"processing":[0-9]*' | cut -d: -f2)

  if [ "$PENDING" = "0" ] && [ "$PROCESSING" = "0" ]; then
    IDLE_STREAK=$((IDLE_STREAK + 1))
    if [ $IDLE_STREAK -ge 3 ]; then
      echo "  Queue drained."
      break
    fi
  else
    IDLE_STREAK=0
    echo "  pending=$PENDING processing=$PROCESSING"
  fi
  sleep 5
done

echo ""
echo "=== Run complete ==="
echo ""
echo "Save the metrics snapshot:"
echo "  curl -s http://localhost:8080/metrics > results/heavy-load-\$(date +%s).txt"
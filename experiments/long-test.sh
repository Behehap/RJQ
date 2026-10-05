#!/bin/bash
# soak-test.sh — Long-running randomized load test.
#
# Submits a continuous, randomized stream of jobs for a set duration.
# Mixes queue types and priority levels at varying rates, similar to
# how a real email service would be used.
#
# Usage:
#   ./soak-test.sh [duration_minutes] [jobs_per_second] [parallelism]
#
# Examples:
#   ./soak-test.sh 5 10 20       # 5 minutes, ~10 jobs/sec, 20 parallel
#   ./soak-test.sh 30 5 10       # 30 minutes, ~5 jobs/sec, 10 parallel
#   ./soak-test.sh 60 20 50      # 1 hour, ~20 jobs/sec, 50 parallel

set -e

DURATION_MIN=${1:-30}
RATE=${2:-5}
PARALLEL=${3:-10}
API="http://localhost:8080/jobs"

DURATION_SEC=$((DURATION_MIN * 60))

echo "=============================================="
echo "  RJQ Soak Test"
echo "  Duration:    ${DURATION_MIN} minutes"
echo "  Target rate: ~${RATE} jobs/sec"
echo "  Parallelism: ${PARALLEL}"
echo "=============================================="
echo ""

mkdir -p results
LOG="results/soak-$(date +%Y%m%d-%H%M%S).log"
METRICS="results/soak-metrics-$(date +%Y%m%d-%H%M%S).txt"

echo "  Log file:     $LOG"
echo "  Metrics file: $METRICS"
echo ""

# Counters for a running summary.
TOTAL_SUBMITTED=0
START_TIME=$(date +%s)
END_TIME=$((START_TIME + DURATION_SEC))

# Choose a random queue type with a realistic distribution.
# 70% fifo, 25% priority, 5% rate-limited.
pick_queue() {
  local r=$((RANDOM % 100))
  if [ $r -lt 70 ]; then
    echo "fifo"
  elif [ $r -lt 95 ]; then
    echo "priority"
  else
    echo "rate-limited"
  fi
}

# Choose a priority for the priority queue.
# Most jobs are normal, a few are urgent, very few are super-urgent.
pick_priority() {
  local r=$((RANDOM % 100))
  if [ $r -lt 90 ]; then
    echo "1"
  elif [ $r -lt 99 ]; then
    echo "2"
  else
    echo "3"
  fi
}

# Submit one randomized job.
submit_random() {
  local id="$1"
  local queue=$(pick_queue)
  local priority=1
  if [ "$queue" = "priority" ]; then
    priority=$(pick_priority)
  fi

  local to="soak-${queue}-${id}@test.com"
  local subject="Soak ${queue} ${id}"
  local body="Random load test body ${id}"

  curl -s -X POST "$API" \
    -H "Content-Type: application/json" \
    -d "{\"job_type\":\"email\",\"payload\":{\"to\":\"$to\",\"subject\":\"$subject\",\"body\":\"$body\"},\"queue\":\"$queue\",\"priority\":$priority}" \
    > /dev/null
}

export -f submit_random pick_queue pick_priority
export API

# Main loop.
echo "Starting soak test at $(date '+%H:%M:%S')..."
echo "Will run until $(date -d "+${DURATION_MIN} minutes" '+%H:%M:%S')."
echo ""

# Track progress every 30 seconds.
LAST_REPORT=$START_TIME

while [ "$(date +%s)" -lt "$END_TIME" ]; do
  # How many jobs to submit this iteration?
  # Target rate = RATE jobs/sec, loop interval ~ 0.1s.
  # So submit RATE/10 jobs per iteration.
  COUNT=$((RATE / 10))
  if [ $COUNT -lt 1 ]; then
    COUNT=1
  fi

  # Submit COUNT jobs in parallel.
  for i in $(seq 1 $COUNT); do
    ID=$((TOTAL_SUBMITTED + i))
    submit_random "$ID" &
    # Limit parallel curl processes
    if [ $(jobs -r | wc -l) -ge $PARALLEL ]; then
      wait -n
    fi
  done

  TOTAL_SUBMITTED=$((TOTAL_SUBMITTED + COUNT))

  # Sleep enough to hit ~10 iterations per second.
  sleep 0.1

  # Progress report every 30 seconds.
  NOW=$(date +%s)
  if [ $((NOW - LAST_REPORT)) -ge 30 ]; then
    ELAPSED=$((NOW - START_TIME))
    REMAINING=$((END_TIME - NOW))

    STATS=$(curl -s http://localhost:8080/stats)
    PENDING=$(echo "$STATS" | grep -o '"pending":[0-9]*' | cut -d: -f2)
    PROCESSING=$(echo "$STATS" | grep -o '"processing":[0-9]*' | cut -d: -f2)

    RATE_ACTUAL=$(echo "scale=1; $TOTAL_SUBMITTED / $ELAPSED" | bc)

    echo "[$(date '+%H:%M:%S')] elapsed=${ELAPSED}s remaining=${REMAINING}s submitted=${TOTAL_SUBMITTED} (${RATE_ACTUAL}/s) pending=${PENDING} processing=${PROCESSING}" | tee -a "$LOG"

    LAST_REPORT=$NOW
  fi
done

# Wait for any in-flight submissions to finish.
wait

echo ""
echo "Submission phase done at $(date '+%H:%M:%S')."
echo "Total submitted: $TOTAL_SUBMITTED"
echo ""

# Now wait for the queue to drain.
echo "Waiting for the queue to drain (this may take a while for rate-limited jobs)..."
echo ""

IDLE_STREAK=0
while true; do
  STATS=$(curl -s http://localhost:8080/stats)
  PENDING=$(echo "$STATS" | grep -o '"pending":[0-9]*' | cut -d: -f2)
  PROCESSING=$(echo "$STATS" | grep -o '"processing":[0-9]*' | cut -d: -f2)
  NOW=$(date +%s)
  ELAPSED=$((NOW - START_TIME))

  echo "[$(date '+%H:%M:%S')] elapsed=${ELAPSED}s pending=${PENDING} processing=${PROCESSING}" | tee -a "$LOG"

  if [ "$PENDING" = "0" ] && [ "$PROCESSING" = "0" ]; then
    IDLE_STREAK=$((IDLE_STREAK + 1))
    if [ $IDLE_STREAK -ge 3 ]; then
      break
    fi
  else
    IDLE_STREAK=0
  fi
  sleep 10
done

TOTAL_TIME=$(( $(date +%s) - START_TIME ))

echo ""
echo "=============================================="
echo "  Soak test complete"
echo "=============================================="
echo "  Duration:            ${TOTAL_TIME}s ($((TOTAL_TIME / 60))m)"
echo "  Total submitted:     $TOTAL_SUBMITTED"
echo "  Effective rate:      $(echo "scale=2; $TOTAL_SUBMITTED / $TOTAL_TIME" | bc) jobs/sec"
echo "  Log:                 $LOG"
echo ""

# Capture final metrics.
curl -s http://localhost:8080/metrics > "$METRICS"
echo "  Metrics saved to: $METRICS"
echo ""

# Summary from the database.
echo "--- Summary by queue type ---" | tee -a "$LOG"
docker compose exec -T postgres psql -U rjq -d rjq -c \
  "SELECT queue_type, status, COUNT(*) FROM jobs WHERE created_at > NOW() - INTERVAL '${DURATION_MIN} minutes' GROUP BY queue_type, status ORDER BY queue_type, status;" \
  | tee -a "$LOG"

echo "" | tee -a "$LOG"
echo "--- Latency by priority ---" | tee -a "$LOG"
docker compose exec -T postgres psql -U rjq -d rjq -c \
  "SELECT priority, COUNT(*) AS n, AVG(processed_at - created_at) AS avg_wait, MAX(processed_at - created_at) AS max_wait FROM jobs WHERE queue_type='priority' AND created_at > NOW() - INTERVAL '${DURATION_MIN} minutes' AND processed_at IS NOT NULL GROUP BY priority ORDER BY priority;" \
  | tee -a "$LOG"

echo ""
echo "Done."
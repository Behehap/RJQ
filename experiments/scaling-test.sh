#!/bin/bash
set -e

WORKERS=${1:-1}
JOBS=10000
API="http://localhost:8080/jobs"

echo "=============================================="
echo "  RJQ Worker Scaling Test"
echo "  Workers: $WORKERS"
echo "  Jobs:    $JOBS"
echo "=============================================="

# 1. Update config
sed -i "s/workers:.*/workers: $WORKERS/" config.yaml
sed -i 's/demo_delay_seconds:.*/demo_delay_seconds: 0/' config.yaml
sed -i 's/cooldown_seconds:.*/cooldown_seconds: 0/' config.yaml

echo "Restarting app with $WORKERS workers..."
docker compose restart rjq > /dev/null
sleep 3

# 2. Reset
./experiments/reset.sh > /dev/null

echo "Submitting $JOBS jobs..."

# 3. Submit and time it
START_TIME=$(date +%s%3N)

seq 1 $JOBS | xargs -P 50 -I {} curl -s -X POST "$API" \
  -H "Content-Type: application/json" \
  -d '{"job_type":"email","payload":{"to":"scale-{}@test.com","subject":"Scale {}","body":"B"},"queue":"fifo"}' \
  > /dev/null

echo "All jobs submitted. Waiting for queue to drain..."

# 4. Poll until empty
while true; do
  STATS=$(curl -s http://localhost:8080/stats)
  PENDING=$(echo "$STATS" | grep -o '"pending":[0-9]*' | cut -d: -f2)
  PROCESSING=$(echo "$STATS" | grep -o '"processing":[0-9]*' | cut -d: -f2)
  
  if [ "$PENDING" = "0" ] && [ "$PROCESSING" = "0" ]; then
    break
  fi
  sleep 1
done

END_TIME=$(date +%s%3N)
DURATION_MS=$((END_TIME - START_TIME))
DURATION_SEC=$(echo "scale=2; $DURATION_MS / 1000" | bc)
JPS=$(echo "scale=2; $JOBS / $DURATION_SEC" | bc)

echo ""
echo "=============================================="
echo "  Results for $WORKERS Worker(s)"
echo "=============================================="
echo "  Total Time:  ${DURATION_SEC} seconds"
echo "  Throughput:  ${JPS} jobs/sec"

# 5. Save metrics (with duplicate handling)
mkdir -p results/scaling
BASE_NAME="results/scaling/workers-${WORKERS}"
FILENAME="${BASE_NAME}.txt"

# Check if the file exists and find the next available number
if [ -f "$FILENAME" ]; then
  COUNTER=1
  while [ -f "${BASE_NAME}(${COUNTER}).txt" ]; do
    COUNTER=$((COUNTER + 1))
  done
  FILENAME="${BASE_NAME}(${COUNTER}).txt"
fi

curl -s http://localhost:8080/metrics > "$FILENAME"
echo "  Metrics saved to $FILENAME"
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
START_TIME=$(date +%s%3N)

seq 1 $JOBS | xargs -P 50 -I {} curl -s -X POST "$API" \
  -H "Content-Type: application/json" \
  -d '{"job_type":"email","payload":{"to":"scale-{}@test.com","subject":"Scale {}","body":"B"},"queue":"fifo"}' \
  > /dev/null

echo "All jobs submitted. Waiting for queue to drain..."

# 3. Poll until empty
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

echo ""
echo "=============================================="
echo "  Total Time:  ${DURATION_SEC} seconds"
echo "=============================================="

# 4. Save metrics
mkdir -p results/scaling
BASE_NAME="results/scaling/workers-${WORKERS}"
FILENAME="${BASE_NAME}.txt"

if [ -f "$FILENAME" ]; then
  COUNTER=1
  while [ -f "${BASE_NAME}(${COUNTER}).txt" ]; do
    COUNTER=$((COUNTER + 1))
  done
  FILENAME="${BASE_NAME}(${COUNTER}).txt"
fi

curl -s http://localhost:8080/metrics > "$FILENAME"
echo "  Metrics saved to $FILENAME"

# 5. Save the exact time for the analysis script
if [ ! -f results/scaling_times.csv ]; then
    echo "workers,wall_clock_seconds" > results/scaling_times.csv
fi
echo "$WORKERS,$DURATION_SEC" >> results/scaling_times.csv
echo "  Time recorded in results/scaling_times.csv"
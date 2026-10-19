#!/bin/bash
# normalization-test.sh — Finds the baseline throughput of a single worker.
# 
# Submits a large batch of jobs, measures how long they take to drain,
# and calculates the exact number of jobs needed for a 60-second test.

set -e

API="http://localhost:8080/jobs"
BATCH_SIZE=10000

echo "=============================================="
echo "  RJQ Normalization Test (1 Worker, FIFO)"
echo "=============================================="

# 1. Force 1 worker
sed -i 's/workers:.*/workers: 1/' config.yaml
sed -i 's/demo_delay_seconds:.*/demo_delay_seconds: 0/' config.yaml
sed -i 's/cooldown_seconds:.*/cooldown_seconds: 0/' config.yaml

echo "Restarting app with 1 worker..."
docker compose restart rjq > /dev/null
sleep 3
./experiments/reset.sh > /dev/null

echo "Submitting $BATCH_SIZE jobs..."

# 2. Submit jobs in parallel (to not bottleneck on curl)
START_TIME=$(date +%s%3N) # milliseconds
seq 1 $BATCH_SIZE | xargs -P 50 -I {} curl -s -X POST "$API" \
  -H "Content-Type: application/json" \
  -d '{"job_type":"email","payload":{"to":"norm-{}@test.com","subject":"Norm {}","body":"B"},"queue":"fifo"}' \
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
JPS=$(echo "scale=2; $BATCH_SIZE / $DURATION_SEC" | bc)

echo ""
echo "=============================================="
echo "  Results"
echo "=============================================="
echo "  Total Jobs:      $BATCH_SIZE"
echo "  Total Time:      ${DURATION_SEC} seconds"
echo "  Throughput:      ${JPS} jobs/sec (with 1 worker)"
echo ""
echo "  To get a 60-second test run, you should submit:"
RECOMMENDED=$(echo "scale=0; $JPS * 60" | bc)
echo "  ===> $RECOMMENDED jobs per run. <==="
echo ""
echo "Update your load scripts with this number before running the scaling experiments."
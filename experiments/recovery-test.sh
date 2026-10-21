#!/bin/bash
set -e

API="http://localhost:8080/jobs"
JOBS=100000 # 100k jobs is enough to guarantee we catch it mid-flight

echo "=============================================="
echo "  RJQ Crash Recovery Test"
echo "=============================================="

# Force exactly 5 workers
sed -i 's/workers:.*/workers: 5/' config.yaml
sed -i 's/demo_delay_seconds:.*/demo_delay_seconds: 0/' config.yaml
sed -i 's/cooldown_seconds:.*/cooldown_seconds: 0/' config.yaml

echo "Restarting app and resetting database..."
docker compose restart rjq > /dev/null
sleep 3
./experiments/reset.sh > /dev/null

echo "1. Submitting $JOBS jobs in the background (this will take a while)..."
seq 1 $JOBS | xargs -P 200 -I {} curl -s -X POST "$API" \
  -H "Content-Type: application/json" \
  -d '{"job_type":"email","payload":{"to":"crash-{}@test.com","subject":"C","body":"B"},"queue":"fifo"}' \
  > /dev/null &

SUBMIT_PID=$!

echo "2. Waiting for the queue to build up..."
# Wait until we have jobs in the database
while true; do
  STATS=$(curl -s http://localhost:8080/stats)
  PENDING=$(echo "$STATS" | grep -o '"pending":[0-9]*' | cut -d: -f2)
  PROCESSING=$(echo "$STATS" | grep -o '"processing":[0-9]*' | cut -d: -f2)
  
  if [ "$PENDING" -gt 100 ] || [ "$PROCESSING" -gt 0 ]; then
    echo "   Queue is active (pending=$PENDING, processing=$PROCESSING). Proceeding to crash."
    break
  fi
  sleep 1
done

echo "3. Checking state before crash:"
BEFORE_CRASH=$(curl -s http://localhost:8080/stats)
echo "   $BEFORE_CRASH"

echo "4. KILLING THE SERVER (Simulating a power failure)..."
docker compose kill rjq > /dev/null

# Stop the submission loop
kill $SUBMIT_PID 2>/dev/null || true

echo "5. Restarting the server..."
docker compose start rjq > /dev/null
sleep 3

echo "6. Checking state after recovery (should be processing):"
AFTER_RECOVERY=$(curl -s http://localhost:8080/stats)
echo "   $AFTER_RECOVERY"

echo "7. Waiting for all jobs to finish..."
while true; do
  STATS=$(curl -s http://localhost:8080/stats)
  PENDING=$(echo "$STATS" | grep -o '"pending":[0-9]*' | cut -d: -f2)
  PROCESSING=$(echo "$STATS" | grep -o '"processing":[0-9]*' | cut -d: -f2)
  if [ "$PENDING" = "0" ] && [ "$PROCESSING" = "0" ]; then
    break
  fi
  sleep 2
done

echo ""
echo "=============================================="
echo "  Results"
echo "=============================================="
docker compose exec postgres psql -U rjq -d rjq -c \
  "SELECT status, COUNT(*) FROM jobs GROUP BY status;"

echo ""
echo "Check the recovery metric (should be > 0):"
curl -s http://localhost:8080/metrics | grep rjq_jobs_recovered_total
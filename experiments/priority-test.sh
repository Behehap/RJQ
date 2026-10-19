#!/bin/bash
set -e

API="http://localhost:8080/jobs"
JOBS_NORMAL=5000
JOBS_URGENT=500

echo "=============================================="
echo "  RJQ Priority Scheduling Test"
echo "=============================================="

# Force exactly 5 workers
sed -i 's/workers:.*/workers: 5/' config.yaml
sed -i 's/demo_delay_seconds:.*/demo_delay_seconds: 0/' config.yaml
sed -i 's/cooldown_seconds:.*/cooldown_seconds: 0/' config.yaml

echo "Restarting app with 5 workers..."
docker compose restart rjq > /dev/null
sleep 3
./experiments/reset.sh > /dev/null

echo "1. Submitting $JOBS_NORMAL normal priority jobs (Priority 1)..."
seq 1 $JOBS_NORMAL | xargs -P 50 -I {} curl -s -X POST "$API" \
  -H "Content-Type: application/json" \
  -d '{"job_type":"email","payload":{"to":"norm-{}@test.com","subject":"N","body":"B"},"queue":"priority","priority":1}' \
  > /dev/null

# Give the workers a moment to start chewing through the normal jobs
sleep 2

echo "2. Submitting $JOBS_URGENT urgent jobs (Priority 2)..."
seq 1 $JOBS_URGENT | xargs -P 50 -I {} curl -s -X POST "$API" \
  -H "Content-Type: application/json" \
  -d '{"job_type":"email","payload":{"to":"urg-{}@test.com","subject":"U","body":"B"},"queue":"priority","priority":2}' \
  > /dev/null

echo "Waiting for queue to drain..."
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
echo "  Results (Completion Times)"
echo "=============================================="
# Notice how we select MIN and MAX of the time it was processed
docker compose exec postgres psql -U rjq -d rjq -c \
  "SELECT priority, COUNT(*), 
          MIN(processed_at) AS first_done, 
          MAX(processed_at) AS last_done,
          AVG(processed_at - created_at) AS avg_total_time 
   FROM jobs 
   WHERE status='completed' 
   GROUP BY priority 
   ORDER BY priority DESC;"
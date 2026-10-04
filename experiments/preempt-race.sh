#!/bin/bash
# preempt-race.sh — Reproduces the super-urgent preemption race.
#
# Submits a steady stream of normal jobs to keep all workers busy,
# then fires super-urgent jobs in a burst. Under the bug, some
# super-urgent jobs are lost and the counters drift.

API="http://localhost:8080/jobs"

echo "Filling the queue with normal priority jobs..."
for i in $(seq 1 200); do
  curl -s -X POST "$API" \
    -H "Content-Type: application/json" \
    -d "{\"job_type\":\"email\",\"payload\":{\"to\":\"normal-$i@test.com\",\"subject\":\"Normal $i\",\"body\":\"Body\"},\"queue\":\"priority\",\"priority\":1}" > /dev/null
done

sleep 2

echo "Firing 50 super-urgent jobs at once..."
seq 1 50 | xargs -P 50 -I {} curl -s -X POST "$API" \
  -H "Content-Type: application/json" \
  -d '{"job_type":"email","payload":{"to":"super-{}@test.com","subject":"Super {}","body":"Body"},"queue":"priority","priority":3}' > /dev/null

echo "Waiting for queue to drain..."
sleep 20

echo ""
echo "Submitted vs completed:"
curl -s http://localhost:8080/metrics | grep -E "rjq_jobs_(submitted|completed)_total"

echo ""
echo "Queue depth:"
curl -s http://localhost:8080/metrics | grep rjq_queue_depth
#!/bin/bash
# livelock-test.sh — Proves super-urgent jobs never complete when the
# priority queue is otherwise idle.

API="http://localhost:8080/jobs"

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
echo "Submitting one super-urgent job to an idle system..."
RESPONSE=$(curl -s -X POST "$API" \
  -H "Content-Type: application/json" \
  -d '{"job_type":"email","payload":{"to":"lone-super@test.com","subject":"LONE SUPER URGENT","body":"Should complete immediately"},"queue":"priority","priority":3}')

echo "  API response: $RESPONSE"
echo ""
echo "Waiting 30 seconds. If the job is still pending, the livelock is real."
sleep 30

echo ""
echo "Job status:"
JOB_ID=$(echo "$RESPONSE" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)
curl -s "http://localhost:8080/jobs/$JOB_ID"
echo ""
echo "Stats:"
curl -s http://localhost:8080/stats
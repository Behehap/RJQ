#!/bin/bash
# preemption-demo.sh — Shows priority queue and super-urgent preemption

echo "=============================================="
echo "  RJQ — Priority & Preemption Demo"
echo "=============================================="
echo ""
echo "Config: 5 workers, 5s delay, 1s cooldown"
echo ""

# Phase 1: Fill all 5 workers + 3 queued normals
echo "=== Phase 1: Filling the queue ==="
echo "Submitting 8 NORMAL jobs (will fill all workers + 3 in queue)..."
for i in $(seq 1 8); do
  curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d "{\"to\":\"normal-$i@test.com\",\"subject\":\"Normal Job $i\",\"body\":\"demo body\",\"priority\":1}" > /dev/null
  echo "  [$i/8] Normal Job $i submitted"
  sleep 0.5
done

echo ""
echo "Status: 5 workers processing, 3 jobs in queue."
echo "Open http://localhost:8080/dashboard — you should see:"
echo "  - 5 workers active (blue spinner)"
echo "  - 3 normal jobs waiting in queue (yellow)"
sleep 3

# Phase 2: Send urgent — should jump ahead of queued normals
echo ""
echo "=== Phase 2: Urgent job arrives ==="
echo "Submitting URGENT job (priority 2 — jumps to front of queue)..."
curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"to":"urgent@test.com","subject":"URGENT: System Alert","body":"demo body","priority":2}' > /dev/null
echo "  URGENT job submitted!"
echo ""
echo "Dashboard should now show:"
echo "  - URGENT job at position #4 in queue (orange, URGENT badge)"
echo "  - It will be picked up BEFORE the normal jobs waiting behind it"
sleep 3

# Phase 3: More normals
echo ""
echo "=== Phase 3: More normal jobs ==="
echo "Submitting 3 more NORMAL jobs..."
for i in $(seq 9 11); do
  curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d "{\"to\":\"normal-$i@test.com\",\"subject\":\"Normal Job $i\",\"body\":\"demo body\",\"priority\":1}" > /dev/null
  echo "  Normal Job $i submitted"
  sleep 0.5
done
sleep 2

# Phase 4: Super urgent — preempts a running worker
echo ""
echo "=============================================="
echo "=== Phase 4: SUPER URGENT — PREEMPTION! ==="
echo "=============================================="
echo "Submitting SUPER URGENT job (priority 3)..."
echo "This will KICK OUT a running normal job and take its worker!"
echo ""
curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"to":"emergency@test.com","subject":"SUPER URGENT: Critical Alert!","body":"demo body","priority":3}' > /dev/null
echo "  SUPER URGENT submitted!"
echo ""
echo "Watch the dashboard NOW:"
echo "  - One worker's spinner stops abruptly"
echo "  - That worker card turns RED with fast spinner"
echo "  - The SUPER URGENT badge appears"
echo "  - The kicked-out normal job goes back to the queue"
echo "  - It gets RE-QUEUED without retry penalty"
sleep 5

echo ""
echo "=============================================="
echo "  Demo Complete!"
echo "=============================================="
echo ""
echo "Expected final queue order:"
echo "  Queue: preempted normal + urgent + normals (by priority)"
echo "  Workers: super-urgent + normals still processing"
echo ""
echo "Dashboard: http://localhost:8080/dashboard"
echo ""
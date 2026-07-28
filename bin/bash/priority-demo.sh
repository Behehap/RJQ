#!/bin/bash
# multi-queue-demo.sh — Realistic multi-queue simulation

echo "=============================================="
echo "  RJQ — Multi-Queue Demo"
echo "  FIFO · Priority · Rate-Limited"
echo "=============================================="
echo ""


# Phase 1: Priority queue — normals first
echo ""
echo "=== Phase 2: Priority Queue ==="
echo "Submitting 5 NORMAL priority jobs..."
for i in $(seq 1 8); do
  curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d "{\"to\":\"normal-$i@test.com\",\"subject\":\"Report Gen $i\",\"body\":\"demo body\",\"queue\":\"priority\",\"priority\":1}" > /dev/null
  echo "  Normal $i"
  sleep 0.6
done
# Phase 2: FIFO queue — 5 jobs only
echo "=== Phase 1: FIFO Queue (5 jobs) ==="
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d "{\"to\":\"fifo-$i@test.com\",\"subject\":\"Newsletter #$i\",\"body\":\"demo body\",\"queue\":\"fifo\"}" > /dev/null
  echo "  FIFO Job $i"
  sleep 0.8
done

# Let the queue build — workers are busy with FIFO and first normals
echo ""
echo "  (workers are busy, priority queue building...)"
sleep 3

# URGENT arrives — should jump ahead of queued normals
echo ""
echo ">>> URGENT job arrives! (jumps ahead of queued normals)"
curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"to":"urgent@test.com","subject":"URGENT: Password Reset","body":"demo body","queue":"priority","priority":2}' > /dev/null
echo "  URGENT — Password Reset"
sleep 1

# 2 more normals queue behind the urgent
echo ""
echo "2 more normals join behind urgent..."
for i in $(seq 6 7); do
  curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d "{\"to\":\"normal-$i@test.com\",\"subject\":\"Report Gen $i\",\"body\":\"demo body\",\"queue\":\"priority\",\"priority\":1}" > /dev/null
  echo "  Normal $i"
  sleep 0.6
done

# Wait — let the urgent get picked up, normals still waiting
echo ""
echo "  (urgent being processed, normals waiting...)"
sleep 5

# SUPER URGENT — preempts any running normal job
echo ""
echo "=============================================="
echo ">>> SUPER URGENT — PREEMPTION!"
echo "=============================================="
curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"to":"emergency@test.com","subject":"SUPER URGENT: Server Down!","body":"demo body","queue":"priority","priority":3}' > /dev/null
echo "  SUPER URGENT submitted!"
echo "  (kicks out ANY running normal job — FIFO or Priority)"
sleep 1

echo ""
echo "2 final normals..."
for i in $(seq 8 9); do
  curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d "{\"to\":\"normal-$i@test.com\",\"subject\":\"Report Gen $i\",\"body\":\"demo body\",\"queue\":\"priority\",\"priority\":1}" > /dev/null
  echo "  Normal $i"
  sleep 0.6
done

# Phase 3: Rate-limited — only 3 jobs
echo ""
echo "=== Phase 3: Rate-Limited Queue (3 jobs, 6/min) ==="
for i in $(seq 1 3); do
  curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d "{\"to\":\"rate-$i@test.com\",\"subject\":\"Marketing #$i\",\"body\":\"demo body\",\"queue\":\"rate-limited\"}" > /dev/null
  echo "  Rate-Limited $i"
  sleep 0.5
done

echo ""
echo "=============================================="
echo "  Demo Ready — 22 jobs across 3 queues"
echo "=============================================="
echo ""
echo "Priority Queue Order:"
echo "  #1:  SUPER URGENT (red, pulsing)"
echo "  #2:  URGENT (orange)"
echo "  #3+: NORMALs (yellow)"
echo ""
echo "Super-urgent preempts ANY normal job (FIFO or Priority)."
echo "Preempted job goes back to its queue without penalty."
echo ""
echo "Dashboard: http://localhost:8080/dashboard"
echo ""
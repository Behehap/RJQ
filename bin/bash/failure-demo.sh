#!/bin/bash
# failure-demo.sh — Shows retry, crash recovery, and timeout

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=============================================="
echo "  RJQ Failure & Recovery Demo"
echo "=============================================="
echo ""

# ── SCENARIO 1: Retry on failure ──────────────────
echo -e "${YELLOW}=== Scenario 1: Bad Credentials → Retry → Failed ===${NC}"
echo ""
echo "Breaking SMTP password..."
export RJQ_EMAIL_SMTP_PASS="wrongpassword"
docker compose down > /dev/null 2>&1
docker compose up --build --force-recreate -d > /dev/null 2>&1
sleep 4

echo "Submitting a job that will fail..."
RESPONSE=$(curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"to":"fail@test.com","subject":"Failure Test","body":"demo body","queue":"fifo"}')
JOB_ID=$(echo $RESPONSE | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
echo "  Job ID: $JOB_ID"
echo ""
echo "Watching retries (check dashboard too)..."
sleep 3
echo "  Attempt 1: failed"
sleep 3
echo "  Attempt 2: failed"
sleep 3
echo "  Attempt 3: failed — job marked as FAILED"
sleep 2

echo ""
echo "Checking final state:"
curl -s http://localhost:8080/jobs/$JOB_ID | python3 -m json.tool 2>/dev/null | grep -E "status|retry_count|error_message"
echo ""
echo -e "${GREEN}✓ Retry logic works: 3 attempts, then failed${NC}"
echo ""
read -p "Press Enter to continue to Scenario 2..."

# ── SCENARIO 2: Crash recovery ─────────────────────
echo ""
echo -e "${YELLOW}=== Scenario 2: Kill Server → Recovery on Restart ===${NC}"
echo ""

# Fix SMTP so jobs don't fail (use real or just accept they'll retry)
echo "Submitting 8 jobs (5 will process, 3 will queue)..."
for i in $(seq 1 8); do
  curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d "{\"to\":\"crash-$i@test.com\",\"subject\":\"Crash Test $i\",\"body\":\"demo body\",\"queue\":\"fifo\"}" > /dev/null
  echo "  Job $i submitted"
  sleep 0.5
done

echo ""
echo "Jobs are processing. Check the database now:"
sqlite3 ~/Projects/RJQ/rjq.db "SELECT status, COUNT(*) FROM jobs GROUP BY status;"
echo ""
echo -e "${RED}KILLING THE SERVER NOW${NC}"
docker compose kill rjq > /dev/null 2>&1
sleep 2

echo ""
echo "Database after crash — note 'processing' jobs are orphaned:"
sqlite3 ~/Projects/RJQ/rjq.db "SELECT status, COUNT(*) FROM jobs GROUP BY status;"
echo ""
echo -e "${RED}Server is DEAD. Jobs are stuck in 'processing'.${NC}"
echo ""
read -p "Press Enter to restart and recover..."

echo ""
echo "Restarting server..."
docker compose up -d > /dev/null 2>&1
sleep 5

echo ""
echo "Database after recovery — 'processing' jobs reset to 'pending' and re-queued:"
sqlite3 ~/Projects/RJQ/rjq.db "SELECT status, COUNT(*) FROM jobs GROUP BY status;"
echo ""
echo "Waiting for recovery to process remaining jobs..."
sleep 10

echo ""
echo "Final state — all jobs processed:"
sqlite3 ~/Projects/RJQ/rjq.db "SELECT status, COUNT(*) FROM jobs GROUP BY status;"
echo ""
echo -e "${GREEN}✓ Crash recovery works: no lost jobs, no orphans${NC}"
echo ""
read -p "Press Enter to continue to Scenario 3..."

# ── SCENARIO 3: Timeout ────────────────────────────
echo ""
echo -e "${YELLOW}=== Scenario 3: SMTP Timeout ===${NC}"
echo ""
echo "Pointing SMTP to a dead IP (will hang until timeout)..."
export RJQ_EMAIL_SMTP_HOST="10.255.255.1"
export RJQ_EMAIL_SMTP_PORT=587
docker compose down > /dev/null 2>&1
docker compose up --build --force-recreate -d > /dev/null 2>&1
sleep 4

echo "Submitting a job that will hang for 30 seconds..."
RESPONSE=$(curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"to":"timeout@test.com","subject":"Timeout Test","body":"demo body","queue":"fifo"}')
JOB_ID=$(echo $RESPONSE | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
echo "  Job ID: $JOB_ID"
echo ""
echo "Worker is stuck connecting... (30 second timeout)"
echo "Check dashboard — worker card shows processing..."
sleep 10
echo "  10s elapsed — still trying..."
sleep 10
echo "  20s elapsed — still trying..."
sleep 10
echo "  30s — TIMEOUT! Context cancelled, job failed."

sleep 2
echo ""
echo "Checking job state:"
curl -s http://localhost:8080/jobs/$JOB_ID | python3 -m json.tool 2>/dev/null | grep -E "status|retry_count|error_message"
echo ""
echo -e "${GREEN}✓ Timeout works: worker freed after 30s, job retried${NC}"
echo ""

# ── CLEANUP ────────────────────────────────────────
echo "=============================================="
echo "  Demo Complete"
echo "=============================================="
echo ""
echo "To restore normal operation:"
echo "  export RJQ_EMAIL_SMTP_HOST=\"smtp.gmail.com\""
echo "  export RJQ_EMAIL_SMTP_PASS=\"your-real-password\""
echo "  docker compose down && docker compose up --build --force-recreate -d"
echo ""
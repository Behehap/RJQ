#!/bin/bash
# priority-mix.sh — Submit an interleaved mix of priority levels.
#
# Builds a list of N jobs with a given distribution of priorities,
# shuffles the list, and submits them all as one stream. Measures
# whether priority is respected by comparing completion times.
#
# Usage:
#   ./priority-mix.sh [normal_count] [urgent_count] [super_count] [parallelism]
#
# Examples:
#   ./priority-mix.sh 100 0 0 50
#   ./priority-mix.sh 0 100 0 50
#   ./priority-mix.sh 0 0 100 50
#   ./priority-mix.sh 900 90 10 50
#   ./priority-mix.sh 9000 900 100 50

set -e

NORMAL_COUNT=${1:-100}
URGENT_COUNT=${2:-0}
SUPER_COUNT=${3:-0}
PARALLEL=${4:-50}
API="http://localhost:8080/jobs"

TOTAL=$((NORMAL_COUNT + URGENT_COUNT + SUPER_COUNT))

echo "=============================================="
echo "  RJQ Priority Mix Test (Interleaved)"
echo "  Normal:  $NORMAL_COUNT"
echo "  Urgent:  $URGENT_COUNT"
echo "  Super:   $SUPER_COUNT"
echo "  Total:   $TOTAL"
echo "  Parallelism: $PARALLEL"
echo "=============================================="
echo ""

# Wait for idle.
echo "Waiting for the system to drain..."
while true; do
  STATS=$(curl -s http://localhost:8080/stats)
  PENDING=$(echo "$STATS" | grep -o '"pending":[0-9]*' | cut -d: -f2)
  PROCESSING=$(echo "$STATS" | grep -o '"processing":[0-9]*' | cut -d: -f2)
  if [ "$PENDING" = "0" ] && [ "$PROCESSING" = "0" ]; then
    echo "  System idle."
    break
  fi
  sleep 2
done

# Build the interleaved job list.
echo ""
echo "Building interleaved job list..."
JOBLIST=$(mktemp)
trap "rm -f $JOBLIST" EXIT

for i in $(seq 1 $NORMAL_COUNT); do
  echo "1 normal-$i" >> "$JOBLIST"
done
for i in $(seq 1 $URGENT_COUNT); do
  echo "2 urgent-$i" >> "$JOBLIST"
done
for i in $(seq 1 $SUPER_COUNT); do
  echo "3 super-$i" >> "$JOBLIST"
done

# Shuffle so the three priorities are interleaved in a random order.
shuf "$JOBLIST" -o "$JOBLIST"

echo "  Job list built and shuffled: $(wc -l < $JOBLIST) jobs."
echo ""

# Submit function that reads one line and posts the right priority.
submit_one() {
  local line="$1"
  local prio=$(echo "$line" | awk '{print $1}')
  local target=$(echo "$line" | awk '{print $2}')
  local label
  case "$prio" in
    1) label="NORMAL" ;;
    2) label="URGENT" ;;
    3) label="SUPER" ;;
  esac

  curl -s -X POST "$API" \
    -H "Content-Type: application/json" \
    -d "{\"job_type\":\"email\",\"payload\":{\"to\":\"$target@test.com\",\"subject\":\"$label $target\",\"body\":\"Body\"},\"queue\":\"priority\",\"priority\":$prio}" \
    > /dev/null
}
export -f submit_one
export API

START=$(date +%s)
echo "Start time: $(date -d @$START '+%H:%M:%S')"
echo "Submitting $TOTAL jobs in interleaved order..."
echo ""

cat "$JOBLIST" | xargs -P $PARALLEL -I {} bash -c 'submit_one "$@"' _ {}

END_SUBMIT=$(date +%s)
echo "  All submitted in $((END_SUBMIT - START))s."
echo ""

# Monitor drain.
echo "Monitoring drain..."
while true; do
  STATS=$(curl -s http://localhost:8080/stats)
  PENDING=$(echo "$STATS" | grep -o '"pending":[0-9]*' | cut -d: -f2)
  PROCESSING=$(echo "$STATS" | grep -o '"processing":[0-9]*' | cut -d: -f2)
  NOW=$(date +%s)
  echo "  t+$((NOW - START))s  pending=$PENDING processing=$PROCESSING"
  if [ "$PENDING" = "0" ] && [ "$PROCESSING" = "0" ]; then
    break
  fi
  sleep 2
done

TOTAL_TIME=$(( $(date +%s) - START ))
echo ""
echo "  Drained at t+${TOTAL_TIME}s"
echo ""

# Analyze: order of completion per priority level.
mkdir -p results
OUT="results/priority-mix-$(date +%s).txt"

echo "=============================================="
echo "  Completion order (first 10 per priority)"
echo "=============================================="
echo ""

for prio in 1 2 3; do
  case "$prio" in
    1) label="NORMAL" ;;
    2) label="URGENT" ;;
    3) label="SUPER" ;;
  esac
  echo "--- $label (priority=$prio) ---" | tee -a "$OUT"
  docker compose exec -T postgres psql -U rjq -d rjq -c \
    "SELECT ROW_NUMBER() OVER (ORDER BY processed_at) AS ord, processed_at, processed_at - created_at AS wait FROM jobs WHERE queue_type='priority' AND priority=$prio ORDER BY processed_at LIMIT 10;" \
    | tee -a "$OUT"
  echo "" | tee -a "$OUT"
done

echo "--- Summary by priority ---" | tee -a "$OUT"
docker compose exec -T postgres psql -U rjq -d rjq -c \
  "SELECT priority, COUNT(*) AS total, AVG(processed_at - created_at) AS avg_wait, MAX(processed_at - created_at) AS max_wait FROM jobs WHERE queue_type='priority' AND processed_at IS NOT NULL GROUP BY priority ORDER BY priority;" \
  | tee -a "$OUT"

echo ""
echo "Results saved to: $OUT"
echo ""
echo "How to read this:"
echo "  - If priority works: avg_wait(3) < avg_wait(2) < avg_wait(1)"
echo "  - If priority is broken: all priorities have similar avg_wait, or"
echo "    the higher priorities wait longer."
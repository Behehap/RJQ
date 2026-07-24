# RJQ — Reliable Job Queue

A fault-tolerant job queue written in Go that accepts email jobs via HTTP, persists them in SQLite, processes them with configurable workers, and survives crashes without losing work.

## Features

- REST API to submit and track email jobs
- Configurable number of concurrent workers
- Automatic retry with exponential backoff
- Per-job timeout via context deadline
- Crash recovery — pending jobs reloaded on startup
- Graceful shutdown — in-flight jobs complete before exit
- SQLite persistence with interface for swapping backends
- Docker support with multi-stage builds

## Quick Start

### Local

```bash
# Install dependencies
go mod download

# Build
go build -o rjq ./cmd/server

# Run
./rjq
```

# 1. Stop everything

docker compose down

# 2. Clear the broken database

rm ~/Projects/RJQ/rjq.db
touch ~/Projects/RJQ/rjq.db

# 3. Rebuild and start

docker compose up --build --force-recreate -d

# 4. Wait for it to start

sleep 3

# 5. Check it's alive

curl -s http://localhost:8080/health

# 6. Submit 10 jobs sequentially with 2-second gap

for i in $(seq 1 10); do
curl -s -X POST http://localhost:8080/jobs \
 -H "Content-Type: application/json" \
 -d "{\"to\":\"beh.e.h18@gmail.com\",\"subject\":\"Sequential Job $i\",\"body\":\"This is job number $i\"}"
echo ""
sleep 2
done

# 7. Watch the stats while it processes

watch -n 2 'curl -s http://localhost:8080/stats'

# 8. Open dashboard in browser

# http://localhost:8080/dashboard

# 9. When done, check the database

sqlite3 ~/Projects/RJQ/rjq.db "SELECT status, COUNT(\*) FROM jobs GROUP BY status;"

# 10. Stop when finished

docker compose down

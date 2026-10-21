```markdown
# RJQ — A Reliable Job Queue in Go

A small, self-contained job queue that accepts work over HTTP, persists it to a database, and processes it concurrently with automatic retries, crash recovery, and priority-based scheduling.

Built as an undergraduate systems project to study the tradeoffs between reliability, scheduling policies, and observability in a single-node queue.

---

## Why this exists

Most job queues in production rely on Redis, RabbitMQ, or Kafka. That's fine when you have infrastructure. It's overkill when you're sending a few thousand emails a day and want something you can read end to end in an afternoon.

RJQ runs as a single binary. It uses SQLite or Postgres as its only dependency. It exposes the internal state through Prometheus, so you can see exactly what's happening. It survives crashes because every job is written to disk before it enters the queue.

The goal is not to compete with Celery or Sidekiq. The goal is to be small enough to reason about, reliable enough to trust, and honest enough to measure.

---

## Features

- **HTTP API** for job submission and status
- **Persistent storage** in SQLite or PostgreSQL, behind a single interface
- **Concurrent workers** with configurable pool size
- **Three queue types**: FIFO, Priority, Rate-Limited
- **Preemption** for super-urgent jobs: interrupts a running normal job, requeues it without penalty
- **Automatic retries** with bounded attempts and clean failure states
- **Crash recovery** on startup: pending and processing jobs are re-enqueued
- **Periodic sweeper** for orphaned jobs and stuck processing states
- **Worker supervision**: dead workers are restarted automatically
- **Prometheus metrics** for every state transition, latency, and failure
- **Live dashboard** served from the binary, plus a Grafana dashboard for deep analysis
- **Docker Compose** stack with Postgres, Mailpit, Prometheus, and Grafana

---

## Architecture
```

                    ┌──────────────┐
                    │   Client     │
                    └──────┬───────┘
                           │ POST /jobs
                    ┌──────▼───────┐
                    │   HTTP API   │
                    └──────┬───────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
    ┌──────────────────┐     ┌──────────────────┐
    │ Save to database │     │  Route to queue  │
    │  (source of      │     │                  │
    │   truth)         │     │  ┌────────────┐  │
    └──────────────────┘     │  │ FIFO       │  │
                             │  ├────────────┤  │
                             │  │ Priority   │  │
                             │  ├────────────┤  │
                             │  │ Rate-Ltd   │  │
                             │  └────────────┘  │
                             └────────┬─────────┘
                                      │
                             ┌────────▼─────────┐
                             │  Worker Pool     │
                             │  (supervised)    │
                             └────────┬─────────┘
                                      │
                             ┌────────▼─────────┐
                             │  Email via SMTP  │
                             └──────────────────┘

````

The database is the source of truth. Jobs are written before they are queued. If the process dies, the queue is rebuilt from the database on startup.

The in-memory queue is a delivery mechanism, not a store. It can be emptied at any time, and the database still knows what needs to happen.

---

## Quick start

### With Docker

```bash
git clone <repo>
cd rjq
docker compose up -d --build
````

Five containers start: the application, Postgres, Mailpit (a local SMTP sink), Prometheus, and Grafana.

Open the dashboard at `http://localhost:8080/dashboard`.

### Locally

Requires Go 1.24+ and gcc (for the SQLite driver).

```bash
go mod download
go build -o rjq ./cmd/server
./rjq
```

By default it uses SQLite and Mailpit. To point it at Postgres, edit `config.yaml`.

---

## Configuration

All configuration lives in `config.yaml`. Environment variables with the `RJQ_` prefix override file values. Dots in YAML keys become underscores. So `queue.workers` is overridden by `RJQ_QUEUE_WORKERS`.

```yaml
server:
  port: 8080

queue:
  workers: 5
  demo_delay_seconds: 0
  cooldown_seconds: 0

email:
  mode: "test" # "test" for Mailpit, "production" for real SMTP

  smtp_host: "smtp.gmail.com"
  smtp_port: 587
  smtp_user: ""
  smtp_pass: ""

  test_host: "mailpit"
  test_port: 1025
  test_user: "rjq@localhost"
  test_pass: ""

retry:
  max_attempts: 3

timeout:
  job_seconds: 30

rate_limit:
  emails_per_minute: 6
  burst: 3

database:
  backend: "postgres" # "sqlite" or "postgres"
  sqlite_path: "rjq.db"
  postgres_dsn: "postgres://rjq:rjq@localhost:5432/rjq?sslmode=disable"
```

Credentials should never be committed. Put them in `.env`:

```
RJQ_EMAIL_SMTP_USER=you@example.com
RJQ_EMAIL_SMTP_PASS=your-app-password
```

---

## API

### Submit a job

```bash
POST /jobs
Content-Type: application/json

{
  "job_type": "email",
  "payload": {
    "to": "user@example.com",
    "subject": "Hello",
    "body": "World"
  },
  "queue": "fifo",
  "priority": 1
}
```

`queue` is one of `fifo`, `priority`, `rate-limited`.  
`priority` is `1` (normal), `2` (urgent), or `3` (super-urgent). Ignored for FIFO and rate-limited queues.

Response:

```json
{ "job_id": "a3f1...", "status": "pending" }
```

### Get a job

```bash
GET /jobs/{id}
```

### List recent jobs

```bash
GET /jobs
```

### Get aggregate counts

```bash
GET /counts
```

Returns a map of queue type to status counts. Unlike `/jobs`, this queries the entire table, not a recent window.

### Get current state

```bash
GET /stats     # pending, processing, queued
GET /pending   # list of pending jobs
GET /processing # list of jobs in flight
GET /health    # worker liveness
```

### Retry a failed job

```bash
POST /jobs/{id}/retry
```

Resets the retry counter and adds three more attempts.

---

## Design notes

### Persistence before delivery

Every job is written to the database before it is enqueued. If the process crashes between those two steps, the job is still on disk. `Recover()` finds it on the next startup and re-enqueues it. This is why jobs are never lost.

### Three queue types

The FIFO queue is a Go channel. It preserves order and has no priority logic. It is the default.

The priority queue is a sorted slice protected by a mutex. Jobs are ordered by priority descending, then creation time ascending. A signal channel wakes a waiting worker when a job is added. Super-urgent jobs preempt a running normal job; urgent jobs jump the queue; normal jobs are served in order.

The rate-limited queue wraps a channel with a token bucket. Tokens refill at a configured rate. Jobs wait for a token before they are returned to a worker. This is how the system enforces a sending rate that matches a provider's limits.

### Worker supervision

Each worker runs a dequeue-process loop. If a worker panics, the panic is recovered, logged with a stack trace, and the worker goroutine exits. A supervisor goroutine watches a `workerDied` channel and restarts the dead worker after a one-second backoff. Without supervision, a single panic would silently remove a worker from the pool, the channel would eventually fill, and every producer would block.

### Metrics in the storage layer

Counters for `completed`, `failed`, and `retried` are incremented inside the storage methods, not the queue. A counter fires only when the database transition actually happens. This prevents the drift that occurs when the queue and the database disagree about what just happened.

### Opportunistic STARTTLS

The email client checks the server's `EHLO` response for the `STARTTLS` extension. If it's advertised, the connection is upgraded before authentication. Gmail requires this. Mailpit does not, and rejects the command if sent unconditionally. The same code path works against both.

### Bounded network calls

SMTP dials have a five-second timeout. The entire conversation is bounded by a deadline on the underlying TCP connection, set from the job's context. If the network becomes unreachable, the call fails within the deadline instead of blocking a worker indefinitely.

---

## Metrics

Prometheus exposes the following series under `/metrics`.

| Metric                                | Type      | Description                                    |
| ------------------------------------- | --------- | ---------------------------------------------- |
| `rjq_jobs_submitted_total`            | counter   | Jobs accepted by the API                       |
| `rjq_jobs_completed_total`            | counter   | Jobs finished successfully                     |
| `rjq_jobs_failed_total`               | counter   | Jobs that exhausted retries                    |
| `rjq_jobs_retried_total`              | counter   | Every retry attempt                            |
| `rjq_jobs_preempted_total`            | counter   | Normal jobs interrupted by super-urgent jobs   |
| `rjq_jobs_recovered_total`            | counter   | Jobs reloaded on startup                       |
| `rjq_job_wait_duration_seconds`       | histogram | Time from submission to processing             |
| `rjq_job_processing_duration_seconds` | histogram | Time from start to Ack/Nack                    |
| `rjq_queue_depth`                     | gauge     | Pending jobs per queue, read from the database |
| `rjq_recovery_duration_seconds`       | gauge     | Time spent in startup recovery                 |

Utilization is derived from the processing histogram:

```
sum(rate(rjq_job_processing_duration_seconds_sum[1m]))
```

This is the average number of busy workers over the interval. Divide by the configured worker count for a percentage. It does not suffer from the aliasing problem that an instantaneous busy-worker gauge has at high throughput.

---

## Testing

```bash
go test ./...          # unit tests
go test ./... -race    # with race detector
```

The storage and queue packages have coverage. The worker pool and API layers are exercised through the demo scripts.

Two test scripts are included in `experiments/`:

- `priority-mix.sh` submits a mixed priority workload and reports the completion order
- `soak-test.sh` runs a randomized continuous load for a configurable duration

---

## Project structure

```
rjq/
├── cmd/server/          # entry point
├── internal/
│   ├── api/             # HTTP handlers and dashboard HTML
│   ├── config/          # Viper-based configuration loading
│   ├── jobs/email/      # SMTP processor
│   ├── metrics/         # Prometheus metric definitions and collectors
│   ├── queue/           # queue interfaces and implementations
│   ├── storage/         # storage interface, SQLite and Postgres backends
│   └── worker/          # worker pool and supervisor
├── pkg/models/          # Job struct shared across packages
├── experiments/         # load test and demo scripts
├── grafana/             # dashboard JSON export
├── config.yaml
├── docker-compose.yml
└── Dockerfile
```

---

## Limitations

This is a single-node queue. It does not coordinate across multiple instances. Running two copies of the binary against the same database will cause double processing.

The rate limiter is per-process. Two instances would each apply the configured rate, resulting in twice the outbound traffic.

The sweeper runs on a fixed interval. Jobs that become orphaned between sweeps wait until the next pass. For a queue processing thousands of jobs per minute, this is fine. For very low-volume queues, a shorter interval would be better.

The priority queue holds jobs in memory. A pending backlog larger than the available RAM would exhaust memory. For the workloads this system was designed for, that ceiling is far higher than the actual demand.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

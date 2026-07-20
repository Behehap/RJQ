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

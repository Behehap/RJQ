CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    queue_type TEXT DEFAULT 'fifo',
    priority INTEGER DEFAULT 1,
    status TEXT NOT NULL CHECK(status IN ('pending','processing','completed','failed')),
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_queue
ON jobs (status, queue_type, priority DESC, created_at ASC);
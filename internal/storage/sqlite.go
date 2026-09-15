package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"rjq/pkg/models"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteStorage implements Storage using SQLite as the backend.
// It uses a single *sql.DB connection pool, which is safe for
// concurrent goroutines by default.
type SQLiteStorage struct {
	db *sql.DB
}

// NewSQLiteStorage opens the SQLite database at dsn, runs the schema
// migration, and returns a ready-to-use storage instance.
//
// The dsn (Data Source Name) can include query parameters recognized
// by the SQLite driver, e.g. "file:rjq.db?cache=shared&_journal_mode=WAL".
func NewSQLiteStorage(dsn string) (*SQLiteStorage, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Connection pool tuning for SQLite.
	// SQLite only supports one writer at a time, so keep these low.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return &SQLiteStorage{db: db}, nil
}

// migrate creates the jobs table if it doesn't exist.
// The schema is job-type agnostic. Type-specific data lives in payload (JSON).
func migrate(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		job_type TEXT NOT NULL,
		payload TEXT NOT NULL,
		queue_type TEXT DEFAULT 'fifo',
		priority INTEGER DEFAULT 1,
		status TEXT NOT NULL CHECK(status IN ('pending','processing','completed','failed')),
		retry_count INTEGER DEFAULT 0,
		max_retries INTEGER DEFAULT 3,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		processed_at DATETIME,
		error_message TEXT
	);`
	_, err := db.Exec(query)
	return err
}

// marshalPayload converts the payload map into JSON for storage.
func marshalPayload(payload map[string]interface{}) (string, error) {
	if payload == nil {
		return "{}", nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}
	return string(b), nil
}

// unmarshalPayload converts stored JSON back into a map.
func unmarshalPayload(data string) (map[string]interface{}, error) {
	if data == "" {
		return map[string]interface{}{}, nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	return m, nil
}

// scanJob reads a single row into a Job struct.
func scanJob(scanner interface {
	Scan(dest ...interface{}) error
}) (*models.Job, error) {
	job := &models.Job{}
	var payloadStr string
	var processedAt sql.NullTime
	var errorMsg sql.NullString

	err := scanner.Scan(
		&job.ID,
		&job.JobType,
		&payloadStr,
		&job.QueueType,
		&job.Priority,
		&job.Status,
		&job.RetryCount,
		&job.MaxRetries,
		&job.CreatedAt,
		&job.UpdatedAt,
		&processedAt,
		&errorMsg,
	)
	if err != nil {
		return nil, err
	}

	job.Payload, err = unmarshalPayload(payloadStr)
	if err != nil {
		return nil, err
	}

	if processedAt.Valid {
		job.ProcessedAt = &processedAt.Time
	}
	if errorMsg.Valid {
		job.ErrorMessage = errorMsg.String
	}

	return job, nil
}

// SaveJob inserts a new job into the database.
func (s *SQLiteStorage) SaveJob(job *models.Job) error {
	payloadStr, err := marshalPayload(job.Payload)
	if err != nil {
		return err
	}

	query := `
	INSERT INTO jobs (id, job_type, payload, queue_type, priority, status, retry_count, max_retries, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err = s.db.Exec(query,
		job.ID,
		job.JobType,
		payloadStr,
		job.QueueType,
		job.Priority,
		job.Status,
		job.RetryCount,
		job.MaxRetries,
		job.CreatedAt,
		job.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to save job: %w", err)
	}
	return nil
}

// GetJob retrieves a single job by ID.
func (s *SQLiteStorage) GetJob(id string) (*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs WHERE id = ?`

	job, err := scanJob(s.db.QueryRow(query, id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}
	return job, nil
}

// UpdateJobStatus updates the job's status and optionally records an error.
func (s *SQLiteStorage) UpdateJobStatus(id string, status string, errMsg string) error {
	now := time.Now()
	query := `
	UPDATE jobs
	SET status = ?,
	    error_message = ?,
	    updated_at = ?,
	    processed_at = CASE WHEN ? IN ('completed', 'failed') THEN ? ELSE processed_at END
	WHERE id = ?`

	_, err := s.db.Exec(query, status, errMsg, now, status, now, id)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}
	return nil
}

// ListPendingJobs returns all jobs that haven't reached a terminal state.
func (s *SQLiteStorage) ListPendingJobs() ([]*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status IN ('pending', 'processing')
	ORDER BY priority DESC, created_at ASC`

	return s.queryJobs(query)
}

// Close closes the underlying database connection.
func (s *SQLiteStorage) Close() error {
	return s.db.Close()
}

// ListRecentJobs returns the most recent jobs, newest first.
func (s *SQLiteStorage) ListRecentJobs(limit int) ([]*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	ORDER BY created_at DESC
	LIMIT ?`

	return s.queryJobs(query, limit)
}

// GetProcessingJobs returns all jobs currently being processed.
func (s *SQLiteStorage) GetProcessingJobs() ([]*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status = 'processing'
	ORDER BY created_at ASC`

	return s.queryJobs(query)
}

// ListAllPendingJobs returns all jobs with status 'pending' (not processing).
func (s *SQLiteStorage) ListAllPendingJobs() ([]*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status = 'pending'
	ORDER BY priority DESC, created_at ASC`

	return s.queryJobs(query)
}

// ListPendingByQueue returns pending jobs for a specific queue type.
func (s *SQLiteStorage) ListPendingByQueue(queueType string) ([]*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status = 'pending' AND queue_type = ?
	ORDER BY priority DESC, created_at ASC`

	return s.queryJobs(query, queueType)
}

// ListProcessingByQueue returns processing jobs for a specific queue type.
func (s *SQLiteStorage) ListProcessingByQueue(queueType string) ([]*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status = 'processing' AND queue_type = ?
	ORDER BY created_at ASC`

	return s.queryJobs(query, queueType)
}

// queryJobs runs a query and returns the list of jobs.
// Handles common scan logic for all list methods.
func (s *SQLiteStorage) queryJobs(query string, args ...interface{}) ([]*models.Job, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job row: %w", err)
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// UpdateJobRetry updates the retry count and status of a job.
func (s *SQLiteStorage) UpdateJobRetry(id string, status string, retryCount int, errMsg string) error {
	now := time.Now()
	query := `
	UPDATE jobs
	SET status = ?,
	    retry_count = ?,
	    error_message = ?,
	    updated_at = ?,
	    processed_at = CASE WHEN ? IN ('completed', 'failed') THEN ? ELSE processed_at END
	WHERE id = ?`

	_, err := s.db.Exec(query, status, retryCount, errMsg, now, status, now, id)
	if err != nil {
		return fmt.Errorf("failed to update job retry: %w", err)
	}
	return nil
}

// Requeue resets a preempted job back to pending without penalizing it.
func (s *SQLiteStorage) Requeue(id string) error {
	now := time.Now()
	query := `
	UPDATE jobs
	SET status = 'pending',
	    updated_at = ?
	WHERE id = ?`

	_, err := s.db.Exec(query, now, id)
	if err != nil {
		return fmt.Errorf("failed to requeue job: %w", err)
	}
	return nil
}

// ResetForRetry gives a failed job more retries and puts it back in the queue.
func (s *SQLiteStorage) ResetForRetry(id string, extraRetries int) error {
	now := time.Now()
	query := `
	UPDATE jobs
	SET status = 'pending',
	    retry_count = 0,
	    max_retries = max_retries + ?,
	    error_message = '',
	    updated_at = ?
	WHERE id = ? AND status = 'failed'`

	result, err := s.db.Exec(query, extraRetries, now, id)
	if err != nil {
		return fmt.Errorf("failed to reset job for retry: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("job not found or not in failed state")
	}
	return nil
}

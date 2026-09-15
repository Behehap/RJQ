package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"rjq/pkg/models"

	_ "github.com/lib/pq"
)

// PostgresStorage implements Storage using PostgreSQL as the backend.
type PostgresStorage struct {
	db *sql.DB
}

// NewPostgresStorage opens the Postgres database at dsn, runs the schema
// migration, and returns a ready-to-use storage instance.
//
// dsn example: postgres://user:pass@localhost:5432/rjq?sslmode=disable
func NewPostgresStorage(dsn string) (*PostgresStorage, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	if err := pgMigrate(db); err != nil {
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return &PostgresStorage{db: db}, nil
}

func pgMigrate(db *sql.DB) error {
	query := `
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
	`
	_, err := db.Exec(query)
	return err
}

func pgScanJob(scanner interface {
	Scan(dest ...interface{}) error
}) (*models.Job, error) {
	job := &models.Job{}
	var payloadBytes []byte
	var processedAt sql.NullTime
	var errorMsg sql.NullString

	err := scanner.Scan(
		&job.ID,
		&job.JobType,
		&payloadBytes,
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

	if len(payloadBytes) > 0 {
		if err := json.Unmarshal(payloadBytes, &job.Payload); err != nil {
			return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
		}
	}

	if processedAt.Valid {
		job.ProcessedAt = &processedAt.Time
	}
	if errorMsg.Valid {
		job.ErrorMessage = errorMsg.String
	}

	return job, nil
}

func (s *PostgresStorage) SaveJob(job *models.Job) error {
	payloadBytes, err := json.Marshal(job.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	query := `
	INSERT INTO jobs (id, job_type, payload, queue_type, priority, status, retry_count, max_retries, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	_, err = s.db.Exec(query,
		job.ID,
		job.JobType,
		payloadBytes,
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

func (s *PostgresStorage) GetJob(id string) (*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs WHERE id = $1`

	job, err := pgScanJob(s.db.QueryRow(query, id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}
	return job, nil
}

func (s *PostgresStorage) UpdateJobStatus(id string, status string, errMsg string) error {
	now := time.Now()
	query := `
	UPDATE jobs
	SET status = $1,
	    error_message = $2,
	    updated_at = $3,
	    processed_at = CASE WHEN $1 IN ('completed', 'failed') THEN $3 ELSE processed_at END
	WHERE id = $4`

	_, err := s.db.Exec(query, status, errMsg, now, id)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}
	return nil
}

func (s *PostgresStorage) UpdateJobRetry(id string, status string, retryCount int, errMsg string) error {
	now := time.Now()
	query := `
	UPDATE jobs
	SET status = $1,
	    retry_count = $2,
	    error_message = $3,
	    updated_at = $4,
	    processed_at = CASE WHEN $1 IN ('completed', 'failed') THEN $4 ELSE processed_at END
	WHERE id = $5`

	_, err := s.db.Exec(query, status, retryCount, errMsg, now, id)
	if err != nil {
		return fmt.Errorf("failed to update job retry: %w", err)
	}
	return nil
}

func (s *PostgresStorage) Requeue(id string) error {
	now := time.Now()
	query := `UPDATE jobs SET status = 'pending', updated_at = $1 WHERE id = $2`
	_, err := s.db.Exec(query, now, id)
	if err != nil {
		return fmt.Errorf("failed to requeue job: %w", err)
	}
	return nil
}

func (s *PostgresStorage) ResetForRetry(id string, extraRetries int) error {
	now := time.Now()
	query := `
	UPDATE jobs
	SET status = 'pending',
	    retry_count = 0,
	    max_retries = max_retries + $1,
	    error_message = '',
	    updated_at = $2
	WHERE id = $3 AND status = 'failed'`

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

func (s *PostgresStorage) ListPendingJobs() ([]*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status IN ('pending', 'processing')
	ORDER BY priority DESC, created_at ASC`
	return s.pgQueryJobs(query)
}

func (s *PostgresStorage) ListAllPendingJobs() ([]*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status = 'pending'
	ORDER BY priority DESC, created_at ASC`
	return s.pgQueryJobs(query)
}

func (s *PostgresStorage) ListRecentJobs(limit int) ([]*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	ORDER BY created_at DESC
	LIMIT $1`
	return s.pgQueryJobs(query, limit)
}

func (s *PostgresStorage) GetProcessingJobs() ([]*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status = 'processing'
	ORDER BY created_at ASC`
	return s.pgQueryJobs(query)
}

func (s *PostgresStorage) ListPendingByQueue(queueType string) ([]*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status = 'pending' AND queue_type = $1
	ORDER BY priority DESC, created_at ASC`
	return s.pgQueryJobs(query, queueType)
}

func (s *PostgresStorage) ListProcessingByQueue(queueType string) ([]*models.Job, error) {
	query := `
	SELECT id, job_type, payload, queue_type, priority, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status = 'processing' AND queue_type = $1
	ORDER BY created_at ASC`
	return s.pgQueryJobs(query, queueType)
}

func (s *PostgresStorage) Close() error {
	return s.db.Close()
}

func (s *PostgresStorage) pgQueryJobs(query string, args ...interface{}) ([]*models.Job, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		job, err := pgScanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job row: %w", err)
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

package storage

import (
	"database/sql"
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
func migrate(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		to_email TEXT NOT NULL,
		subject TEXT NOT NULL,
		body TEXT NOT NULL,
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

// SaveJob inserts a new job into the database.
func (s *SQLiteStorage) SaveJob(job *models.Job) error {
	query := `
	INSERT INTO jobs (id, to_email, subject, body, status, retry_count, max_retries, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.Exec(query,
		job.ID,
		job.ToEmail,
		job.Subject,
		job.Body,
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
	SELECT id, to_email, subject, body, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs WHERE id = ?`

	job := &models.Job{}
	var processedAt sql.NullTime
	var errorMsg sql.NullString

	err := s.db.QueryRow(query, id).Scan(
		&job.ID,
		&job.ToEmail,
		&job.Subject,
		&job.Body,
		&job.Status,
		&job.RetryCount,
		&job.MaxRetries,
		&job.CreatedAt,
		&job.UpdatedAt,
		&processedAt,
		&errorMsg,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	if processedAt.Valid {
		job.ProcessedAt = &processedAt.Time
	}
	if errorMsg.Valid {
		job.ErrorMessage = errorMsg.String
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
	SELECT id, to_email, subject, body, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status IN ('pending', 'processing')
	ORDER BY created_at ASC`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to list pending jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		job := &models.Job{}
		var processedAt sql.NullTime
		var errorMsg sql.NullString

		err := rows.Scan(
			&job.ID,
			&job.ToEmail,
			&job.Subject,
			&job.Body,
			&job.Status,
			&job.RetryCount,
			&job.MaxRetries,
			&job.CreatedAt,
			&job.UpdatedAt,
			&processedAt,
			&errorMsg,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job row: %w", err)
		}

		if processedAt.Valid {
			job.ProcessedAt = &processedAt.Time
		}
		if errorMsg.Valid {
			job.ErrorMessage = errorMsg.String
		}

		jobs = append(jobs, job)
	}

	return jobs, rows.Err()
}

// Close closes the underlying database connection.
func (s *SQLiteStorage) Close() error {
	return s.db.Close()
}

// ListRecentJobs returns the most recent jobs, newest first.
func (s *SQLiteStorage) ListRecentJobs(limit int) ([]*models.Job, error) {
	query := `
	SELECT id, to_email, subject, body, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	ORDER BY created_at DESC
	LIMIT ?`

	rows, err := s.db.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list recent jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		job := &models.Job{}
		var processedAt sql.NullTime
		var errorMsg sql.NullString

		err := rows.Scan(
			&job.ID,
			&job.ToEmail,
			&job.Subject,
			&job.Body,
			&job.Status,
			&job.RetryCount,
			&job.MaxRetries,
			&job.CreatedAt,
			&job.UpdatedAt,
			&processedAt,
			&errorMsg,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job row: %w", err)
		}

		if processedAt.Valid {
			job.ProcessedAt = &processedAt.Time
		}
		if errorMsg.Valid {
			job.ErrorMessage = errorMsg.String
		}

		jobs = append(jobs, job)
	}

	return jobs, rows.Err()
}

// GetProcessingJobs returns all jobs currently being processed.
func (s *SQLiteStorage) GetProcessingJobs() ([]*models.Job, error) {
	query := `
	SELECT id, to_email, subject, body, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status = 'processing'
	ORDER BY created_at ASC`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to list processing jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		job := &models.Job{}
		var processedAt sql.NullTime
		var errorMsg sql.NullString

		err := rows.Scan(
			&job.ID, &job.ToEmail, &job.Subject, &job.Body,
			&job.Status, &job.RetryCount, &job.MaxRetries,
			&job.CreatedAt, &job.UpdatedAt, &processedAt, &errorMsg,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job row: %w", err)
		}
		if processedAt.Valid {
			job.ProcessedAt = &processedAt.Time
		}
		if errorMsg.Valid {
			job.ErrorMessage = errorMsg.String
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *SQLiteStorage) ListAllPendingJobs() ([]*models.Job, error) {
	query := `
	SELECT id, to_email, subject, body, status, retry_count, max_retries,
	       created_at, updated_at, processed_at, error_message
	FROM jobs
	WHERE status = 'pending'
	ORDER BY created_at ASC`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to list all pending jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		job := &models.Job{}
		var processedAt sql.NullTime
		var errorMsg sql.NullString

		err := rows.Scan(
			&job.ID, &job.ToEmail, &job.Subject, &job.Body,
			&job.Status, &job.RetryCount, &job.MaxRetries,
			&job.CreatedAt, &job.UpdatedAt, &processedAt, &errorMsg,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job row: %w", err)
		}
		if processedAt.Valid {
			job.ProcessedAt = &processedAt.Time
		}
		if errorMsg.Valid {
			job.ErrorMessage = errorMsg.String
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

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

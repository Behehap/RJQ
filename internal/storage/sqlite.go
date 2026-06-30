package storage

import (
	"database/sql"
	"fmt"
	"rjq/pkg/models"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteStorage imptemts storage using SQLite as the backend.
// It uses a single *sql.DB connection pool, which is safe for
// concurrent goroutines by defailt.

type SQLiteStorage struct {
	db *sql.DB
}

// NewSQLiteStorage opens the SQlite databse at dsn, runs the schema
// migration, and returns a read to use storage instace.

// The dsn (Data Source Namw) can include query parameters recognized
// by the SQLite driver, e.g. "file:RJQ.db?chache=share&_journal_mode=WAL".

func NewSQLiteStorage(dsn string) (*SQLiteStorage, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("Failed to open the database: %w,", err)
	}

	// Connection pool tuning for SQLite.
	// SQlite only suppots one write at a time, the number must be kept low.
	db.SetMaxOpenConns(1)
	db.SetConnMaxIdleTime(1)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("Failed to ping database: %w", err)
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

// / ListPendingJObs returns all jobs that haven't reached a termnal state.
func (s *SQLiteStorage) ListPendingJObs() ([]*models.Job, error) {
	query := `
	SELECT id, to_email, subject, body, status, retry_count, max_retries, created_at, updated_at, processed_at, error_message
	From jobs
	WHERE status IN('pending','processing')
	ORDER BY created_at ASC
	`

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

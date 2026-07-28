// Package storage defines the persistence layer for jobs.
// The Storage interface allows swapping backends (SQLite, Postgres, etc.)
// without changing any consuming code.
package storage

import "rjq/pkg/models"

// Storage is the data access layer for jobs.
// All implementations must be safe for concurrent use.
type Storage interface {
	// SaveJob persists a new job. Returns an error if a job with the
	// same ID already exists.
	SaveJob(job *models.Job) error

	// GetJob retrieves a job by ID. Returns nil and no error if the
	// job doesn't exist.
	GetJob(id string) (*models.Job, error)

	// UpdateJobStatus changes the status of a job and optionally sets
	// the error message. It also bumps UpdatedAt and, if the status is
	// "completed" or "failed", sets ProcessedAt.
	UpdateJobStatus(id string, status string, errMsg string) error

	// ListPendingJobs returns all jobs with status "pending" or
	// "processing". Used for recovery on restart.
	ListPendingJobs() ([]*models.Job, error)

	// Close releases any resources held by the storage backend.
	Close() error

	// ListRecentJobs returns the most recent jobs, newest first.
	ListRecentJobs(limit int) ([]*models.Job, error)

	// GetProcessingJobs returns all jobs currently being processed.
	GetProcessingJobs() ([]*models.Job, error)

	// ListAllPendingJobs returns all jobs with status 'pending' (not processing).
	ListAllPendingJobs() ([]*models.Job, error)

	// UpdateJobRetry increments the retry count and updates status.
	UpdateJobRetry(id string, status string, retryCount int, errMsg string) error

	// RequeueAtFront returns a job to pending status without incrementing
	// retry_count. Used for preempted jobs.
	RequeueAtFront(id string) error

	// ListPendingByQueue returns pending jobs for a specific queue type.
	ListPendingByQueue(queueType string) ([]*models.Job, error)

	// ListProcessingByQueue returns processing jobs for a specific queue type.
	ListProcessingByQueue(queueType string) ([]*models.Job, error)

	// ResetForRetry resets a failed job back to pending with additional retries.
	ResetForRetry(id string, extraRetries int) error
}

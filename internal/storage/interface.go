// Package storage defines the persistence layer for jobs.

package storage

import (
	"rjq/pkg/models"
)

// storage is the data access layer for jobs.
// ALl implementations must be safe for concurrent use.
type Storage interface {
	// SaveJob persists a new job. Returns an error if a job with the
	// same ID already exists.
	SaveJob(job *models.Job) error

	// GetJob retrieves a job by ID. Returns nil and no error if the
	// job doesn't exist.
	GetJob(id string) (*models.Job, error)

	// UpdateJobStatus changes the status of a job and optionally sets
	// the error message. It also bumps UpdateAt and, if the status is
	// "completed" or "failed", sets ProcessedAt.

	UpdateJobStatus(id string, status string, errMsg string) error

	// ListPendingJobs returns all jobs with status "pending" or
	// "Proccessong". Used for recovery on restart.
	ListPendingJobs() ([]*models.Job, error)

	// Close releases ant resources held by the storage backend.
	Close() error

	// ListRecentJobs returns the most recent jobs, newest first.
	ListRecentJobs(limit int) ([]*models.Job, error)
}

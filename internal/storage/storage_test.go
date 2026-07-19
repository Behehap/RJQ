package storage

import (
	"testing"
	"time"

	"rjq/pkg/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestStorage creates an in-memory SQLite storage for testing.
// ":memory:" tells SQLite to use a temporary database that is
// destroyed when the connection closes.
func setupTestStorage(t *testing.T) *SQLiteStorage {
	t.Helper()
	store, err := NewSQLiteStorage(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		store.Close()
	})
	return store
}

// sampleJob returns a job with fixed values for testing.
func sampleJob() *models.Job {
	now := time.Now()
	return &models.Job{
		ID:         "test-id-1",
		ToEmail:    "test@example.com",
		Subject:    "Test Subject",
		Body:       "Test Body",
		Status:     models.StatusPending,
		RetryCount: 0,
		MaxRetries: 3,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

func TestSaveJob(t *testing.T) {
	store := setupTestStorage(t)
	job := sampleJob()

	err := store.SaveJob(job)
	assert.NoError(t, err)
}

func TestGetJob_Exists(t *testing.T) {
	store := setupTestStorage(t)
	job := sampleJob()
	require.NoError(t, store.SaveJob(job))

	result, err := store.GetJob(job.ID)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, job.ID, result.ID)
	assert.Equal(t, job.ToEmail, result.ToEmail)
	assert.Equal(t, job.Status, result.Status)
}

func TestGetJob_NotFound(t *testing.T) {
	store := setupTestStorage(t)

	result, err := store.GetJob("nonexistent")
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestUpdateJobStatus_Completed(t *testing.T) {
	store := setupTestStorage(t)
	job := sampleJob()
	require.NoError(t, store.SaveJob(job))

	err := store.UpdateJobStatus(job.ID, models.StatusCompleted, "")
	require.NoError(t, err)

	updated, err := store.GetJob(job.ID)
	require.NoError(t, err)
	assert.Equal(t, models.StatusCompleted, updated.Status)
	assert.NotNil(t, updated.ProcessedAt)
}

func TestUpdateJobStatus_Failed(t *testing.T) {
	store := setupTestStorage(t)
	job := sampleJob()
	require.NoError(t, store.SaveJob(job))

	err := store.UpdateJobStatus(job.ID, models.StatusFailed, "SMTP connection refused")
	require.NoError(t, err)

	updated, err := store.GetJob(job.ID)
	require.NoError(t, err)
	assert.Equal(t, models.StatusFailed, updated.Status)
	assert.Equal(t, "SMTP connection refused", updated.ErrorMessage)
	assert.NotNil(t, updated.ProcessedAt)
}

func TestListPendingJobs(t *testing.T) {
	store := setupTestStorage(t)

	// Save two pending and one completed job.
	j1 := sampleJob()
	j1.ID = "pending-1"
	require.NoError(t, store.SaveJob(j1))

	j2 := sampleJob()
	j2.ID = "pending-2"
	require.NoError(t, store.SaveJob(j2))

	j3 := sampleJob()
	j3.ID = "completed-1"
	j3.Status = models.StatusCompleted
	require.NoError(t, store.SaveJob(j3))

	pending, err := store.ListPendingJobs()
	require.NoError(t, err)
	assert.Len(t, pending, 2)
}

func TestDuplicateSave(t *testing.T) {
	store := setupTestStorage(t)
	job := sampleJob()
	require.NoError(t, store.SaveJob(job))

	// Saving the same ID again should fail (PRIMARY KEY constraint).
	err := store.SaveJob(job)
	assert.Error(t, err)
}

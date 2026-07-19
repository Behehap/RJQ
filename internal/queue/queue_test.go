package queue

import (
	"testing"
	"time"

	"rjq/internal/storage"
	"rjq/pkg/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestQueue creates a MemoryQueue backed by an in-memory SQLite storage.
func setupTestQueue(t *testing.T) (*MemoryQueue, *storage.SQLiteStorage) {
	t.Helper()
	store, err := storage.NewSQLiteStorage(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		store.Close()
	})

	q := NewMemoryQueue(store, 10)
	return q, store
}

// testJob returns a job with a unique ID for testing.
func testJob(id string) *models.Job {
	now := time.Now()
	return &models.Job{
		ID:         id,
		ToEmail:    "test@example.com",
		Subject:    "Subject",
		Body:       "Body",
		Status:     models.StatusPending,
		RetryCount: 0,
		MaxRetries: 3,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

func TestEnqueueDequeue(t *testing.T) {
	q, store := setupTestQueue(t)
	job := testJob("job-1")
	require.NoError(t, store.SaveJob(job))
	require.NoError(t, q.Enqueue(job))

	result, err := q.Dequeue()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "job-1", result.ID)
}

func TestAck(t *testing.T) {
	q, store := setupTestQueue(t)
	job := testJob("job-2")
	require.NoError(t, store.SaveJob(job))
	require.NoError(t, q.Enqueue(job))

	_, err := q.Dequeue()
	require.NoError(t, err)

	err = q.Ack("job-2")
	require.NoError(t, err)

	updated, err := store.GetJob("job-2")
	require.NoError(t, err)
	assert.Equal(t, models.StatusCompleted, updated.Status)
}

func TestNack_Retry(t *testing.T) {
	q, store := setupTestQueue(t)
	job := testJob("job-3")
	require.NoError(t, store.SaveJob(job))
	require.NoError(t, q.Enqueue(job))

	_, err := q.Dequeue()
	require.NoError(t, err)

	err = q.Nack("job-3")
	require.NoError(t, err)

	// Job should be re-enqueued with pending status.
	updated, err := store.GetJob("job-3")
	require.NoError(t, err)
	assert.Equal(t, models.StatusPending, updated.Status)
	assert.Equal(t, 1, updated.RetryCount)

	// Should be able to dequeue it again.
	result, err := q.Dequeue()
	require.NoError(t, err)
	assert.Equal(t, "job-3", result.ID)
}

func TestNack_MaxRetries(t *testing.T) {
	q, store := setupTestQueue(t)
	job := testJob("job-4")
	job.MaxRetries = 2
	require.NoError(t, store.SaveJob(job))
	require.NoError(t, q.Enqueue(job))

	// First attempt.
	_, err := q.Dequeue()
	require.NoError(t, err)
	require.NoError(t, q.Nack("job-4"))

	// Second attempt.
	_, err = q.Dequeue()
	require.NoError(t, err)
	require.NoError(t, q.Nack("job-4"))

	// Third Nack should exhaust retries.
	_, err = q.Dequeue()
	require.NoError(t, err)
	require.NoError(t, q.Nack("job-4"))

	updated, err := store.GetJob("job-4")
	require.NoError(t, err)
	assert.Equal(t, models.StatusFailed, updated.Status)
	assert.Equal(t, 3, updated.RetryCount)
}

func TestPendingCount(t *testing.T) {
	q, store := setupTestQueue(t)

	j1 := testJob("job-5")
	j2 := testJob("job-6")
	require.NoError(t, store.SaveJob(j1))
	require.NoError(t, store.SaveJob(j2))
	require.NoError(t, q.Enqueue(j1))
	require.NoError(t, q.Enqueue(j2))

	assert.Equal(t, 2, q.PendingCount())

	q.Dequeue()
	assert.Equal(t, 1, q.PendingCount())

	q.Dequeue()
	assert.Equal(t, 0, q.PendingCount())
}

func TestClose(t *testing.T) {
	q, _ := setupTestQueue(t)

	err := q.Close()
	require.NoError(t, err)

	// Dequeue on closed queue returns nil.
	job, err := q.Dequeue()
	assert.NoError(t, err)
	assert.Nil(t, job)

	// Enqueue on closed queue returns error.
	err = q.Enqueue(testJob("job-7"))
	assert.Error(t, err)
}

func TestRecover(t *testing.T) {
	store, err := storage.NewSQLiteStorage(":memory:")
	require.NoError(t, err)
	defer store.Close()

	// Save two pending jobs and one processing (simulating a crash).
	j1 := testJob("recover-1")
	require.NoError(t, store.SaveJob(j1))

	j2 := testJob("recover-2")
	j2.Status = models.StatusProcessing
	require.NoError(t, store.SaveJob(j2))

	j3 := testJob("recover-3")
	j3.Status = models.StatusCompleted
	require.NoError(t, store.SaveJob(j3))

	// Create a fresh queue and recover.
	q := NewMemoryQueue(store, 10)
	err = q.Recover()
	require.NoError(t, err)

	// Should have 2 jobs in the queue (pending + processing).
	assert.Equal(t, 2, q.PendingCount())

	// The processing job should be reset to pending.
	recovered, err := store.GetJob("recover-2")
	require.NoError(t, err)
	assert.Equal(t, models.StatusPending, recovered.Status)
}

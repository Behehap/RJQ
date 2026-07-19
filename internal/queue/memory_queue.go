package queue

import (
	"fmt"
	"rjq/internal/storage"
	"rjq/pkg/models"
	"sync"
)

// MemoryQueue implements Queue using a buffered Go channel for in-memory
// delivery and a Storage backend for persistence.

// Recovery: On startup, call Recover() to liad pending jobs from storage and
// push them into the channel.

type MemoryQueue struct {
	store  storage.Storage
	jobs   chan *models.Job
	mu     sync.Mutex
	closed bool
}

// NewMemoryQueue creates a queue backed by the given storage.
// bufferSize controls how many jobs can wait in memory before Enqueue blocks.
func NewMemoryQueue(store storage.Storage, buffersize int) *MemoryQueue {
	return &MemoryQueue{
		store: store,
		jobs:  make(chan *models.Job, buffersize),
	}
}

// Enqueue pushed a job into the chanel. The job must already be saved
// to storage bt the caller.

func (q *MemoryQueue) Enqueue(job *models.Job) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return fmt.Errorf("queue is closed")
	}
	q.mu.Unlock()

	q.jobs <- job

	return nil
}

// Dequese blocks until a job is available. If the queue is closed and
// empty, it returns nil.

func (q *MemoryQueue) Dequeue() (*models.Job, error) {
	job, ok := <-q.jobs
	if !ok {
		return nil, nil
	}
	return job, nil
}

// Ack updates the job status to completed.
func (q *MemoryQueue) Ack(jobID string) error {
	return q.store.UpdateJobStatus(jobID, models.StatusCompleted, "")
}

// Nack updates the job status to failed if max retries reached,
// otherwise resets it to pending for re-processing.

func (q *MemoryQueue) Nack(jobID string) error {
	job, err := q.store.GetJob(jobID)
	if err != nil {
		return fmt.Errorf("nack: failed to get job: %w", err)
	}
	if job == nil {
		return fmt.Errorf("nack: job %s not found", jobID)
	}

	job.RetryCount++
	if job.RetryCount >= job.MaxRetries {
		return q.store.UpdateJobStatus(jobID, models.StatusFailed,
			fmt.Sprintf("exhausted %d retries", job.RetryCount))
	}

	// Re-queue for another attempt.
	if err := q.store.UpdateJobStatus(jobID, models.StatusPending, ""); err != nil {
		return err
	}
	job.Status = models.StatusPending
	return q.Enqueue(job)
}

// PendingCOunt returns the number of jobs currently in the channel.
func (q *MemoryQueue) PendingCount() int {
	return len(q.jobs)
}

// Close shuts down the channel. Workers reading from Dequeue will recieve nil
// once the cahnnel drains.

func (q *MemoryQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.jobs)
	}
	return nil
}

// Recover loads all pending and processing jobs from storage and
// enqueues them. Call this once on startup before starting workers.
func (q *MemoryQueue) Recover() error {
	jobs, err := q.store.ListPendingJobs()
	if err != nil {
		return fmt.Errorf("recover: failed to list pending jobs: %w", err)
	}
	for _, job := range jobs {
		// Reset processing jobs back to pending — the worker that
		// held them is gone.
		if job.Status == models.StatusProcessing {
			if err := q.store.UpdateJobStatus(job.ID, models.StatusPending, ""); err != nil {
				return fmt.Errorf("recover: failed to reset job %s: %w", job.ID, err)
			}
			job.Status = models.StatusPending
		}
		if err := q.Enqueue(job); err != nil {
			return fmt.Errorf("recover: failed to enqueue job %s: %w", job.ID, err)
		}
	}
	return nil
}

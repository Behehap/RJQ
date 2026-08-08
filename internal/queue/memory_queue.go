package queue

import (
	"fmt"
	"rjq/internal/storage"
	"rjq/pkg/models"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// MemoryQueue implements Queue using a buffered Go channel for in-memory
// delivery and a Storage backend for persistence.
//
// Recovery: On startup, call Recover() to load pending jobs from storage and
// push them into the channel.
type MemoryQueue struct {
	store  storage.Storage
	jobs   chan *models.Job
	mu     sync.Mutex
	wg     sync.WaitGroup // tracks in-flight Enqueue sends
	closed bool
	done   chan struct{}
}

// NewMemoryQueue creates a queue backed by the given storage.
// bufferSize controls how many jobs can wait in memory before Enqueue blocks.
func NewMemoryQueue(store storage.Storage, bufferSize int) *MemoryQueue {
	return &MemoryQueue{
		store: store,
		jobs:  make(chan *models.Job, bufferSize),
		done:  make(chan struct{}),
	}
}

// Enqueue pushes a job into the channel. The job must already be saved
// to storage by the caller.
func (q *MemoryQueue) Enqueue(job *models.Job) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return fmt.Errorf("queue is closed")
	}
	// Register an in-flight send before releasing the lock.
	// This ensures Close() will wait for us to finish.
	q.wg.Add(1)
	q.mu.Unlock()
	defer q.wg.Done()

	select {
	case q.jobs <- job:
		return nil
	case <-q.done:
		return fmt.Errorf("queue is closing")
	}

}

// Dequeue blocks until a job is available. If the queue is closed and
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
		return q.store.UpdateJobRetry(jobID, models.StatusFailed, job.RetryCount,
			fmt.Sprintf("exhausted %d retries", job.RetryCount))
	}

	if err := q.store.UpdateJobRetry(jobID, models.StatusPending, job.RetryCount, ""); err != nil {
		return err
	}
	job.Status = models.StatusPending
	return q.Enqueue(job)
}

// PendingCount returns the number of jobs currently in the channel.
func (q *MemoryQueue) PendingCount() int {
	return len(q.jobs)
}

// Close shuts down the channel. Workers reading from Dequeue will receive nil
// once the channel drains. It waits for any in-flight Enqueue calls to finish.
func (q *MemoryQueue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil // already closed
	}
	q.closed = true
	close(q.done)
	q.mu.Unlock()

	// Wait for all Enqueue calls that observed !closed to complete their sends.
	q.wg.Wait()
	close(q.jobs)
	return nil
}

// Recover loads all pending and processing jobs from storage and enqueues them.
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

// SetProcessing marks a job as processing in storage.
func (q *MemoryQueue) SetProcessing(jobID string) error {
	return q.store.UpdateJobStatus(jobID, models.StatusProcessing, "")
}

// Requeue resets a preempted job and enqueues it.
func (q *MemoryQueue) Requeue(id string) error {
	if err := q.store.Requeue(id); err != nil {
		return err
	}
	job, err := q.store.GetJob(id)
	if err != nil || job == nil {
		return fmt.Errorf("requeue: job %s not found", id)
	}
	job.Status = models.StatusPending
	return q.Enqueue(job)
}

// Retry gives a failed job more retries and re-enqueues it.
func (q *MemoryQueue) Retry(jobID string, extraRetries int) error {
	if err := q.store.ResetForRetry(jobID, extraRetries); err != nil {
		return err
	}
	job, err := q.store.GetJob(jobID)
	if err != nil || job == nil {
		return fmt.Errorf("retry: job %s not found", jobID)
	}
	job.Status = models.StatusPending
	return q.Enqueue(job)
}

// StartSweeper periodically re-enqueues orphaned pending jobs and resets
// stuck processing jobs. Call it once after creating the queue.
func (q *MemoryQueue) StartSweeper(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			q.sweep()
		}
	}()
}

// sweep performs one pass of the sweeper.
func (q *MemoryQueue) sweep() {
	// Fetch all jobs that are not in a terminal state.
	jobs, err := q.store.ListPendingJobs() // returns pending + processing
	if err != nil {
		// Log error, but don't crash the sweeper.
		log.WithError(err).Error("Sweeper: failed to list pending jobs")
		return
	}

	for _, job := range jobs {
		switch job.Status {
		case models.StatusPending:
			// Orphaned job: saved to DB but not in the channel.
			// Try to enqueue it. Ignore errors (channel might be full or closing).
			if err := q.Enqueue(job); err != nil {
				log.WithFields(log.Fields{
					"job_id": job.ID,
					"error":  err,
				}).Debug("Sweeper: could not enqueue pending job")
			}
		case models.StatusProcessing:
			// Stuck processing job: worker crashed after SetProcessing.
			// Reset to pending if it's been stuck for > 10 minutes.
			if time.Since(job.UpdatedAt) > 10*time.Minute {
				log.WithField("job_id", job.ID).Warn("Sweeper: resetting stuck processing job")
				if err := q.store.UpdateJobStatus(job.ID, models.StatusPending, ""); err != nil {
					log.WithError(err).Error("Sweeper: failed to reset processing job")
					continue
				}
				job.Status = models.StatusPending
				if err := q.Enqueue(job); err != nil {
					log.WithError(err).Error("Sweeper: failed to enqueue reset job")
				}
			}
		}
	}
}

package queue

import (
	"fmt"
	"sync"
	"time"

	"rjq/internal/storage"
	"rjq/pkg/models"

	log "github.com/sirupsen/logrus"
)

// SortedQueue is a priority-aware in-memory queue. Unlike MemoryQueue
// (which is FIFO via a channel), this queue keeps jobs ordered by
// (priority DESC, created_at ASC) so urgent jobs jump ahead of normals.
type SortedQueue struct {
	store storage.Storage

	mu     sync.Mutex
	jobs   []*models.Job
	signal chan struct{} // buffered size 1; signals waiters that a job is ready
	closed bool
	done   chan struct{}
}

// NewSortedQueue creates a sorted queue backed by the given storage.
func NewSortedQueue(store storage.Storage, bufferSize int) *SortedQueue {
	return &SortedQueue{
		store:  store,
		signal: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
}

// Enqueue inserts a job in sorted position and signals any waiting Dequeue.
func (q *SortedQueue) Enqueue(job *models.Job) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return fmt.Errorf("queue is closed")
	}
	q.insertLocked(job)
	q.mu.Unlock()

	// non-blocking wakeup for a single waiter
	select {
	case q.signal <- struct{}{}:
	default:
	}
	return nil
}

// insertLocked places the job so the slice stays sorted by priority DESC,
// then created_at ASC. Caller must hold q.mu.
func (q *SortedQueue) insertLocked(job *models.Job) {
	i := 0
	for i < len(q.jobs) {
		cur := q.jobs[i]
		if job.Priority > cur.Priority {
			break
		}
		if job.Priority == cur.Priority && job.CreatedAt.Before(cur.CreatedAt) {
			break
		}
		i++
	}
	q.jobs = append(q.jobs, nil)
	copy(q.jobs[i+1:], q.jobs[i:])
	q.jobs[i] = job
}

// popLocked removes and returns the highest-priority job, or nil if empty.
// Caller must hold q.mu.
func (q *SortedQueue) popLocked() *models.Job {
	if len(q.jobs) == 0 {
		return nil
	}
	job := q.jobs[0]
	q.jobs = q.jobs[1:]
	return job
}

// Dequeue blocks until a job is available.
func (q *SortedQueue) Dequeue() (*models.Job, error) {
	for {
		q.mu.Lock()
		if job := q.popLocked(); job != nil {
			q.mu.Unlock()
			return job, nil
		}
		closed := q.closed
		q.mu.Unlock()

		if closed {
			return nil, nil
		}

		select {
		case <-q.signal:
		case <-q.done:
			return nil, nil
		}
	}
}

// DequeueOrWait waits up to d for a job. Returns nil if none arrived.
func (q *SortedQueue) DequeueOrWait(d time.Duration) (*models.Job, error) {
	q.mu.Lock()
	if job := q.popLocked(); job != nil {
		q.mu.Unlock()
		return job, nil
	}
	closed := q.closed
	q.mu.Unlock()

	if closed {
		return nil, nil
	}

	select {
	case <-q.signal:
	case <-q.done:
		return nil, nil
	case <-time.After(d):
		return nil, nil
	}

	q.mu.Lock()
	job := q.popLocked()
	q.mu.Unlock()
	return job, nil
}

// Ack updates the job status to completed.
func (q *SortedQueue) Ack(jobID string) error {
	return q.store.UpdateJobStatus(jobID, models.StatusCompleted, "")
}

// Nack updates the job status to failed if max retries reached,
// otherwise resets it to pending for re-processing.
func (q *SortedQueue) Nack(jobID string) error {
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

// PendingCount returns the number of jobs currently in the queue.
func (q *SortedQueue) PendingCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}

// Close shuts the queue down. Waiting Dequeue calls return nil.
func (q *SortedQueue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	close(q.done)
	q.mu.Unlock()
	return nil
}

// Recover loads pending and processing priority jobs from storage.
func (q *SortedQueue) Recover() error {
	jobs, err := q.store.ListPendingJobs()
	if err != nil {
		return fmt.Errorf("recover: failed to list pending jobs: %w", err)
	}
	for _, job := range jobs {
		if job.QueueType != models.QueueTypePriority {
			continue
		}
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
func (q *SortedQueue) SetProcessing(jobID string) error {
	return q.store.UpdateJobStatus(jobID, models.StatusProcessing, "")
}

// Requeue resets a preempted job and enqueues it.
func (q *SortedQueue) Requeue(id string) error {
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
func (q *SortedQueue) Retry(jobID string, extraRetries int) error {
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
// stuck processing jobs.
func (q *SortedQueue) StartSweeper(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			q.sweep()
		}
	}()
}

// sweep performs one pass of the sweeper.
func (q *SortedQueue) sweep() {
	jobs, err := q.store.ListPendingJobs()
	if err != nil {
		log.WithError(err).Error("Sweeper: failed to list pending jobs")
		return
	}

	for _, job := range jobs {
		if job.QueueType != models.QueueTypePriority {
			continue
		}
		switch job.Status {
		case models.StatusPending:
			if err := q.Enqueue(job); err != nil {
				log.WithFields(log.Fields{
					"job_id": job.ID,
					"error":  err,
				}).Debug("Sweeper: could not enqueue pending job")
			}
		case models.StatusProcessing:
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

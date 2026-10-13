package queue

import (
	"fmt"
	"time"

	"rjq/pkg/models"

	log "github.com/sirupsen/logrus"
)

// Router holds all three queues and routes jobs to the correct one.
// Workers pull through the Router; they never touch a queue directly.
type Router struct {
	FIFO        *MemoryQueue
	Priority    *SortedQueue
	RateLimited *RateLimitedQueue
}

// NewRouter creates a router with all three queue types.
func NewRouter(fifo *MemoryQueue, priority *SortedQueue, rateLimited *RateLimitedQueue) *Router {
	return &Router{
		FIFO:        fifo,
		Priority:    priority,
		RateLimited: rateLimited,
	}
}

// Enqueue routes a job to the appropriate queue based on its QueueType.
func (r *Router) Enqueue(job *models.Job) error {
	switch job.QueueType {
	case models.QueueTypePriority:
		return r.Priority.Enqueue(job)
	case models.QueueTypeRateLimited:
		return r.RateLimited.Enqueue(job)
	default:
		return r.FIFO.Enqueue(job)
	}
}

// Dequeue returns the next job, preferring priority over rate-limited over FIFO.
//
// It blocks until a job is available. Returns nil if all queues are closed.
func (r *Router) Dequeue() (*models.Job, error) {
	for {
		// Priority first.
		if job, err := r.Priority.DequeueOrWait(0); err == nil && job != nil {
			return job, nil
		}

		// Rate-limited next (only if a token is available).
		if r.RateLimited.AvailableTokens() > 0 {
			if job, err := r.RateLimited.DequeueOrWait(0); err == nil && job != nil {
				return job, nil
			}
		}

		// FIFO last.
		if job, err := r.FIFO.DequeueOrWait(0); err == nil && job != nil {
			return job, nil
		}

		// Nothing available. Sleep briefly then retry.
		time.Sleep(20 * time.Millisecond)
	}
}

// DequeueOrWait waits up to d for a job. Returns nil if none arrived.
//
// The worker pool calls this in a loop so it can periodically re-check
// the preempt slots without blocking forever.
func (r *Router) DequeueOrWait(d time.Duration) (*models.Job, error) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		// Priority first.
		if job, err := r.Priority.DequeueOrWait(0); err == nil && job != nil {
			return job, nil
		}

		// Rate-limited next if token available.
		if r.RateLimited.AvailableTokens() > 0 {
			if job, err := r.RateLimited.DequeueOrWait(0); err == nil && job != nil {
				return job, nil
			}
		}

		// FIFO last.
		if job, err := r.FIFO.DequeueOrWait(0); err == nil && job != nil {
			return job, nil
		}

		// Nothing available. Short sleep then retry.
		time.Sleep(10 * time.Millisecond)
	}
	return nil, nil
}

// Ack marks a job as successfully processed.
func (r *Router) Ack(jobID string) error {
	return r.FIFO.Ack(jobID)
}

// Nack handles a failed job.
func (r *Router) Nack(jobID string) error {
	return r.FIFO.Nack(jobID)
}

// PendingCount returns the sum of pending jobs across all queues.
func (r *Router) PendingCount() int {
	return r.FIFO.PendingCount() + r.Priority.PendingCount() + r.RateLimited.PendingCount()
}

// SetProcessing marks a job as processing in storage.
func (r *Router) SetProcessing(jobID string) error {
	return r.FIFO.SetProcessing(jobID)
}

// Requeue resets a preempted job and re-enqueues it.
func (r *Router) Requeue(jobID string) error {
	return r.FIFO.Requeue(jobID)
}

// Retry gives a failed job more retries and re-enqueues it.
func (r *Router) Retry(jobID string, extraRetries int) error {
	return r.FIFO.Retry(jobID, extraRetries)
}

// Close shuts down all three queues.
func (r *Router) Close() error {
	if err := r.FIFO.Close(); err != nil {
		log.WithError(err).Error("Failed to close FIFO queue")
	}
	if err := r.Priority.Close(); err != nil {
		log.WithError(err).Error("Failed to close priority queue")
	}
	if err := r.RateLimited.Close(); err != nil {
		log.WithError(err).Error("Failed to close rate-limited queue")
	}
	return nil
}

// Recover restores pending and processing jobs on startup.
func (r *Router) Recover() error {
	if err := r.FIFO.Recover(); err != nil {
		return fmt.Errorf("fifo recover: %w", err)
	}
	if err := r.Priority.Recover(); err != nil {
		return fmt.Errorf("priority recover: %w", err)
	}
	if err := r.RateLimited.Recover(); err != nil {
		return fmt.Errorf("rate-limited recover: %w", err)
	}
	return nil
}

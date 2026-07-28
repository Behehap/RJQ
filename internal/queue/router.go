package queue

import (
	"rjq/pkg/models"
)

// Router holds all three queues and routes jobs to the correct one.
type Router struct {
	FIFO        *MemoryQueue
	Priority    *MemoryQueue
	RateLimited *RateLimitedQueue
}

// NewRouter creates a router with all three queue types.
func NewRouter(fifo, priority *MemoryQueue, rateLimited *RateLimitedQueue) *Router {
	return &Router{
		FIFO:        fifo,
		Priority:    priority,
		RateLimited: rateLimited,
	}
}

// Enqueue routes a job to the appropriate queue based on QueueType.
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

// Dequeue pulls from all queues with strict priority order:
// 1. Priority (always first — urgent matters)
// 2. Rate-limited (if token available)
// 3. FIFO (lowest priority)
func (r *Router) Dequeue() (*models.Job, error) {
	for {
		// Priority ALWAYS checked first.
		select {
		case job := <-r.Priority.jobs:
			return job, nil
		default:
		}

		// Rate-limited with token check.
		if r.RateLimited.AvailableTokens() > 0 {
			select {
			case job := <-r.RateLimited.jobs:
				r.RateLimited.mu.Lock()
				if r.RateLimited.tokens > 0 {
					r.RateLimited.tokens--
				}
				r.RateLimited.mu.Unlock()
				return job, nil
			default:
			}
		}

		// FIFO last.
		select {
		case job := <-r.FIFO.jobs:
			return job, nil
		default:
		}

		// All empty — block on all three simultaneously.
		select {
		case job := <-r.Priority.jobs:
			return job, nil
		case job := <-r.RateLimited.jobs:
			r.RateLimited.mu.Lock()
			if r.RateLimited.tokens > 0 {
				r.RateLimited.tokens--
			}
			r.RateLimited.mu.Unlock()
			return job, nil
		case job, ok := <-r.FIFO.jobs:
			if !ok {
				return nil, nil
			}
			return job, nil
		}
	}
}

// Close closes all queues.
func (r *Router) Close() error {
	r.FIFO.Close()
	r.Priority.Close()
	return r.RateLimited.Close()
}

// Ack delegates to the appropriate queue. Since Ack only needs a job ID,
// we use the FIFO queue's storage (all queues share the same storage).
func (r *Router) Ack(jobID string) error {
	return r.FIFO.Ack(jobID)
}

// Nack delegates to the FIFO queue's Nack.
func (r *Router) Nack(jobID string) error {
	return r.FIFO.Nack(jobID)
}

// PendingCount returns the sum of pending jobs across all queues.
func (r *Router) PendingCount() int {
	return r.FIFO.PendingCount() + r.Priority.PendingCount() + r.RateLimited.PendingCount()
}

// SetProcessing delegates to the FIFO queue.
func (r *Router) SetProcessing(jobID string) error {
	return r.FIFO.SetProcessing(jobID)
}

// RequeueAtFront delegates to the FIFO queue.
func (r *Router) RequeueAtFront(jobID string) error {
	return r.FIFO.RequeueAtFront(jobID)
}

// Recover delegates to all queues.
func (r *Router) Recover() error {
	if err := r.FIFO.Recover(); err != nil {
		return err
	}
	if err := r.Priority.Recover(); err != nil {
		return err
	}
	return r.RateLimited.Recover()
}

// Retry delegates to the FIFO queue's Retry.
func (r *Router) Retry(jobID string, extraRetries int) error {
	return r.FIFO.Retry(jobID, extraRetries)
}

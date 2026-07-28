package queue

import (
	"fmt"
	"sync"
	"time"

	"rjq/internal/storage"
	"rjq/pkg/models"
)

// RateLimitedQueue wraps a MemoryQueue with a token bucket rate limiter.
type RateLimitedQueue struct {
	*MemoryQueue
	rate       int // tokens per minute
	burst      int // max tokens
	tokens     int // current tokens
	mu         sync.Mutex
	stopRefill chan struct{}
}

// NewRateLimitedQueue creates a rate-limited queue.
func NewRateLimitedQueue(store storage.Storage, bufferSize, ratePerMinute, burst int) *RateLimitedQueue {
	q := &RateLimitedQueue{
		MemoryQueue: NewMemoryQueue(store, bufferSize),
		rate:        ratePerMinute,
		burst:       burst,
		tokens:      burst,
		stopRefill:  make(chan struct{}),
	}
	q.startRefill()
	return q
}

// startRefill adds tokens at the configured rate.
func (q *RateLimitedQueue) startRefill() {
	interval := time.Minute / time.Duration(q.rate)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				q.mu.Lock()
				if q.tokens < q.burst {
					q.tokens++
				}
				q.mu.Unlock()
			case <-q.stopRefill:
				return
			}
		}
	}()
}

// Dequeue blocks until a token is available, then dequeues.
func (q *RateLimitedQueue) Dequeue() (*models.Job, error) {
	// Wait for a token.
	for {
		q.mu.Lock()
		if q.tokens > 0 {
			q.tokens--
			q.mu.Unlock()
			break
		}
		q.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

	return q.MemoryQueue.Dequeue()
}

// Close stops the refill goroutine and closes the underlying queue.
func (q *RateLimitedQueue) Close() error {
	close(q.stopRefill)
	return q.MemoryQueue.Close()
}

// AvailableTokens returns the current token count (for the dashboard).
func (q *RateLimitedQueue) AvailableTokens() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.tokens
}

// Enqueue adds a job to the rate-limited queue.
func (q *RateLimitedQueue) Enqueue(job *models.Job) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return fmt.Errorf("queue is closed")
	}
	q.mu.Unlock()

	q.jobs <- job
	return nil
}

package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"rjq/internal/queue"
	"rjq/pkg/models"

	log "github.com/sirupsen/logrus"
)

// Processor is called by the pool for each dequeued job.
type Processor interface {
	Process(ctx context.Context, job *models.Job) error
}

// workerState tracks a single worker goroutine's current job and cancel function.
type workerState struct {
	currentJob *models.Job
	cancel     context.CancelFunc
}

// Pool manages a fixed number of worker goroutines.
type Pool struct {
	queue        queue.Queue
	processor    Processor
	count        int
	timeout      time.Duration
	cooldown     time.Duration
	wg           sync.WaitGroup
	mu           sync.Mutex
	workers      []*workerState
	preemptQueue []*models.Job // preempted slot gets the super-urgent job
}

// NewPool creates a worker pool.
func NewPool(q queue.Queue, p Processor, workers int, timeout, cooldown time.Duration) *Pool {
	return &Pool{
		queue:        q,
		processor:    p,
		count:        workers,
		timeout:      timeout,
		cooldown:     cooldown,
		workers:      make([]*workerState, workers),
		preemptQueue: make([]*models.Job, workers),
	}
}

// Start launches all worker goroutines.
func (p *Pool) Start() {
	for i := 0; i < p.count; i++ {
		p.workers[i] = &workerState{}
		p.wg.Add(1)
		go p.worker(i)
	}
	log.WithField("workers", p.count).Info("Worker pool started")
}

// Wait blocks until all workers have exited.
func (p *Pool) Wait() {
	p.wg.Wait()
	log.Info("All workers stopped")
}

// Preempt cancels a running normal-priority job to make room for a
// super-urgent job. Returns the preempted job's ID.
func (p *Pool) Preempt(job *models.Job) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, w := range p.workers {
		if w.currentJob != nil && w.currentJob.Priority == models.PriorityNormal {
			preemptedID := w.currentJob.ID
			log.WithFields(log.Fields{
				"preempted_job": preemptedID,
				"super_urgent":  job.ID,
				"worker_id":     i,
			}).Info("Preempting job")
			w.cancel()
			p.preemptQueue[i] = job
			return preemptedID, nil
		}
	}

	return "", fmt.Errorf("no preemptable job found")
}

// worker runs the dequeue-process loop for a single goroutine.
func (p *Pool) worker(id int) {
	defer p.wg.Done()
	defer p.recoverWorker(id)

	log.WithField("worker_id", id).Info("Worker started")

	for {
		// Check for a preempted super-urgent job assigned to this slot.
		p.mu.Lock()
		preemptJob := p.preemptQueue[id]
		p.preemptQueue[id] = nil
		p.mu.Unlock()

		var job *models.Job
		var err error

		if preemptJob != nil {
			job = preemptJob
		} else {
			job, err = p.queue.Dequeue()
			if err != nil {
				log.WithFields(log.Fields{
					"worker_id": id,
					"error":     err,
				}).Error("Dequeue failed")
				continue
			}
			if job == nil {
				log.WithField("worker_id", id).Info("Worker exiting, queue closed")
				return
			}
		}

		p.processJob(id, job)
		time.Sleep(p.cooldown)
	}
}

// processJob wraps the Processor call with a timeout and handles Ack/Nack.
func (p *Pool) processJob(workerID int, job *models.Job) {
	if err := p.queue.SetProcessing(job.ID); err != nil {
		log.WithFields(log.Fields{
			"worker_id": workerID,
			"job_id":    job.ID,
			"error":     err,
		}).Error("Failed to mark job as processing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)

	// Register this job and cancel function so Preempt can find it.
	p.mu.Lock()
	p.workers[workerID].currentJob = job
	p.workers[workerID].cancel = cancel
	p.mu.Unlock()

	defer func() {
		cancel()
		p.mu.Lock()
		p.workers[workerID].currentJob = nil
		p.workers[workerID].cancel = nil
		p.mu.Unlock()
	}()

	log.WithFields(log.Fields{
		"worker_id": workerID,
		"job_id":    job.ID,
		"priority":  job.Priority,
	}).Info("Processing job")

	err := p.processor.Process(ctx, job)
	if err != nil {
		// Check if this was a preemption (context cancelled).
		if ctx.Err() == context.Canceled {
			log.WithFields(log.Fields{
				"worker_id": workerID,
				"job_id":    job.ID,
			}).Info("Job preempted, requeueing without retry penalty")
			// Requeue without incrementing retry count.
			if reqErr := p.queue.Requeue(job.ID); reqErr != nil {
				log.WithFields(log.Fields{
					"worker_id": workerID,
					"job_id":    job.ID,
					"error":     reqErr,
				}).Error("Failed to requeue preempted job")
			}
			return
		}

		log.WithFields(log.Fields{
			"worker_id": workerID,
			"job_id":    job.ID,
			"error":     err,
		}).Error("Job failed, nacking")

		if nackErr := p.queue.Nack(job.ID); nackErr != nil {
			log.WithFields(log.Fields{
				"worker_id": workerID,
				"job_id":    job.ID,
				"error":     nackErr,
			}).Error("Nack failed")
		}
		return
	}

	if ackErr := p.queue.Ack(job.ID); ackErr != nil {
		log.WithFields(log.Fields{
			"worker_id": workerID,
			"job_id":    job.ID,
			"error":     ackErr,
		}).Error("Ack failed")
	}
}

// recoverWorker catches panics so one bad job doesn't crash the pool.
func (p *Pool) recoverWorker(id int) {
	if r := recover(); r != nil {
		log.WithFields(log.Fields{
			"worker_id": id,
			"panic":     r,
		}).Error("Worker panicked and recovered")
	}
}

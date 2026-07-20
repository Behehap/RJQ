package worker

import (
	"context"
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

// Pool manages a fixed number of worker goroutines.
type Pool struct {
	queue     queue.Queue
	processor Processor
	count     int
	timeout   time.Duration
	wg        sync.WaitGroup
}

// NewPool creates a worker pool.
func NewPool(q queue.Queue, p Processor, workers int, timeout time.Duration) *Pool {
	return &Pool{
		queue:     q,
		processor: p,
		count:     workers,
		timeout:   timeout,
	}
}

// Start launches all worker goroutines.
func (p *Pool) Start() {
	for i := 0; i < p.count; i++ {
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

// worker runs the dequeue-process loop for a single goroutine.
func (p *Pool) worker(id int) {
	defer p.wg.Done()
	defer p.recoverWorker(id)

	log.WithField("worker_id", id).Info("Worker started")

	for {
		job, err := p.queue.Dequeue()
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

		p.processJob(id, job)
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

	log.WithFields(log.Fields{
		"worker_id": workerID,
		"job_id":    job.ID,
	}).Info("Processing job")

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	err := p.processor.Process(ctx, job)
	if err != nil {
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

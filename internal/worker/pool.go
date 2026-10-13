package worker

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"rjq/internal/metrics"
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
	queue     queue.Queue
	processor Processor
	count     int
	timeout   time.Duration
	cooldown  time.Duration

	wg   sync.WaitGroup
	mu   sync.Mutex
	done chan struct{} // closed to tell workers to exit

	workers       []*workerState
	preemptQueue  []*models.Job   // preempted slot gets the super-urgent job
	preemptSignal []chan struct{} // Signal for a super urgent job

	workerDied chan int // worker ID sent here on exit

	// number of worker goroutines currently running
	aliveWorkers atomic.Int32
}

// NewPool creates a worker pool.
func NewPool(q queue.Queue, p Processor, workers int, timeout, cooldown time.Duration) *Pool {
	return &Pool{
		queue:         q,
		processor:     p,
		count:         workers,
		timeout:       timeout,
		cooldown:      cooldown,
		done:          make(chan struct{}),
		workers:       make([]*workerState, workers),
		preemptQueue:  make([]*models.Job, workers),
		preemptSignal: make([]chan struct{}, workers),
		workerDied:    make(chan int, workers),
	}
}

// Start launches all worker goroutines.
func (p *Pool) Start() {
	for i := 0; i < p.count; i++ {
		p.workers[i] = &workerState{}
		p.preemptSignal[i] = make(chan struct{}, 1)
		p.wg.Add(1)
		go p.worker(i)
	}
	go p.supervise()
	log.WithField("workers", p.count).Info("Worker pool started")
}

// Stop tells all workers to exit their loops.
func (p *Pool) Stop() {
	close(p.done)
}

// Wait blocks until all workers have exited.
func (p *Pool) Wait() {
	p.wg.Wait()
	log.Info("All workers stopped")
}

// ExpectedWorkers returns the configured worker count.
func (p *Pool) ExpectedWorkers() int {
	return p.count
}

// AliveWorkers returns how many worker goroutines are running.
func (p *Pool) AliveWorkers() int {
	return int(p.aliveWorkers.Load())
}

// Preempt cancels a running normal-priority job to make room for a
// super-urgent job. Returns the preempted job's ID.
func (p *Pool) Preempt(job *models.Job) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, w := range p.workers {
		if w.currentJob != nil &&
			w.currentJob.Priority == models.PriorityNormal &&
			p.preemptQueue[i] == nil {
			preemptedID := w.currentJob.ID

			// Count the preemption by queue type.
			metrics.JobsPreempted.WithLabelValues(w.currentJob.QueueType).Inc()

			w.cancel()
			p.preemptQueue[i] = job
			return preemptedID, nil
		}
	}

	return "", fmt.Errorf("no preemptable job found")
}

// worker runs the dequeue-process loop for a single goroutine.
// Deferred order is LIFO: death signal, recover, wg.Done.
func (p *Pool) worker(id int) {
	p.aliveWorkers.Add(1)
	defer p.aliveWorkers.Add(-1)

	defer p.wg.Done()
	defer p.recoverWorker(id)
	defer func() {
		// non-blocking so a full channel doesn't block shutdown
		select {
		case p.workerDied <- id:
		default:
			log.WithField("worker_id", id).Warn("death signal dropped")
		}
	}()

	log.WithField("worker_id", id).Info("Worker started")

	for {
		select {
		case <-p.done:
			log.WithField("worker_id", id).Info("Worker exiting: pool stopped")
			return
		default:
		}

		p.mu.Lock()
		preemptJob := p.preemptQueue[id]
		p.preemptQueue[id] = nil
		p.mu.Unlock()

		if preemptJob != nil {
			p.processJob(id, preemptJob)
			time.Sleep(p.cooldown)
			continue
		}

		job, err := p.queue.DequeueOrWait(100 * time.Millisecond)
		if err != nil {
			log.WithFields(log.Fields{
				"worker_id": id,
				"error":     err,
			}).Error("Dequeue failed")
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if job == nil {
			continue
		}

		p.processJob(id, job)
		time.Sleep(p.cooldown)
	}
}

// processJob wraps the Processor call with a timeout and handles Ack/Nack.
func (p *Pool) processJob(workerID int, job *models.Job) {
	// Wait time = time from creation to when a worker picked it up.
	wait := time.Since(job.CreatedAt).Seconds()
	metrics.JobWaitDuration.WithLabelValues(job.QueueType).Observe(wait)

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

	metrics.WorkersBusy.Inc()
	defer metrics.WorkersBusy.Dec()

	start := time.Now()
	err := p.processor.Process(ctx, job)
	metrics.JobProcessingDuration.WithLabelValues(job.QueueType).Observe(time.Since(start).Seconds())

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
// Logs the panic value and stack so we can see what killed the worker.
func (p *Pool) recoverWorker(id int) {
	if r := recover(); r != nil {
		log.WithFields(log.Fields{
			"worker_id": id,
			"panic":     r,
			"stack":     string(debug.Stack()),
		}).Error("Worker panicked")
	}
}

// supervise watches for dead workers and restarts them.
func (p *Pool) supervise() {
	for {
		select {
		case <-p.done:
			log.Info("Supervisor exiting: pool stopped")
			return
		case id := <-p.workerDied:
			log.WithField("worker_id", id).Warn("Worker died, restarting")
			select {
			case <-time.After(1 * time.Second):
			case <-p.done:
				log.Info("Supervisor exiting during restart backoff")
				return
			}

			p.wg.Add(1)
			go p.worker(id)

			log.WithField("worker_id", id).Info("Worker restarted")
		}
	}
}

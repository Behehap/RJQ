// Package queue defines the job queue abstraction.
// Implementation must be safe for concurrent use by multiple goroutines.
package queue

import "rjq/pkg/models"

// Queue is a FIFO job Queue. Workers call Dequeue to clam jobs,
// the ACK (success) or NACK (failure) to finalize them.

type Queue interface {
	// Enque adds a job to the queue. The job must already be persisted
	// in storage before calling Enqueue.
	Enqueue(job *models.Job) error

	//Dequeue blocks untill a job is available, then returns it.
	// The return job is marked as in-flight and must be finalized
	// with ACK or NACK
	Dequeue() (*models.Job, error)

	//Ack marks a job as successfully processed.
	Ack(jobID string) error

	// Nack marks a job as failed. The caller is responsible for
	// retry logic (incrementing retry count, deciding if the job
	// should be re-queued or failed permanently).
	Nack(JobID string) error

	// PendingCount returns the number of jobs waiting in the queue.
	PendingCount() int

	// Close shuts down the queue. No new jobs can be enqueued after
	// Close is called. Workers should dreain existing jobs.
	Close() error

	// SetProcessing marks a job as currently being processed.
	SetProcessing(jobID string) error

	// RequeueAtFront resets a job to pending without incrementing retry_count.
	// Used when a job is preempted (not failed).
	RequeueAtFront(id string) error
}

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Every metric here is registered automatically with the default
// Prometheus registry via promauto. No manual registration needed.

var (
	// JobsSubmitted counts every job that reaches the API and is accepted.
	// Labeled by queue type so we can compare FIFO vs priority vs rate-limited.
	JobsSubmitted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rjq_jobs_submitted_total",
			Help: "Total number of jobs submitted, labeled by queue type.",
		},
		[]string{"queue"},
	)

	// JobsCompleted counts jobs that reached a successful end.
	JobsCompleted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rjq_jobs_completed_total",
			Help: "Total number of jobs completed successfully.",
		},
		[]string{"queue"},
	)

	// JobsFailed counts jobs that exhausted all retries and were marked failed.
	JobsFailed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rjq_jobs_failed_total",
			Help: "Total number of jobs that failed permanently.",
		},
		[]string{"queue"},
	)

	// JobsRetried counts every retry attempt, not just the first one.
	JobsRetried = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rjq_jobs_retried_total",
			Help: "Total number of job retries.",
		},
		[]string{"queue"},
	)

	// JobWaitDuration measures how long a job sits in the queue before a
	// worker picks it up. Useful for comparing scheduling policies.
	JobWaitDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "rjq_job_wait_duration_seconds",
			Help:    "Time a job spends waiting in the queue before processing.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"queue"},
	)

	// JobProcessingDuration measures how long the actual work takes.
	// This is what tells us if a queue type is slow or if the work is.
	JobProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "rjq_job_processing_duration_seconds",
			Help:    "Time a job spends being processed.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"queue"},
	)

	// QueueDepth is the number of pending jobs per queue right now.
	// This is the number that tells us if we are falling behind.
	QueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rjq_queue_depth",
			Help: "Number of pending jobs per queue.",
		},
		[]string{"queue"},
	)

	// WorkersBusy tracks how many workers are currently processing.
	// Paired with total worker count, this shows utilization.
	WorkersBusy = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "rjq_workers_busy",
			Help: "Number of workers currently processing a job.",
		},
	)

	// JobsRecovered is incremented for every job that comes back from the
	// database on startup. If this is high, the last shutdown was not clean.
	JobsRecovered = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "rjq_jobs_recovered_total",
			Help: "Total number of jobs recovered on startup.",
		},
	)

	// RecoveryDuration is how long the startup recovery took.
	// A spike here usually means a large backlog after a crash.
	RecoveryDuration = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "rjq_recovery_duration_seconds",
			Help: "Time taken to recover pending jobs on startup.",
		},
	)
)

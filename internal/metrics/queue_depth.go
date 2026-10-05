package metrics

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

// QueueDepthCollector reads pending job counts from the database on each
// Prometheus scrape. Unlike an in-memory gauge, this cannot drift: it
// always reflects the real state of the jobs table.
type QueueDepthCollector struct {
	db   *sql.DB
	desc *prometheus.Desc
}

// NewQueueDepthCollector creates a collector that queries the given
// database for pending job counts grouped by queue type.
func NewQueueDepthCollector(db *sql.DB) *QueueDepthCollector {
	return &QueueDepthCollector{
		db: db,
		desc: prometheus.NewDesc(
			"rjq_queue_depth",
			"Number of pending jobs per queue, read live from the database.",
			[]string{"queue"},
			nil,
		),
	}
}

// Describe registers the metric descriptor with Prometheus.
func (c *QueueDepthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect runs the query and emits one gauge value per queue type.
func (c *QueueDepthCollector) Collect(ch chan<- prometheus.Metric) {
	rows, err := c.db.Query(
		"SELECT queue_type, COUNT(*) FROM jobs WHERE status = 'pending' GROUP BY queue_type",
	)
	if err != nil {
		// If the query fails, emit nothing. Prometheus will just miss
		// this scrape; the next one will retry.
		return
	}
	defer rows.Close()

	for rows.Next() {
		var queueType string
		var count int
		if err := rows.Scan(&queueType, &count); err != nil {
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			prometheus.GaugeValue,
			float64(count),
			queueType,
		)
	}
}

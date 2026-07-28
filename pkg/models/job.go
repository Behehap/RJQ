package models

import "time"

// Job status constants. Using typed constants prevents accidental assignment of arbitary strings

const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
)

// Priority levels.
const (
	PriorityNormal      = 1
	PriorityUrgent      = 2
	PrioritySuperUrgent = 3
)

// Queue type constants.
const (
	QueueTypeFIFO        = "fifo"
	QueueTypePriority    = "priority"
	QueueTypeRateLimited = "rate-limited"
)

// Job represents a single email-sending task.
// Struct tas:
//
//	json: controls JSON field names (right side : REST API)
//	db: would be for sqlx, but we're using sqlite3 directly, so omit

type Job struct {
	ID           string     `json:"id"`
	ToEmail      string     `json:"to"`
	Subject      string     `json:"subject"`
	Body         string     `json:"body"`
	QueueType    string     `json:"queue"`
	Priority     int        `json:"priority"`
	Status       string     `json:"status"`
	RetryCount   int        `json:"retry_count"`
	MaxRetries   int        `json:"max_retries"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	ProcessedAt  *time.Time `json:"processed_at,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
}

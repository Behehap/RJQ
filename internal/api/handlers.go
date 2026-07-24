package api

import (
	"embed"
	"encoding/json"
	"net/http"
	"time"

	"rjq/internal/queue"
	"rjq/internal/storage"
	"rjq/internal/worker"
	"rjq/pkg/models"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// Handler holds dependencies for HTTP handlers.
// Nothing here touches the database or queue directly
// beyond calling their interfaces.
type Handler struct {
	store storage.Storage
	queue queue.Queue
	pool  *worker.Pool
}

// NewHandler wires up storage, queue, and pool for the API layer.
func NewHandler(s storage.Storage, q queue.Queue, p *worker.Pool) *Handler {
	return &Handler{store: s, queue: q, pool: p}
}

var _ = embed.FS{} // ensures embed package is not removed by the compiler
//go:embed dashboard.html
var dashboardHTML string

// Dashboard serves the dashboard HTML page.
func (h *Handler) Dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML))
}

// RegisterRoutes attaches all endpoints to a chi router.
func (h *Handler) RegisterRoutes(r chi.Router) {
	r.Post("/jobs", h.CreateJob)
	r.Get("/jobs/{id}", h.GetJob)
	r.Get("/stats", h.GetStats)
	r.Get("/health", h.Health)
	r.Get("/jobs", h.ListJobs)
	r.Get("/dashboard", h.Dashboard)
	r.Get("/processing", h.GetProcessing)
	r.Get("/pending", h.GetPending)
}

// CreateJob handles POST /jobs.
// It validates the request, persists the job, enqueues it,
// and returns the job ID with status pending.
// Super-urgent jobs attempt to preempt a running normal job.
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req struct {
		To       string `json:"to"`
		Subject  string `json:"subject"`
		Body     string `json:"body"`
		Priority int    `json:"priority"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if req.To == "" || req.Subject == "" || req.Body == "" {
		writeError(w, http.StatusBadRequest, "to, subject, and body are required")
		return
	}

	if req.Priority < 1 || req.Priority > 3 {
		req.Priority = models.PriorityNormal
	}

	now := time.Now()
	job := &models.Job{
		ID:         uuid.New().String(),
		ToEmail:    req.To,
		Subject:    req.Subject,
		Body:       req.Body,
		Priority:   req.Priority,
		Status:     models.StatusPending,
		RetryCount: 0,
		MaxRetries: 3,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := h.store.SaveJob(job); err != nil {
		log.WithError(err).Error("Failed to save job")
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}

	// Super-urgent jobs attempt preemption before enqueuing.
	if job.Priority == models.PrioritySuperUrgent {
		preemptedID, err := h.pool.Preempt(job)
		if err != nil {
			log.WithFields(log.Fields{
				"job_id": job.ID,
				"error":  err,
			}).Info("No preemptable job, super-urgent job queued normally")
			// Fall through to normal enqueue.
			if err := h.queue.Enqueue(job); err != nil {
				log.WithError(err).Error("Failed to enqueue job")
				writeError(w, http.StatusInternalServerError, "failed to enqueue job")
				return
			}
		} else {
			log.WithFields(log.Fields{
				"job_id":        job.ID,
				"preempted_job": preemptedID,
			}).Info("Super-urgent job preempted a normal job")
			// Job is already assigned to a worker slot via preemptQueue.
			// Don't enqueue — it'll be picked up directly by the worker.
		}
	} else {
		if err := h.queue.Enqueue(job); err != nil {
			log.WithError(err).Error("Failed to enqueue job")
			writeError(w, http.StatusInternalServerError, "failed to enqueue job")
			return
		}
	}

	log.WithFields(log.Fields{
		"job_id":   job.ID,
		"to":       job.ToEmail,
		"priority": job.Priority,
	}).Info("Job created")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"job_id": job.ID,
		"status": job.Status,
	})
}

// GetJob handles GET /jobs/{id}.
// Returns the full job object or 404.
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	job, err := h.store.GetJob(id)
	if err != nil {
		log.WithError(err).Error("Failed to get job")
		writeError(w, http.StatusInternalServerError, "failed to retrieve job")
		return
	}

	if job == nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// GetStats handles GET /stats.
// Returns pending, processing, and queued counts.
func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	pending, err := h.store.ListPendingJobs()
	if err != nil {
		log.WithError(err).Error("Failed to list pending jobs")
		writeError(w, http.StatusInternalServerError, "failed to get stats")
		return
	}

	processing, err := h.store.GetProcessingJobs()
	if err != nil {
		log.WithError(err).Error("Failed to list processing jobs")
		writeError(w, http.StatusInternalServerError, "failed to get stats")
		return
	}

	// True queue depth = pending minus processing.
	truePending := 0
	for _, j := range pending {
		if j.Status == models.StatusPending {
			truePending++
		}
	}

	stats := map[string]int{
		"pending":    truePending,
		"processing": len(processing),
		"queued":     h.queue.PendingCount(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// Health handles GET /health.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}

// ListJobs handles GET /jobs?limit=20.
func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.store.ListRecentJobs(20)
	if err != nil {
		log.WithError(err).Error("Failed to list jobs")
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}
	if jobs == nil {
		jobs = []*models.Job{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

// GetProcessing returns all jobs currently being processed.
func (h *Handler) GetProcessing(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.store.GetProcessingJobs()
	if err != nil {
		log.WithError(err).Error("Failed to get processing jobs")
		writeError(w, http.StatusInternalServerError, "failed to get processing jobs")
		return
	}
	if jobs == nil {
		jobs = []*models.Job{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

// GetPending returns all jobs waiting in the queue.
func (h *Handler) GetPending(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.store.ListAllPendingJobs()
	if err != nil {
		log.WithError(err).Error("Failed to get pending jobs")
		writeError(w, http.StatusInternalServerError, "failed to get pending jobs")
		return
	}
	if jobs == nil {
		jobs = []*models.Job{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

// writeError is a helper to keep error responses consistent.
func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{
		"error": msg,
	})
}

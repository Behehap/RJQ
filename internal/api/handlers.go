package api

import (
	"embed"

	"encoding/json"
	"net/http"
	"time"

	"rjq/internal/queue"
	"rjq/internal/storage"
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
}

// NewHandler wires up storage and queue for the API layer.
func NewHandler(s storage.Storage, q queue.Queue) *Handler {
	return &Handler{store: s, queue: q}
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
}

// CreateJob handles POST /jobs.
// It validates the request, persists the job, enqueues it,
// and returns the job ID with status pending.
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
		Body    string `json:"body"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if req.To == "" || req.Subject == "" || req.Body == "" {
		writeError(w, http.StatusBadRequest, "to, subject, and body are required")
		return
	}

	now := time.Now()
	job := &models.Job{
		ID:         uuid.New().String(),
		ToEmail:    req.To,
		Subject:    req.Subject,
		Body:       req.Body,
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

	if err := h.queue.Enqueue(job); err != nil {
		log.WithError(err).Error("Failed to enqueue job")
		writeError(w, http.StatusInternalServerError, "failed to enqueue job")
		return
	}

	log.WithFields(log.Fields{
		"job_id": job.ID,
		"to":     job.ToEmail,
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
// Counts jobs by status directly from the database.
func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	// We don't have a dedicated stats method on Storage,
	// so we lean on ListPendingJobs + a simple count approach.
	// In production you'd add a Stats() method to the Storage interface.
	pending, err := h.store.ListPendingJobs()
	if err != nil {
		log.WithError(err).Error("Failed to list pending jobs")
		writeError(w, http.StatusInternalServerError, "failed to get stats")
		return
	}

	stats := map[string]int{
		"pending": len(pending),
		"queued":  h.queue.PendingCount(),
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

// writeError is a helper to keep error responses consistent.
func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{
		"error": msg,
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

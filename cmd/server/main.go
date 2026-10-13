package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rjq/internal/api"
	"rjq/internal/config"
	"rjq/internal/jobs/email"
	"rjq/internal/metrics"
	"rjq/internal/queue"
	"rjq/internal/storage"
	"rjq/internal/worker"
	"rjq/pkg/models"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

func main() {
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.WithError(err).Fatal("Failed to load config")
	}

	var store storage.Storage
	switch cfg.Database.Backend {
	case "postgres":
		store, err = storage.NewPostgresStorage(cfg.Database.PostgresDSN)
	default:
		store, err = storage.NewSQLiteStorage(cfg.Database.SQLitePath)
	}
	if err != nil {
		log.WithError(err).Fatal("Failed to initialize storage")
	}
	defer store.Close()

	// Register the queue depth collector so it reads live from the database.
	if dbStore, ok := store.(interface{ DB() *sql.DB }); ok {
		prometheus.MustRegister(metrics.NewQueueDepthCollector(dbStore.DB()))
		log.Info("Queue depth collector registered")
	} else {
		log.Warn("Storage does not expose DB(), queue depth collector skipped")
	}

	// Pre-register counters with zero values so the dashboard shows them
	// before any failures occur.
	for _, q := range []string{
		models.QueueTypeFIFO,
		models.QueueTypePriority,
		models.QueueTypeRateLimited,
	} {
		metrics.JobsFailed.WithLabelValues(q).Add(0)
		metrics.JobsRetried.WithLabelValues(q).Add(0)
		metrics.JobsPreempted.WithLabelValues(q).Add(0)
	}

	// Create all three queue instances.
	fifoQueue := queue.NewMemoryQueue(store, cfg.Queue.Workers*10)
	priorityQueue := queue.NewSortedQueue(store, cfg.Queue.Workers*10)
	rateLimitedQueue := queue.NewRateLimitedQueue(store, cfg.Queue.Workers*10,
		cfg.RateLimit.EmailsPerMinute, cfg.RateLimit.Burst)

	fifoQueue.StartSweeper(5 * time.Minute)
	priorityQueue.StartSweeper(5 * time.Minute)
	rateLimitedQueue.StartSweeper(5 * time.Minute)

	router := queue.NewRouter(fifoQueue, priorityQueue, rateLimitedQueue)

	// Pick SMTP settings based on mode.
	var smtpHost string
	var smtpPort int
	var smtpUser, smtpPass string
	var useAuth bool

	switch cfg.Email.Mode {
	case "production":
		smtpHost = cfg.Email.SMTPHost
		smtpPort = cfg.Email.SMTPPort
		smtpUser = cfg.Email.SMTPUser
		smtpPass = cfg.Email.SMTPPass
		useAuth = true
	default: // "test"
		smtpHost = cfg.Email.TestHost
		smtpPort = cfg.Email.TestPort
		smtpUser = cfg.Email.TestUser
		smtpPass = cfg.Email.TestPass
		useAuth = false
	}

	emailProcessor := email.NewEmailProcessor(
		smtpHost,
		smtpPort,
		smtpUser,
		smtpPass,
		time.Duration(cfg.Timeout.JobSeconds)*time.Second,
		time.Duration(cfg.Queue.DemoDelaySec)*time.Second,
		useAuth,
	)

	pool := worker.NewPool(router, emailProcessor, cfg.Queue.Workers,
		time.Duration(cfg.Timeout.JobSeconds)*time.Second,
		time.Duration(cfg.Queue.CooldownSec)*time.Second,
	)
	pool.Start()

	recoveryStart := time.Now()
	if err := router.Recover(); err != nil {
		log.WithError(err).Fatal("Failed to recover queues")
	}
	metrics.RecoveryDuration.Set(time.Since(recoveryStart).Seconds())

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	handler := api.NewHandler(store, router, pool)
	handler.RegisterRoutes(r)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Server.Port),
		Handler: r,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.WithField("port", cfg.Server.Port).Info("Server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.WithError(err).Fatal("Server failed")
		}
	}()

	<-quit
	log.Info("Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.WithError(err).Error("Server shutdown failed")
	}

	router.Close()
	pool.Stop()
	pool.Wait()

	log.Info("Server stopped")
}

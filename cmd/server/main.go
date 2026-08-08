package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rjq/internal/api"
	"rjq/internal/config"
	"rjq/internal/queue"
	"rjq/internal/storage"
	"rjq/internal/worker"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	log "github.com/sirupsen/logrus"
)

func main() {
	// Load configuration.
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.WithError(err).Fatal("Failed to load config")
	}

	// Initialize storage.
	store, err := storage.NewSQLiteStorage("rjq.db")
	if err != nil {
		log.WithError(err).Fatal("Failed to initialize storage")
	}
	defer store.Close()

	// Create all three queue instances.
	fifoQueue := queue.NewMemoryQueue(store, cfg.Queue.Workers*10)
	priorityQueue := queue.NewMemoryQueue(store, cfg.Queue.Workers*10)
	rateLimitedQueue := queue.NewRateLimitedQueue(store, cfg.Queue.Workers*10,
		cfg.RateLimit.EmailsPerMinute, cfg.RateLimit.Burst)

	fifoQueue.StartSweeper(5 * time.Minute)
	priorityQueue.StartSweeper(5 * time.Minute)
	rateLimitedQueue.StartSweeper(5 * time.Minute)

	// Wrap them in a router that workers will pull from.
	router := queue.NewRouter(fifoQueue, priorityQueue, rateLimitedQueue)

	// Initialize worker pool.
	emailWorker := worker.NewEmailWorker(
		cfg.Email.SMTPHost,
		cfg.Email.SMTPPort,
		cfg.Email.SMTPUser,
		cfg.Email.SMTPPass,
		time.Duration(cfg.Timeout.JobSeconds)*time.Second,
		time.Duration(cfg.Queue.DemoDelaySec)*time.Second,
	)
	pool := worker.NewPool(router, emailWorker, cfg.Queue.Workers,
		time.Duration(cfg.Timeout.JobSeconds)*time.Second,
		time.Duration(cfg.Queue.CooldownSec)*time.Second,
	)
	pool.Start()

	// Recover pending jobs for all queues on startup.
	if err := router.Recover(); err != nil {
		log.WithError(err).Fatal("Failed to recover queues")
	}

	// Initialize API router.
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	handler := api.NewHandler(store, router, pool)
	handler.RegisterRoutes(r)

	// Start HTTP server.
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Server.Port),
		Handler: r,
	}

	// Graceful shutdown on SIGINT/SIGTERM.
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

	// Stop accepting new requests.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.WithError(err).Error("Server shutdown failed")
	}

	// Stop the queue and wait for workers to finish.
	router.Close()
	pool.Wait()

	log.Info("Server stopped")
}

package email

import (
	"context"
	"fmt"
	"net/smtp"
	"time"

	"rjq/pkg/models"

	log "github.com/sirupsen/logrus"
)

// EmailProcessor sends emails via SMTP.
type EmailProcessor struct {
	smtpHost  string
	smtpPort  int
	smtpUser  string
	smtpPass  string
	timeout   time.Duration
	demoDelay time.Duration
}

// NewEmailProcessor creates an EmailProcessor with the given SMTP settings.
func NewEmailProcessor(host string, port int, user, pass string, timeout time.Duration, demoDelay time.Duration) *EmailProcessor {
	return &EmailProcessor{
		smtpHost:  host,
		smtpPort:  port,
		smtpUser:  user,
		smtpPass:  pass,
		timeout:   timeout,
		demoDelay: demoDelay,
	}
}

// Process sends an email. It respects the context deadline set by the pool.
func (w *EmailProcessor) Process(ctx context.Context, job *models.Job) error {
	log.WithFields(log.Fields{
		"job_id": job.ID,
		"to":     job.ToEmail,
	}).Info("Sending email")

	addr := fmt.Sprintf("%s:%d", w.smtpHost, w.smtpPort)
	msg := buildMessage(w.smtpUser, job.ToEmail, job.Subject, job.Body)

	var auth smtp.Auth
	if w.smtpUser != "" && w.smtpPass != "" {
		auth = smtp.PlainAuth("", w.smtpUser, w.smtpPass, w.smtpHost)
	}

	done := make(chan error, 1)
	go func() {
		done <- smtp.SendMail(addr, auth, w.smtpUser, []string{job.ToEmail}, msg)
	}()

	select {
	case err := <-done:
		if err != nil {
			log.WithFields(log.Fields{
				"job_id": job.ID,
				"error":  err,
			}).Error("Failed to send email")
			return err
		}
	case <-ctx.Done():
		log.WithFields(log.Fields{
			"job_id": job.ID,
			"error":  ctx.Err(),
		}).Error("Email sending timed out")
		return ctx.Err()
	}

	// Demo delay — keeps job in "processing" state so the dashboard
	// progress bar can fill completely before the job completes.
	select {
	case <-time.After(w.demoDelay):
	case <-ctx.Done():
		return ctx.Err()
	}

	log.WithFields(log.Fields{
		"job_id": job.ID,
		"to":     job.ToEmail,
	}).Info("Email sent")
	return nil
}

// buildMessage constructs an email message.
func buildMessage(from, to, subject, body string) []byte {
	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s",
		from, to, subject, body)
	return []byte(msg)
}

package worker

import (
	"context"
	"fmt"
	"net/smtp"
	"time"

	"rjq/pkg/models"

	log "github.com/sirupsen/logrus"
)

// EmailWorker sends emails via SMTP.
type EmailWorker struct {
	smtpHost string
	smtpPort int
	smtpUser string
	smtpPass string
	timeout  time.Duration
}

// NewEmailWorker creates an EmailWorker with the given SMTP settings.
func NewEmailWorker(host string, port int, user, pass string, timeout time.Duration) *EmailWorker {
	return &EmailWorker{
		smtpHost: host,
		smtpPort: port,
		smtpUser: user,
		smtpPass: pass,
		timeout:  timeout,
	}
}

// Process sends an email. It respects the context deadline set by the pool.
func (w *EmailWorker) Process(ctx context.Context, job *models.Job) error {
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
		log.WithFields(log.Fields{
			"job_id": job.ID,
			"to":     job.ToEmail,
		}).Info("Email sent")
		return nil
	case <-ctx.Done():
		log.WithFields(log.Fields{
			"job_id": job.ID,
			"error":  ctx.Err(),
		}).Error("Email sending timed out")
		return ctx.Err()
	}
}

// buildMessage constructs a minimal RFC 822 email message.
func buildMessage(from, to, subject, body string) []byte {
	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s",
		from, to, subject, body)
	return []byte(msg)
}

package email

import (
	"context"
	"fmt"
	"net/smtp"
	"time"

	"rjq/pkg/models"

	log "github.com/sirupsen/logrus"
)

// EmailProcessor sends emails through SMTP.
// It implements the worker.Processor interface.
type EmailProcessor struct {
	smtpHost  string
	smtpPort  int
	smtpUser  string
	smtpPass  string
	timeout   time.Duration
	demoDelay time.Duration
}

// NewEmailProcessor creates an EmailProcessor with the given SMTP settings.
func NewEmailProcessor(host string, port int, user, pass string, timeout, demoDelay time.Duration) *EmailProcessor {
	return &EmailProcessor{
		smtpHost:  host,
		smtpPort:  port,
		smtpUser:  user,
		smtpPass:  pass,
		timeout:   timeout,
		demoDelay: demoDelay,
	}
}

// Process reads email fields from the job payload and sends the message.
// Returns an error if the payload is missing required fields or SMTP fails.
func (w *EmailProcessor) Process(ctx context.Context, job *models.Job) error {
	// Extract fields from payload.
	to, _ := job.Payload["to"].(string)
	subject, _ := job.Payload["subject"].(string)
	body, _ := job.Payload["body"].(string)

	if to == "" || subject == "" || body == "" {
		return fmt.Errorf("email payload missing required fields: to, subject, body")
	}

	log.WithFields(log.Fields{
		"job_id": job.ID,
		"to":     to,
	}).Info("Sending email")

	addr := fmt.Sprintf("%s:%d", w.smtpHost, w.smtpPort)
	msg := buildMessage(w.smtpUser, to, subject, body)

	var auth smtp.Auth
	if w.smtpUser != "" && w.smtpPass != "" {
		auth = smtp.PlainAuth("", w.smtpUser, w.smtpPass, w.smtpHost)
	}

	done := make(chan error, 1)
	go func() {
		done <- smtp.SendMail(addr, auth, w.smtpUser, []string{to}, msg)
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

	// Demo delay keeps the job in "processing" for visualization.
	select {
	case <-time.After(w.demoDelay):
	case <-ctx.Done():
		return ctx.Err()
	}

	log.WithFields(log.Fields{
		"job_id": job.ID,
		"to":     to,
	}).Info("Email sent")
	return nil
}

// buildMessage constructs a minimal RFC 822 email message.
func buildMessage(from, to, subject, body string) []byte {
	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s",
		from, to, subject, body)
	return []byte(msg)
}

package email

import (
	"context"
	"fmt"
	"net"
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
	useAuth   bool
}

// NewEmailProcessor creates an EmailProcessor with the given SMTP settings.
// useAuth should be true for real SMTP providers and false for local SMTP sinks
// like Mailpit that accept unauthenticated connections.
func NewEmailProcessor(host string, port int, user, pass string, timeout, demoDelay time.Duration, useAuth bool) *EmailProcessor {
	return &EmailProcessor{
		smtpHost:  host,
		smtpPort:  port,
		smtpUser:  user,
		smtpPass:  pass,
		timeout:   timeout,
		demoDelay: demoDelay,
		useAuth:   useAuth,
	}
}

// Process reads email fields from the job payload and sends the message.
// Returns an error if the payload is missing required fields or SMTP fails.
func (w *EmailProcessor) Process(ctx context.Context, job *models.Job) error {
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

	// Only authenticate when explicitly enabled and credentials are present.
	// This avoids the "unencrypted connection" error from Go's PlainAuth
	// when talking to local SMTP sinks like Mailpit.
	var auth smtp.Auth
	if w.useAuth && w.smtpUser != "" && w.smtpPass != "" {
		auth = smtp.PlainAuth("", w.smtpUser, w.smtpPass, w.smtpHost)
	}

	done := make(chan error, 1)
	go func() {
		done <- w.sendWithTimeout(ctx, addr, auth, to, msg)
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

// sendWithTimeout dials the SMTP server with a bounded timeout and sends
// the message. Uses net.Dialer so a stuck connection cannot block the
// worker indefinitely.
func (w *EmailProcessor) sendWithTimeout(ctx context.Context, addr string, auth smtp.Auth, to string, msg []byte) error {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	c, err := smtp.NewClient(conn, w.smtpHost)
	if err != nil {
		conn.Close()
		return fmt.Errorf("smtp client failed: %w", err)
	}
	defer c.Close()

	if auth != nil {
		if err := c.Auth(auth); err != nil {
			return fmt.Errorf("auth failed: %w", err)
		}
	}

	if err := c.Mail(w.smtpUser); err != nil {
		return fmt.Errorf("mail from failed: %w", err)
	}
	if err := c.Rcpt(to); err != nil {
		return fmt.Errorf("rcpt to failed: %w", err)
	}

	wc, err := c.Data()
	if err != nil {
		return fmt.Errorf("data failed: %w", err)
	}
	if _, err := wc.Write(msg); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("close failed: %w", err)
	}

	if err := c.Quit(); err != nil {
		return fmt.Errorf("quit failed: %w", err)
	}
	return nil
}

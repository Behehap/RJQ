// internal/config/config.go
package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config is the top-level configuration struct.
// It maps directly to the structure of config.yaml.
type Config struct {
	Server    ServerConfig
	Queue     QueueConfig
	Email     EmailConfig
	Retry     RetryConfig
	Timeout   TimeoutConfig
	RateLimit RateLimitConfig `mapstructure:"rate_limit"`
}

type ServerConfig struct {
	Port int
}

type QueueConfig struct {
	Workers      int
	DemoDelaySec int `mapstructure:"demo_delay_seconds"`
	CooldownSec  int `mapstructure:"cooldown_seconds"`
}

// EmailConfig holds SMTP settings for sending emails.
// mapstructure tags map snake_case YAML keys to exported Go fields.
type EmailConfig struct {
	SMTPHost string `mapstructure:"smtp_host"`
	SMTPPort int    `mapstructure:"smtp_port"`
	SMTPUser string `mapstructure:"smtp_user"`
	SMTPPass string `mapstructure:"smtp_pass"`
}

// RetryConfig holds retry policy settings.
type RetryConfig struct {
	MaxAttempts    int `mapstructure:"max_attempts"`
	BackoffSeconds int `mapstructure:"backoff_seconds"`
}

// TimeoutConfig holds per-job timeout settings.
type TimeoutConfig struct {
	JobSeconds int `mapstructure:"job_seconds"`
}

// RateLimitConfig holds Rate limit policy.
type RateLimitConfig struct {
	EmailsPerMinute int `mapstructure:"emails_per_minute"`
	Burst           int `mapstructure:"burst"`
}

// LoadConfig reads the YAML config file and applies environment variable overrides.
// It returns a validated Config or an error if required fields are missing or invalid.
func LoadConfig(configPath string) (*Config, error) {
	v := viper.New()

	// Read the YAML file
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Environment variables override file valies.
	// Prefix: RJQ_ (e.g., RJQ_QUEUE_WORKERS=10).
	// Dots in YAML keys become underscore (queue.workers -> QUEUE_WORKERS).
	v.SetEnvPrefix("RJQ")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate required fields.
	if cfg.Server.Port == 0 {
		return nil, fmt.Errorf("server.port must be set")
	}
	if cfg.Queue.Workers <= 0 {
		return nil, fmt.Errorf("queue.workers must be positive")
	}

	return &cfg, nil
}

package config

import (
	"flag"
	"fmt"
	"io"
	"strconv"
)

// Config holds the application configuration.
type Config struct {
	ListenAddr    string
	DBPath        string
	AdminPassword string
	BufferSize    int
	FlushEveryMS  int
	MaxBodyBytes  int64
	MaxDBBytes    int64
}

// Load loads configuration from flags and environment variables.
// Precedence: flags > env > defaults.
func Load(args []string, getenv func(string) string) (Config, error) {
	cfg := Config{
		ListenAddr:   ":3617",
		DBPath:       "./agenterr.db",
		AdminPassword: "",
		BufferSize:   10000,
		FlushEveryMS: 200,
		MaxBodyBytes: 5 << 20, // 5MB
		MaxDBBytes:   0,
	}

	// Apply environment variable overrides
	if val := getenv("AGENTERR_LISTEN"); val != "" {
		cfg.ListenAddr = val
	}
	if val := getenv("AGENTERR_DB"); val != "" {
		cfg.DBPath = val
	}
	if val := getenv("AGENTERR_ADMIN_PASSWORD"); val != "" {
		cfg.AdminPassword = val
	}
	if val := getenv("AGENTERR_BUFFER_SIZE"); val != "" {
		bufSize, err := strconv.Atoi(val)
		if err != nil {
			return Config{}, fmt.Errorf("AGENTERR_BUFFER_SIZE: invalid value %q: %w", val, err)
		}
		cfg.BufferSize = bufSize
	}
	if val := getenv("AGENTERR_FLUSH_EVERY_MS"); val != "" {
		flushEvery, err := strconv.Atoi(val)
		if err != nil {
			return Config{}, fmt.Errorf("AGENTERR_FLUSH_EVERY_MS: invalid value %q: %w", val, err)
		}
		cfg.FlushEveryMS = flushEvery
	}
	if val := getenv("AGENTERR_MAX_BODY_BYTES"); val != "" {
		maxBody, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("AGENTERR_MAX_BODY_BYTES: invalid value %q: %w", val, err)
		}
		cfg.MaxBodyBytes = maxBody
	}
	if val := getenv("AGENTERR_MAX_DB_BYTES"); val != "" {
		maxDB, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("AGENTERR_MAX_DB_BYTES: invalid value %q: %w", val, err)
		}
		cfg.MaxDBBytes = maxDB
	}

	// Parse flags (they override env and defaults)
	fs := flag.NewFlagSet("agenterr", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var (
		listenAddr    = fs.String("listen", cfg.ListenAddr, "Listen address")
		dbPath        = fs.String("db", cfg.DBPath, "Database path")
		adminPassword = fs.String("admin-password", cfg.AdminPassword, "Admin password")
		bufferSize    = fs.Int("buffer-size", cfg.BufferSize, "Buffer size")
		flushEvery    = fs.Int("flush-every", cfg.FlushEveryMS, "Flush interval in milliseconds")
		maxBodyBytes  = fs.Int64("max-body-bytes", cfg.MaxBodyBytes, "Max body bytes")
		maxDBBytes    = fs.Int64("max-db-bytes", cfg.MaxDBBytes, "Max database bytes")
	)

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	// Track which flags were explicitly set by visiting them
	flagSet := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		flagSet[f.Name] = true
	})

	// Apply flag overrides (only if explicitly set)
	if flagSet["listen"] {
		cfg.ListenAddr = *listenAddr
	}
	if flagSet["db"] {
		cfg.DBPath = *dbPath
	}
	if flagSet["admin-password"] {
		cfg.AdminPassword = *adminPassword
	}
	if flagSet["buffer-size"] {
		cfg.BufferSize = *bufferSize
	}
	if flagSet["flush-every"] {
		cfg.FlushEveryMS = *flushEvery
	}
	if flagSet["max-body-bytes"] {
		cfg.MaxBodyBytes = *maxBodyBytes
	}
	if flagSet["max-db-bytes"] {
		cfg.MaxDBBytes = *maxDBBytes
	}

	// Validate configuration
	if cfg.FlushEveryMS <= 0 {
		return Config{}, fmt.Errorf("flush-every must be > 0, got %d", cfg.FlushEveryMS)
	}
	if cfg.BufferSize <= 0 {
		return Config{}, fmt.Errorf("buffer-size must be > 0, got %d", cfg.BufferSize)
	}
	if cfg.MaxBodyBytes <= 0 {
		return Config{}, fmt.Errorf("max-body-bytes must be > 0, got %d", cfg.MaxBodyBytes)
	}
	if cfg.MaxDBBytes < 0 {
		return Config{}, fmt.Errorf("max-db-bytes must be >= 0, got %d", cfg.MaxDBBytes)
	}

	return cfg, nil
}

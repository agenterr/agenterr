// Package config loads Agenterr's runtime configuration from defaults,
// environment variables, and command-line flags, in that precedence order
// (flags win, then env, then the built-in defaults below).
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
	ParseBodies   bool
	NoiseFlushMS  int
}

// configFlags holds the flag.Value pointers registered against a FlagSet,
// so flag-registration and override-application can be split into their
// own functions without repeating the flag list.
type configFlags struct {
	listenAddr    *string
	dbPath        *string
	adminPassword *string
	bufferSize    *int
	flushEvery    *int
	maxBodyBytes  *int64
	maxDBBytes    *int64
	parseBodies   *bool
	noiseFlushMS  *int
}

// Load loads configuration from flags and environment variables.
// Precedence: flags > env > defaults.
func Load(args []string, getenv func(string) string) (Config, error) {
	cfg := Config{
		ListenAddr:    ":3617",
		DBPath:        "./agenterr.db",
		AdminPassword: "",
		BufferSize:    10000,
		FlushEveryMS:  200,
		MaxBodyBytes:  5 << 20, // 5MB
		MaxDBBytes:    0,
		ParseBodies:   true,
		NoiseFlushMS:  30000,
	}

	if err := applyEnvOverrides(&cfg, getenv); err != nil {
		return Config{}, err
	}

	fs := flag.NewFlagSet("agenterr", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	flags := registerFlags(fs, cfg)

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	// Track which flags were explicitly set by visiting them, so a flag
	// left at its zero value doesn't clobber an env-derived override.
	flagSet := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		flagSet[f.Name] = true
	})
	applyFlagOverrides(&cfg, flags, flagSet)

	if err := validate(cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// applyEnvOverrides overlays cfg's defaults with any explicitly-set
// AGENTERR_* environment variables.
func applyEnvOverrides(cfg *Config, getenv func(string) string) error {
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
			return fmt.Errorf("AGENTERR_BUFFER_SIZE: invalid value %q: %w", val, err)
		}
		cfg.BufferSize = bufSize
	}
	if val := getenv("AGENTERR_FLUSH_EVERY_MS"); val != "" {
		flushEvery, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("AGENTERR_FLUSH_EVERY_MS: invalid value %q: %w", val, err)
		}
		cfg.FlushEveryMS = flushEvery
	}
	if val := getenv("AGENTERR_MAX_BODY_BYTES"); val != "" {
		maxBody, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return fmt.Errorf("AGENTERR_MAX_BODY_BYTES: invalid value %q: %w", val, err)
		}
		cfg.MaxBodyBytes = maxBody
	}
	if val := getenv("AGENTERR_MAX_DB_BYTES"); val != "" {
		maxDB, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return fmt.Errorf("AGENTERR_MAX_DB_BYTES: invalid value %q: %w", val, err)
		}
		cfg.MaxDBBytes = maxDB
	}
	if val := getenv("AGENTERR_PARSE_BODIES"); val != "" {
		parse, err := strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("AGENTERR_PARSE_BODIES: invalid value %q: %w", val, err)
		}
		cfg.ParseBodies = parse
	}
	if val := getenv("AGENTERR_NOISE_FLUSH_MS"); val != "" {
		noiseFlush, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("AGENTERR_NOISE_FLUSH_MS: invalid value %q: %w", val, err)
		}
		cfg.NoiseFlushMS = noiseFlush
	}
	return nil
}

// registerFlags registers the CLI flags against fs, seeded from cfg (the
// post-env-override values) so an unset flag falls back to whatever env
// or defaults already produced.
func registerFlags(fs *flag.FlagSet, cfg Config) configFlags {
	return configFlags{
		listenAddr:    fs.String("listen", cfg.ListenAddr, "Listen address"),
		dbPath:        fs.String("db", cfg.DBPath, "Database path"),
		adminPassword: fs.String("admin-password", cfg.AdminPassword, "Admin password"),
		bufferSize:    fs.Int("buffer-size", cfg.BufferSize, "Buffer size"),
		flushEvery:    fs.Int("flush-every", cfg.FlushEveryMS, "Flush interval in milliseconds"),
		maxBodyBytes:  fs.Int64("max-body-bytes", cfg.MaxBodyBytes, "Max body bytes"),
		maxDBBytes:    fs.Int64("max-db-bytes", cfg.MaxDBBytes, "Max database bytes"),
		parseBodies:   fs.Bool("parse-bodies", cfg.ParseBodies, "Lift fields from JSON/logfmt log bodies at ingest"),
		noiseFlushMS:  fs.Int("noise-flush-ms", cfg.NoiseFlushMS, "Noise-rule drop-counter flush interval in milliseconds"),
	}
}

// applyFlagOverrides overlays cfg with any flag explicitly passed on the
// command line, per flagSet (from fs.Visit). Flags left at their default
// are skipped so they don't clobber an env-derived value.
func applyFlagOverrides(cfg *Config, flags configFlags, flagSet map[string]bool) {
	if flagSet["listen"] {
		cfg.ListenAddr = *flags.listenAddr
	}
	if flagSet["db"] {
		cfg.DBPath = *flags.dbPath
	}
	if flagSet["admin-password"] {
		cfg.AdminPassword = *flags.adminPassword
	}
	if flagSet["buffer-size"] {
		cfg.BufferSize = *flags.bufferSize
	}
	if flagSet["flush-every"] {
		cfg.FlushEveryMS = *flags.flushEvery
	}
	if flagSet["max-body-bytes"] {
		cfg.MaxBodyBytes = *flags.maxBodyBytes
	}
	if flagSet["max-db-bytes"] {
		cfg.MaxDBBytes = *flags.maxDBBytes
	}
	if flagSet["parse-bodies"] {
		cfg.ParseBodies = *flags.parseBodies
	}
	if flagSet["noise-flush-ms"] {
		cfg.NoiseFlushMS = *flags.noiseFlushMS
	}
}

// validate rejects configurations that would make the rest of the app
// misbehave in confusing ways (e.g. a zero flush interval that never
// flushes, or a negative byte limit).
func validate(cfg Config) error {
	if cfg.FlushEveryMS <= 0 {
		return fmt.Errorf("flush-every must be > 0, got %d", cfg.FlushEveryMS)
	}
	if cfg.BufferSize <= 0 {
		return fmt.Errorf("buffer-size must be > 0, got %d", cfg.BufferSize)
	}
	if cfg.MaxBodyBytes <= 0 {
		return fmt.Errorf("max-body-bytes must be > 0, got %d", cfg.MaxBodyBytes)
	}
	if cfg.MaxDBBytes < 0 {
		return fmt.Errorf("max-db-bytes must be >= 0, got %d", cfg.MaxDBBytes)
	}
	if cfg.NoiseFlushMS <= 0 {
		return fmt.Errorf("noise-flush-ms must be > 0, got %d", cfg.NoiseFlushMS)
	}
	return nil
}

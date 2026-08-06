package ship

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Config holds agenterr-ship's runtime configuration, loaded from flags and
// environment variables in that precedence order (flags win, then env, then
// the built-in defaults below) — mirroring internal/config's Load pattern,
// but self-contained here per the ship package's independence from
// internal/config (ship has no config file; flags/env only).
type Config struct {
	URL string
	Key string

	Docker     bool
	DockerSock string
	// Files holds repeated --file 'GLOB=SERVICE' entries, one per glob.
	Files []string

	Exclude []string
	Only    []string

	DataDir        string
	MaxBufferBytes int64
	JoinWindowMS   int
}

// configFlags holds the flag.Value pointers registered against a FlagSet, so
// flag-registration and override-application can be split into their own
// functions without repeating the flag list.
type configFlags struct {
	url            *string
	key            *string
	docker         *bool
	dockerSock     *string
	exclude        *string
	only           *string
	dataDir        *string
	maxBufferBytes *int64
	joinWindowMS   *int
}

// Load loads ShipConfig from flags and environment variables. Precedence:
// flags > env > defaults.
func Load(args []string, getenv func(string) string) (Config, error) {
	cfg := Config{
		DockerSock:     "/var/run/docker.sock",
		DataDir:        "./agenterr-ship-data",
		MaxBufferBytes: 512 << 20, // 512MB
		JoinWindowMS:   1000,
	}

	if err := applyEnvOverrides(&cfg, getenv); err != nil {
		return Config{}, err
	}

	fs := flag.NewFlagSet("agenterr ship", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	flags := registerFlags(fs, cfg)

	// --file is repeatable, so it's collected via flag.Func directly into
	// cfg.Files rather than through configFlags (there's no single pointer
	// to seed/read back for a repeated flag). It's seeded from any env-set
	// files below; each --file occurrence on the command line appends.
	var fileFlagSeen bool
	fs.Func("file", "Tail a file glob as a service: 'GLOB=SERVICE' (repeatable)", func(v string) error {
		if !fileFlagSeen {
			cfg.Files = nil // flags fully replace an env-derived file list, not append to it
			fileFlagSeen = true
		}
		cfg.Files = append(cfg.Files, v)
		return nil
	})

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

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

func applyEnvOverrides(cfg *Config, getenv func(string) string) error {
	if v := getenv("AGENTERR_SHIP_URL"); v != "" {
		cfg.URL = v
	}
	if v := getenv("AGENTERR_SHIP_KEY"); v != "" {
		cfg.Key = v
	}
	if v := getenv("AGENTERR_SHIP_DOCKER"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("AGENTERR_SHIP_DOCKER: invalid value %q: %w", v, err)
		}
		cfg.Docker = b
	}
	if v := getenv("AGENTERR_SHIP_DOCKER_SOCK"); v != "" {
		cfg.DockerSock = v
	}
	if v := getenv("AGENTERR_SHIP_FILE"); v != "" {
		cfg.Files = splitNonEmpty(v, ",")
	}
	if v := getenv("AGENTERR_SHIP_EXCLUDE"); v != "" {
		cfg.Exclude = splitNonEmpty(v, ",")
	}
	if v := getenv("AGENTERR_SHIP_ONLY"); v != "" {
		cfg.Only = splitNonEmpty(v, ",")
	}
	if v := getenv("AGENTERR_SHIP_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := getenv("AGENTERR_SHIP_MAX_BUFFER_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("AGENTERR_SHIP_MAX_BUFFER_BYTES: invalid value %q: %w", v, err)
		}
		cfg.MaxBufferBytes = n
	}
	if v := getenv("AGENTERR_SHIP_JOIN_WINDOW_MS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("AGENTERR_SHIP_JOIN_WINDOW_MS: invalid value %q: %w", v, err)
		}
		cfg.JoinWindowMS = n
	}
	return nil
}

func registerFlags(fs *flag.FlagSet, cfg Config) configFlags {
	return configFlags{
		url:            fs.String("url", cfg.URL, "Agenterr ingest URL, e.g. https://your-agenterr-host"),
		key:            fs.String("key", cfg.Key, "Ingest API key"),
		docker:         fs.Bool("docker", cfg.Docker, "Tail all non-excluded containers via the Docker socket"),
		dockerSock:     fs.String("docker-sock", cfg.DockerSock, "Docker socket path"),
		exclude:        fs.String("exclude", strings.Join(cfg.Exclude, ","), "Comma-separated service names to exclude"),
		only:           fs.String("only", strings.Join(cfg.Only, ","), "Comma-separated service names to include exclusively"),
		dataDir:        fs.String("data-dir", cfg.DataDir, "Spool directory for buffered records"),
		maxBufferBytes: fs.Int64("max-buffer-bytes", cfg.MaxBufferBytes, "Max on-disk spool bytes before oldest segments are dropped"),
		joinWindowMS:   fs.Int("join-window-ms", cfg.JoinWindowMS, "Multiline join window in milliseconds"),
	}
}

func applyFlagOverrides(cfg *Config, flags configFlags, flagSet map[string]bool) {
	if flagSet["url"] {
		cfg.URL = *flags.url
	}
	if flagSet["key"] {
		cfg.Key = *flags.key
	}
	if flagSet["docker"] {
		cfg.Docker = *flags.docker
	}
	if flagSet["docker-sock"] {
		cfg.DockerSock = *flags.dockerSock
	}
	if flagSet["exclude"] {
		cfg.Exclude = splitNonEmpty(*flags.exclude, ",")
	}
	if flagSet["only"] {
		cfg.Only = splitNonEmpty(*flags.only, ",")
	}
	if flagSet["data-dir"] {
		cfg.DataDir = *flags.dataDir
	}
	if flagSet["max-buffer-bytes"] {
		cfg.MaxBufferBytes = *flags.maxBufferBytes
	}
	if flagSet["join-window-ms"] {
		cfg.JoinWindowMS = *flags.joinWindowMS
	}
}

// validate rejects configurations main shouldn't even try to run:
// url/key are always required, and at least one source (--docker or one or
// more --file entries) must be enabled or there's nothing to ship.
func validate(cfg Config) error {
	if cfg.URL == "" {
		return fmt.Errorf("--url (or AGENTERR_SHIP_URL) is required")
	}
	if cfg.Key == "" {
		return fmt.Errorf("--key (or AGENTERR_SHIP_KEY) is required")
	}
	if !cfg.Docker && len(cfg.Files) == 0 {
		return fmt.Errorf("no source enabled: pass --docker and/or --file 'GLOB=SERVICE'")
	}
	if cfg.MaxBufferBytes <= 0 {
		return fmt.Errorf("--max-buffer-bytes must be > 0, got %d", cfg.MaxBufferBytes)
	}
	if cfg.JoinWindowMS <= 0 {
		return fmt.Errorf("--join-window-ms must be > 0, got %d", cfg.JoinWindowMS)
	}
	return nil
}

// splitNonEmpty splits s on sep, trimming whitespace and dropping empty
// elements — so a trailing comma or accidental double comma in --exclude/
// --only/--file env values doesn't produce spurious empty entries.
func splitNonEmpty(s, sep string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

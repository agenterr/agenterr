package ship

import (
	"reflect"
	"testing"
)

func noEnv(string) string { return "" }

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load([]string{"--url", "https://ingest.example", "--key", "k", "--docker"}, noEnv)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := Config{
		URL:            "https://ingest.example",
		Key:            "k",
		Docker:         true,
		DockerSock:     "/var/run/docker.sock",
		DataDir:        "./agenterr-ship-data",
		MaxBufferBytes: 512 << 20,
		JoinWindowMS:   1000,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("Load() = %+v, want %+v", cfg, want)
	}
}

func TestLoadMissingURLOrKeyIsUsageError(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"missing both", []string{"--docker"}},
		{"missing key", []string{"--url", "https://x", "--docker"}},
		{"missing url", []string{"--key", "k", "--docker"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Load(tt.args, noEnv); err == nil {
				t.Fatal("Load succeeded, want a usage error")
			}
		})
	}
}

func TestLoadNoSourceEnabledIsUsageError(t *testing.T) {
	_, err := Load([]string{"--url", "https://x", "--key", "k"}, noEnv)
	if err == nil {
		t.Fatal("Load succeeded with no --docker and no --file, want a usage error")
	}
}

func TestLoadFileSourceSatisfiesSourceRequirement(t *testing.T) {
	cfg, err := Load([]string{"--url", "https://x", "--key", "k", "--file", "/var/log/*.log=web"}, noEnv)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Files) != 1 || cfg.Files[0] != "/var/log/*.log=web" {
		t.Errorf("Files = %v, want one entry", cfg.Files)
	}
}

func TestLoadRepeatableFileFlag(t *testing.T) {
	cfg, err := Load([]string{
		"--url", "https://x", "--key", "k",
		"--file", "/a/*.log=svc-a",
		"--file", "/b/*.log=svc-b",
	}, noEnv)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []string{"/a/*.log=svc-a", "/b/*.log=svc-b"}
	if !reflect.DeepEqual(cfg.Files, want) {
		t.Errorf("Files = %v, want %v", cfg.Files, want)
	}
}

func TestLoadExcludeOnlyCommaSplit(t *testing.T) {
	cfg, err := Load([]string{
		"--url", "https://x", "--key", "k", "--docker",
		"--exclude", "worker, cron",
		"--only", "web,worker,cron",
	}, noEnv)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !reflect.DeepEqual(cfg.Exclude, []string{"worker", "cron"}) {
		t.Errorf("Exclude = %v", cfg.Exclude)
	}
	if !reflect.DeepEqual(cfg.Only, []string{"web", "worker", "cron"}) {
		t.Errorf("Only = %v", cfg.Only)
	}
}

func TestLoadEnvTwins(t *testing.T) {
	env := map[string]string{
		"AGENTERR_SHIP_URL":              "https://env.example",
		"AGENTERR_SHIP_KEY":              "envkey",
		"AGENTERR_SHIP_DOCKER":           "true",
		"AGENTERR_SHIP_DOCKER_SOCK":      "/tmp/docker.sock",
		"AGENTERR_SHIP_FILE":             "/a/*.log=svc-a,/b/*.log=svc-b",
		"AGENTERR_SHIP_EXCLUDE":          "worker",
		"AGENTERR_SHIP_ONLY":             "web,worker",
		"AGENTERR_SHIP_DATA_DIR":         "/data/ship",
		"AGENTERR_SHIP_MAX_BUFFER_BYTES": "1000",
		"AGENTERR_SHIP_JOIN_WINDOW_MS":   "500",
	}
	getenv := func(k string) string { return env[k] }

	cfg, err := Load(nil, getenv)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := Config{
		URL:            "https://env.example",
		Key:            "envkey",
		Docker:         true,
		DockerSock:     "/tmp/docker.sock",
		Files:          []string{"/a/*.log=svc-a", "/b/*.log=svc-b"},
		Exclude:        []string{"worker"},
		Only:           []string{"web", "worker"},
		DataDir:        "/data/ship",
		MaxBufferBytes: 1000,
		JoinWindowMS:   500,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("Load() = %+v, want %+v", cfg, want)
	}
}

func TestLoadFlagsOverrideEnv(t *testing.T) {
	env := map[string]string{
		"AGENTERR_SHIP_URL": "https://env.example",
		"AGENTERR_SHIP_KEY": "envkey",
	}
	getenv := func(k string) string { return env[k] }

	cfg, err := Load([]string{"--url", "https://flag.example", "--docker"}, getenv)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.URL != "https://flag.example" {
		t.Errorf("URL = %q, want flag to win over env", cfg.URL)
	}
	if cfg.Key != "envkey" {
		t.Errorf("Key = %q, want env value preserved when flag unset", cfg.Key)
	}
}

func TestLoadFileFlagReplacesEnvFileList(t *testing.T) {
	// A --file flag on the command line fully replaces an env-derived file
	// list rather than appending to it — otherwise there'd be no way to
	// override AGENTERR_SHIP_FILE from the command line to a single glob.
	env := map[string]string{
		"AGENTERR_SHIP_URL":  "https://x",
		"AGENTERR_SHIP_KEY":  "k",
		"AGENTERR_SHIP_FILE": "/env/*.log=envsvc",
	}
	getenv := func(k string) string { return env[k] }

	cfg, err := Load([]string{"--file", "/flag/*.log=flagsvc"}, getenv)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []string{"/flag/*.log=flagsvc"}
	if !reflect.DeepEqual(cfg.Files, want) {
		t.Errorf("Files = %v, want %v", cfg.Files, want)
	}
}

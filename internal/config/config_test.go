package config

import (
	"testing"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		getenv  func(string) string
		want    Config
		wantErr string // substring to check in error message
	}{
		{
			name: "all defaults",
			args: []string{},
			getenv: func(string) string { return "" },
			want: Config{
				ListenAddr:   ":3617",
				DBPath:       "./agenterr.db",
				AdminPassword: "",
				BufferSize:   10000,
				FlushEveryMS: 200,
				MaxBodyBytes: 5 << 20,
				MaxDBBytes:   0,
			},
		},
		{
			name: "env overrides defaults",
			args: []string{},
			getenv: func(key string) string {
				switch key {
				case "AGENTERR_LISTEN":
					return ":9000"
				case "AGENTERR_DB":
					return "/tmp/test.db"
				case "AGENTERR_ADMIN_PASSWORD":
					return "secret123"
				case "AGENTERR_BUFFER_SIZE":
					return "5000"
				case "AGENTERR_FLUSH_EVERY_MS":
					return "500"
				case "AGENTERR_MAX_BODY_BYTES":
					return "10485760" // 10MB
				case "AGENTERR_MAX_DB_BYTES":
					return "1073741824" // 1GB
				}
				return ""
			},
			want: Config{
				ListenAddr:    ":9000",
				DBPath:        "/tmp/test.db",
				AdminPassword: "secret123",
				BufferSize:    5000,
				FlushEveryMS:  500,
				MaxBodyBytes:  10485760,
				MaxDBBytes:    1073741824,
			},
		},
		{
			name: "flags override env",
			args: []string{"--listen", ":8080", "--db", "/data/agenterr.db", "--admin-password", "flag_pass"},
			getenv: func(key string) string {
				switch key {
				case "AGENTERR_LISTEN":
					return ":9000"
				case "AGENTERR_DB":
					return "/tmp/test.db"
				case "AGENTERR_ADMIN_PASSWORD":
					return "env_pass"
				}
				return ""
			},
			want: Config{
				ListenAddr:    ":8080",
				DBPath:        "/data/agenterr.db",
				AdminPassword: "flag_pass",
				BufferSize:    10000,
				FlushEveryMS:  200,
				MaxBodyBytes:  5 << 20,
				MaxDBBytes:    0,
			},
		},
		{
			name: "numeric flag overrides numeric env in same call",
			args: []string{"--buffer-size", "300"},
			getenv: func(key string) string {
				switch key {
				case "AGENTERR_BUFFER_SIZE":
					return "5000"
				case "AGENTERR_MAX_DB_BYTES":
					return "123"
				}
				return ""
			},
			want: Config{
				ListenAddr:    ":3617",
				DBPath:        "./agenterr.db",
				AdminPassword: "",
				BufferSize:    300,      // flag overrides env
				FlushEveryMS:  200,
				MaxBodyBytes:  5 << 20,
				MaxDBBytes:    123,      // env overrides default
			},
		},
		{
			name: "all flags set",
			args: []string{
				"--listen", ":7000",
				"--db", "/db/test.db",
				"--admin-password", "test_pass",
				"--buffer-size", "2000",
				"--flush-every", "100",
				"--max-body-bytes", "2097152",
				"--max-db-bytes", "536870912",
			},
			getenv: func(string) string { return "" },
			want: Config{
				ListenAddr:    ":7000",
				DBPath:        "/db/test.db",
				AdminPassword: "test_pass",
				BufferSize:    2000,
				FlushEveryMS:  100,
				MaxBodyBytes:  2097152,
				MaxDBBytes:    536870912,
			},
		},
		{
			name:    "error: negative flush-every",
			args:    []string{"--flush-every", "-5"},
			getenv:  func(string) string { return "" },
			wantErr: "flush-every",
		},
		{
			name:    "error: zero flush-every",
			args:    []string{"--flush-every", "0"},
			getenv:  func(string) string { return "" },
			wantErr: "flush-every",
		},
		{
			name:    "error: negative buffer-size",
			args:    []string{"--buffer-size", "-100"},
			getenv:  func(string) string { return "" },
			wantErr: "buffer-size",
		},
		{
			name:    "error: zero buffer-size",
			args:    []string{"--buffer-size", "0"},
			getenv:  func(string) string { return "" },
			wantErr: "buffer-size",
		},
		{
			name:    "error: negative max-body-bytes",
			args:    []string{"--max-body-bytes", "-1024"},
			getenv:  func(string) string { return "" },
			wantErr: "max-body-bytes",
		},
		{
			name:    "error: zero max-body-bytes",
			args:    []string{"--max-body-bytes", "0"},
			getenv:  func(string) string { return "" },
			wantErr: "max-body-bytes",
		},
		{
			name:    "error: negative max-db-bytes (but 0 is ok)",
			args:    []string{"--max-db-bytes", "-1"},
			getenv:  func(string) string { return "" },
			wantErr: "max-db-bytes",
		},
		{
			name:    "error: invalid flush-every format",
			args:    []string{"--flush-every", "not-a-number"},
			getenv:  func(string) string { return "" },
			wantErr: "flush-every",
		},
		{
			name:    "error: invalid buffer-size format",
			args:    []string{"--buffer-size", "invalid"},
			getenv:  func(string) string { return "" },
			wantErr: "buffer-size",
		},
		{
			name:    "error: invalid max-body-bytes format",
			args:    []string{"--max-body-bytes", "bad"},
			getenv:  func(string) string { return "" },
			wantErr: "max-body-bytes",
		},
		{
			name:    "error: invalid max-db-bytes format",
			args:    []string{"--max-db-bytes", "nope"},
			getenv:  func(string) string { return "" },
			wantErr: "max-db-bytes",
		},
		{
			name: "empty admin password is ok",
			args: []string{},
			getenv: func(string) string { return "" },
			want: Config{
				ListenAddr:    ":3617",
				DBPath:        "./agenterr.db",
				AdminPassword: "",
				BufferSize:    10000,
				FlushEveryMS:  200,
				MaxBodyBytes:  5 << 20,
				MaxDBBytes:    0,
			},
		},
		{
			name: "max-db-bytes zero is valid (unlimited)",
			args: []string{"--max-db-bytes", "0"},
			getenv: func(string) string { return "" },
			want: Config{
				ListenAddr:    ":3617",
				DBPath:        "./agenterr.db",
				AdminPassword: "",
				BufferSize:    10000,
				FlushEveryMS:  200,
				MaxBodyBytes:  5 << 20,
				MaxDBBytes:    0,
			},
		},
		{
			name: "positive max-db-bytes",
			args: []string{"--max-db-bytes", "100000000"},
			getenv: func(string) string { return "" },
			want: Config{
				ListenAddr:    ":3617",
				DBPath:        "./agenterr.db",
				AdminPassword: "",
				BufferSize:    10000,
				FlushEveryMS:  200,
				MaxBodyBytes:  5 << 20,
				MaxDBBytes:    100000000,
			},
		},
		{
			name: "env set max-db-bytes",
			args: []string{},
			getenv: func(key string) string {
				if key == "AGENTERR_MAX_DB_BYTES" {
					return "2147483648" // 2GB
				}
				return ""
			},
			want: Config{
				ListenAddr:    ":3617",
				DBPath:        "./agenterr.db",
				AdminPassword: "",
				BufferSize:    10000,
				FlushEveryMS:  200,
				MaxBodyBytes:  5 << 20,
				MaxDBBytes:    2147483648,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Load(tt.args, tt.getenv)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("Load() got no error, want error containing %q", tt.wantErr)
				}
				if !contains(err.Error(), tt.wantErr) {
					t.Fatalf("Load() error = %q, want containing %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() got unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Load() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

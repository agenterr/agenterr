package normalize

import "testing"

func TestStripANSI(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
		red  bool
	}{
		{"no escapes fast path", "plain log line", "plain log line", false},
		// GORM's colorized not-found line. Shape is from a real production
		// corpus; the import path is synthetic.
		{"gorm red line",
			"2026/08/08 22:18:20 \x1b[31;1mgithub.com/acme/orders-api/internal/repositories/billing/invoice/repo.go:22 \x1b[35;1mrecord not found",
			"2026/08/08 22:18:20 github.com/acme/orders-api/internal/repositories/billing/invoice/repo.go:22 record not found",
			true},
		{"bright red 91", "\x1b[91merror\x1b[0m done", "error done", true},
		{"magenta only is not red", "\x1b[35;1mrecord not found", "record not found", false},
		{"31 must be a full param, not a substring", "\x1b[131mx\x1b[315my", "xy", false},
		{"non-SGR CSI stripped, no red", "\x1b[2Jcleared\x1b[10;20H", "cleared", false},
		{"unterminated escape at end dropped", "tail\x1b[31;1", "tail", false},
		{"bare ESC pair removed", "a\x1bcb", "ab", false},
		{"lone ESC at end removed", "x\x1b", "x", false},
		{"empty", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, red := StripANSI(tt.in)
			if got != tt.want {
				t.Errorf("clean = %q, want %q", got, tt.want)
			}
			if red != tt.red {
				t.Errorf("red = %v, want %v", red, tt.red)
			}
		})
	}
}
